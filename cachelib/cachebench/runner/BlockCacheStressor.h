#pragma once

#include <folly/Random.h>
#include <folly/TokenBucket.h>
#include <libaio.h>

#include <assert.h>     /* assert */
#include <atomic>
#include <cstddef>
#include <iostream>
#include <memory>
#include <chrono>
#include <thread>
#include <unordered_set>
#include <algorithm>    // std::max

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/cache/TimeStampTicker.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/runner/BlockCacheStressor.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"
#include "cachelib/cachebench/util/BlockRequestBase.h"

using namespace std::chrono_literals;

namespace facebook {
namespace cachelib {
namespace cachebench {

constexpr uint64_t maxBlockReqQueueSize = 500000;
constexpr uint64_t maxPendingBlockRequests = 100000;
constexpr uint64_t maxConcurrentIO = 60000;
constexpr uint64_t backingStoreAlignment = 512;


// Implementation of block cache stressor that uses block trace replay 
// to stress an instance of the cache and the backing storage device. 
template <typename Allocator>
class BlockCacheStressor : public BlockCacheStressorBase {
	public:
		using CacheT = Cache<Allocator>;
		using Key = typename CacheT::Key;
		using ItemHandle = typename CacheT::ItemHandle;

    // @param cacheConfig   the config to instantiate the cache instance
    // @param config        stress test config
    // @param generator     workload  generator
    BlockCacheStressor(CacheConfig cacheConfig,
                StressorConfig config,
                std::unique_ptr<GeneratorBase>&& generator)
            : config_(std::move(config)),
            blockReplayStatVec_(config_.numThreads),
            wg_(std::move(generator)),
            hardcodedString_(genHardcodedString()),
            replayDoneFlagVec_(config_.numThreads),
            blockRequestVec_(maxPendingBlockRequests, nullptr),
            endTime_{std::chrono::system_clock::time_point::max()} {
        
        // setup the cache 
        cache_ = std::make_unique<CacheT>(cacheConfig);

        // open the file on disk to perform DIRECT IO
        backingStoreFileHandle_ = open(config_.replayGeneratorConfig.diskFilePath.c_str(), O_DIRECT|O_RDWR, 0644);
        if (backingStoreFileHandle_ == -1) {
            throw std::runtime_error(
                folly::sformat("Could not open file: {} \n", config_.replayGeneratorConfig.diskFilePath.c_str()));
        }

        // initiate an IO context to submit async IO requests 
        ctx_ = new io_context_t();
        int ret = io_setup(maxConcurrentIO, ctx_);
        if (ret != 0) {
            throw std::runtime_error(
                folly::sformat("Error in io_setup, return: {}\n", ret));
        }
    }


    ~BlockCacheStressor() override { finish(); }


    // Start the stress test by spawning the worker threads and waiting for them
    // to finish the stress operations.
    void start() override {
        {
            std::lock_guard<std::mutex> l(timeMutex_);
            startTime_ = std::chrono::system_clock::now();
        }
        stressWorker_ = std::thread([this] {
            std::vector<std::thread> workers;
            // replay thread 
            for (uint64_t i = 0; i < config_.numThreads; ++i) {
                workers.push_back(std::thread([this, blockReplayStats = &blockReplayStatVec_.at(i), index=i]() {
                    stressByBlockReplay(*blockReplayStats, index);
                }));
            }

            // process block request thread  
            for (uint64_t i = 0; i<config_.replayGeneratorConfig.processorThreadCount; ++i) {
                workers.push_back(
                    std::thread([this, blockReplayStats = &blockReplayStatVec_.at(0)]() {
                    processRequest(*blockReplayStats);
                }));
            }
            
            // async IO tracker 
            workers.push_back(
                std::thread([this, blockReplayStats = &blockReplayStatVec_.at(0)]() {
                asyncIOTracker(*blockReplayStats);
            }));

            // process completed block request thread 
            for (uint64_t i = 0; i<config_.replayGeneratorConfig.asyncIOTrackerThreadCount; ++i) {
                workers.push_back(
                    std::thread([this, blockReplayStats = &blockReplayStatVec_.at(0)]() {
                    processCompletedRequest(*blockReplayStats);
                }));
            }
            
            // periodic stat printer thread 
            workers.push_back(
                std::thread([this, blockReplayStats = &blockReplayStatVec_.at(0)]() {
                    printStatThread(*blockReplayStats);
            }));

            for (auto& worker : workers) {
                worker.join();
            }
            {
                std::lock_guard<std::mutex> l(timeMutex_);
                endTime_ = std::chrono::system_clock::now();
            }
        });
    }


    // Block until all stress workers are finished.
    void finish() override {
        if (stressWorker_.joinable()) {
            stressWorker_.join();
        }
        wg_->markShutdown();
        cache_->clearCache(config_.maxInvalidDestructorCount);
        io_destroy(*ctx_);
        close(backingStoreFileHandle_);
    }


    // abort the stress run by indicating to the workload generator and
    // delegating to the base class abort() to stop the test.
    void abort() override {
        wg_->markShutdown();
        BlockCacheStressorBase::abort();
    }


    // obtain stats from the cache instance.
    Stats getCacheStats() const override { return cache_->getStats(); }


    // obtain aggregated block replay stats 
    BlockReplayStats aggregateBlockReplayStats() const override {
        BlockReplayStats res{};
        for (const auto& stats : blockReplayStatVec_) {
            res += stats;
        }
        return res;
    }


    uint64_t getTestDurationNs() const override {
        std::lock_guard<std::mutex> l(timeMutex_);
        return std::chrono::nanoseconds{std::min(std::chrono::system_clock::now(), endTime_) - startTime_}
                .count();
    }


    util::PercentileStats* sLatBlockReadPercentile() const override {
        return sLatBlockReadPercentile_;
    }


    util::PercentileStats* sLatBlockWritePercentile() const override {
        return sLatBlockWritePercentile_;
    }


    util::PercentileStats* cLatBlockReadPercentile() const override {
        return cLatBlockReadPercentile_;
    }


    util::PercentileStats* cLatBlockWritePercentile() const override {
        return cLatBlockWritePercentile_;
    }


    util::PercentileStats* latBackingReadPercentile() const override {
        return latBackingReadPercentile_;
    }


    util::PercentileStats* latBackingWritePercentile() const override {
        return latBackingWritePercentile_;
    }


    util::PercentileStats* getBlockReadSizePercentile() const override {
        return blockReadSizeBytesPercentile_;
    }


    util::PercentileStats* getBlockWriteSizePercentile() const override {
        return blockWriteSizeBytesPercentile_;
    }


    void printStatThread(BlockReplayStats& stats) {
        std::cout << "log: printer thread started \n";
        while (!endExperimentFlag(stats)) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            printCurrentStats(stats);
        }
        std::cout << "log: printer thread terminated \n";
    }


    // populate the input item handle according to the stress setup.
    void populateItem(ItemHandle& handle) {
        if (!config_.populateItem) {
            return;
        }
        XDCHECK(handle);
        XDCHECK_LE(cache_->getSize(handle), 4ULL * 1024 * 1024);
        cache_->setStringItem(handle, hardcodedString_);
    }


    // load the given key to cache 
    void loadKey(const std::string key, BlockReplayStats& stats) {
        stats.loadCount++;
        auto it = cache_->allocate(0, 
                            key, 
                            config_.replayGeneratorConfig.pageSizeBytes, 
                            0);
        if (it == nullptr) {
            stats.loadPageFailure++;
        } else {
            populateItem(it);
            cache_->insertOrReplace(it);
        }
    }


    // get cache misses for a block request 
    std::vector<std::tuple<uint64_t, uint64_t>> getCacheMiss(BlockRequest* req, BlockReplayStats& stats) {
        uint64_t size = 0;
        uint64_t offset = 0;
        std::vector<std::tuple<uint64_t, uint64_t>> cacheMissVec;
        for (uint64_t curPage=req->getStartPage(); 
                curPage<=req->getEndPage(); 
                curPage++) {

            stats.readPageCount++;
            const std::string key = std::to_string(curPage);
            auto it = cache_->find(key, AccessMode::kRead);
            if (it == nullptr) {
                // read cache miss detected 
                req->addMissKey(curPage);

                // if no previous read cache miss detected 
                // set the starting offset
                if (size == 0) 
                    offset = curPage*config_.replayGeneratorConfig.pageSizeBytes;

                // track the size of contigous cache misses 
                size += config_.replayGeneratorConfig.pageSizeBytes;
            } else {
                // there is a cache hit 
                // if there were previous misses not tracked yet 
                // add it to the vector of cache misses and reset size 
                if (size > 0) {
                    cacheMissVec.push_back(std::tuple<uint64_t, uint64_t>(offset, size));
                    size = 0;
                }
            }
        }
        // if there were previous misses not tracked yet 
        // add it to the vector of cache misses 
        if (size > 0) 
            cacheMissVec.push_back(std::tuple<uint64_t, uint64_t>(offset, size));
        
        return cacheMissVec;
    }


    // submit asyn read IO request to the backing store 
    void submitAsyncRead(AsyncIORequest *asyncReq, uint64_t offset, uint64_t size) {
        asyncReq->startLatencyTracking(*latBackingReadPercentile_);
        io_prep_pread(asyncReq->iocbPtr,
            backingStoreFileHandle_,
            (void*) asyncReq->buffer,
            size,
            offset);
        int ret = io_submit(*ctx_, 1, &asyncReq->iocbPtr);
        if (ret < 1) {
            throw std::runtime_error(
                folly::sformat("Error in function io_submit. Return={}\n", ret));
        }
    }


    // submit asyn write IO request to the backing store 
    void submitAsyncWrite(AsyncIORequest *asyncReq, uint64_t offset, uint64_t size) {
        asyncReq->startLatencyTracking(*latBackingWritePercentile_);
        io_prep_pwrite(asyncReq->iocbPtr,
            backingStoreFileHandle_,
            (void*) asyncReq->buffer,
            size,
            offset);
        int ret = io_submit(*ctx_, 1, &asyncReq->iocbPtr);
        if (ret < 1) {
            throw std::runtime_error(
                folly::sformat("Error in function io_submit. Return={}\n", ret));
        }
    }


    void printCurrentStats(BlockReplayStats& stats) {
        Stats cacheStats = cache_->getStats();

        const uint64_t numSeconds = getTestDurationNs()/static_cast<double>(1e9);
        std::cout << folly::sformat("T={},",
                                    numSeconds);

        uint64_t blockReqCount = stats.readReqCount + stats.writeReqCount;
        std::cout << folly::sformat("BlockReqCount={}/{},",
                                        stats.blockReqProcessed,
                                        blockReqCount);

        std::cout << folly::sformat("MaxInputQueueSize={},",
                                        stats.maxInputQueueSize);

        {
            const std::lock_guard<std::mutex> l(inputQueueMutex_);
            std::cout << folly::sformat("InputQueueSize={},",
                                            inputQueue_.size());
        }

        std::cout << folly::sformat("MaxOutputQueueSize={},",
                                        stats.maxOutputQueueSize);

        {
            const std::lock_guard<std::mutex> l(outputQueueMutex_);
            std::cout << folly::sformat("OutputQueueSize={},",
                                            outputQueue_.size());
        }

        {
            std::cout << folly::sformat("PendingIO={},",
                                            pendingIOCount_);
        }

        {
            const std::lock_guard<std::mutex> l(pendingBlockRequestMutex_);
            std::cout << folly::sformat("PendingBlock={},", pendingBlockRequestCount_);
            std::cout << folly::sformat("MaxPendingBlock={},", stats.maxPendingReq);
        }
        
        std::cout << folly::sformat("MaxPendingIO={},",
                                        stats.maxPendingIO);

        const uint64_t ramItemCount = cacheStats.getRAMItemCount();
        std::cout << folly::sformat("t1Size={},", ramItemCount);


        // const uint64_t nvmItemCount = cacheStats.getNVMItemCount();
        // std::cout << folly::sformat("T2 item count={},",
        //                             nvmItemCount);

        std::cout << folly::sformat("Drop={}/{},",
                                        stats.readBlockRequestDropCount,
                                        stats.writeBlockRequestDropCount);

        double asyncPerRead = static_cast<double>(totalMissVecSize_)/static_cast<double>(stats.readReqCount);
        std::cout << folly::sformat("Avg.asyncPerRead={},",
                                        asyncPerRead);

        // std::cout << folly::sformat("Block request processed={},",
        //                             stats.blockReqProcessed);

        // std::cout << folly::sformat("Backing store IO count={},",
        //                             stats.totalBackingStoreIO);

        // std::cout << folly::sformat("Backing store IO returned={},",
        //                             stats.totalBackingStoreIOReturned);

        // std::cout << folly::sformat("Pending IO={},",
        //                             pendingIOCount_);

        // std::cout << folly::sformat("Max pending IO={},",
        //                             stats.maxPendingIO);

        // std::cout << folly::sformat("Block request queue size={},",
        //                             getCurrentQueueSize());

        // std::cout << folly::sformat("Max block request queue size={},",
        //                             stats.maxQueueSize);

        // const double writeRatio = stats.writeReqCount/static_cast<double>(stats.blockReqCount);
        // std::cout << folly::sformat("WriteRatio={:.3f}\n \n",
        //                             writeRatio);

        std::cout << "\n\n";

    }  


    bool endExperimentFlag(BlockReplayStats& stats) {
        const bool replayCompleted = std::all_of(std::begin(replayDoneFlagVec_), 
                                                std::begin(replayDoneFlagVec_)+config_.numThreads, 
                                                []( const bool v){ return v; } );

        uint64_t inputQueueSize;
        {
            const std::lock_guard<std::mutex> l(inputQueueMutex_);
            inputQueueSize = inputQueue_.size();
        }

        uint64_t outputQueueSize;
        {
            const std::lock_guard<std::mutex> l(outputQueueMutex_);
            outputQueueSize = outputQueue_.size();
        }

        uint64_t curPendingIOCount;
        {
            const std::lock_guard<std::mutex> l(iocbToAsyncMapMutex_);
            curPendingIOCount = pendingIOCount_;
        }

        uint64_t curPendingBlockRequestCount;
        {
            const std::lock_guard<std::mutex> l(pendingBlockRequestMutex_);
            curPendingBlockRequestCount = pendingBlockRequestCount_;
        }
        
        if ((replayCompleted) && (curPendingIOCount == 0) && (inputQueueSize==0) && (outputQueueSize==0) && (curPendingBlockRequestCount==0)) {
            if (stats.experimentRuntime == 0)
                stats.experimentRuntime = getTestDurationNs();
            return true;
        } else {
            return false;
        }
    }


    AsyncIORequest* findIOCBToAsyncMap(iocb* iocbPtr) {
        const std::lock_guard<std::mutex> l(iocbToAsyncMapMutex_);
        std::map<iocb*, AsyncIORequest*>::iterator itr = iocbToAsyncMap_.find(iocbPtr);
        AsyncIORequest* ioReq;
        if (itr != iocbToAsyncMap_.end()) {  
            ioReq = itr->second;
            iocbToAsyncMap_.erase(itr);
        } else {
            throw std::runtime_error(
                folly::sformat("No mapping to index found for the IOCB pointer: {} \n", iocbPtr));
        }
        pendingIOCount_--;
        return ioReq;
    }



    // track the list of async IO requests 
    void asyncIOTracker(BlockReplayStats& stats) {
        struct io_event* events = new io_event[maxConcurrentIO];
        struct timespec timeout;
        timeout.tv_sec = 0;
        timeout.tv_nsec = 1000; // 1us
        while (!endExperimentFlag(stats)) {
            int ret = io_getevents(*ctx_, 1, maxConcurrentIO, events, &timeout);
            if (ret > 0) {
                for (int eindex=0; eindex<ret; eindex++) {
                    stats.totalBackingStoreIOReturned++;
                    iocb *retiocb = events[eindex].obj;
                    uint64_t size = retiocb->u.v.nr;
                    uint64_t res = events[eindex].res;
                    short op = retiocb->aio_lio_opcode;

                    if (size != res) {
                        throw std::runtime_error(folly::sformat("Size: {} Return {} Op {}\n", size, res, op));
                    }

                    AsyncIORequest* ioReq = findIOCBToAsyncMap(retiocb);
                    if (op == 0)
                        appendToOutputQueue(ioReq, false);
                    else
                        appendToOutputQueue(ioReq, true);

                }
            }
        }
        std::cout << "log: async IO tracker thread terminated \n";
    }


    uint64_t getNextBlockReq(std::tuple<uint64_t, uint64_t, OpType> reqTuple) {
        const std::lock_guard<std::mutex> l(pendingBlockRequestMutex_);
        uint64_t index = maxPendingBlockRequests;
        for (uint64_t reqIndex=0; reqIndex<blockRequestVec_.size(); reqIndex++) {
            if (blockRequestVec_.at(reqIndex) == nullptr) {
                pendingBlockRequestCount_++;
                index = reqIndex;
                OpType op = std::get<2>(reqTuple);
                if (op==OpType::kGet) {
                    blockRequestVec_.at(reqIndex) = new BlockRequest(std::get<0>(reqTuple), 
                                                                    std::get<1>(reqTuple), 
                                                                    std::get<2>(reqTuple),
                                                                    config_.replayGeneratorConfig.pageSizeBytes, 
                                                                    config_.replayGeneratorConfig.traceBlockSizeBytes,
                                                                    *sLatBlockReadPercentile_);
                } else if (op == OpType::kSet) {
                    blockRequestVec_.at(reqIndex) = new BlockRequest(std::get<0>(reqTuple), 
                                                                    std::get<1>(reqTuple), 
                                                                    std::get<2>(reqTuple),
                                                                    config_.replayGeneratorConfig.pageSizeBytes, 
                                                                    config_.replayGeneratorConfig.traceBlockSizeBytes,
                                                                    *sLatBlockWritePercentile_);
                }
                break;
            }
        }
        return index;
    }


    void removeBlockRequest(uint64_t index) {
        const std::lock_guard<std::mutex> l(pendingBlockRequestMutex_);
        delete blockRequestVec_.at(index);
        blockRequestVec_.at(index) = nullptr;
        pendingBlockRequestCount_--;
    }


    void setKeys(BlockRequest* req, BlockReplayStats& stats) {
        uint64_t startPage = req->getStartPage();
        uint64_t endPage = req->getEndPage();
        OpType op = req->getOp();
        uint64_t size = req->getSize();
        for (uint64_t curPage=startPage; curPage<=endPage; curPage++) {
            const std::string strkey = std::to_string(curPage);
            if (op == OpType::kGet) {
                if (req->checkKeyMiss(curPage)) {
                    loadKey(strkey, stats);
                }
                else {
                    stats.readPageHitCount++;
                    cache_->recordAccess(strkey);
                }
            } else if (op == OpType::kSet) {
                stats.writePageCount++;
                loadKey(strkey, stats);
            } else {
                throw std::runtime_error(
                    folly::sformat("Operation not supported, only read and write {} {} {} {} {} {} \n", int (op),
                                                                            startPage,
                                                                            endPage,
                                                                            size,
                                                                            req->getOffset(),
                                                                            req->getLBA()));
            }
        }
    }


    void updateAsyncCompletion(uint64_t index, uint64_t size, bool writeFlag, BlockReplayStats& stats, iocb* iocbPtr) {
        
        BlockRequest* req = blockRequestVec_.at(index);
        req->async(size, writeFlag);
        req->trackIOCB(iocbPtr);
        if (req->isBlockRequestProcessed()) {
            stats.blockReqProcessed++;
            setKeys(req, stats);
            delete blockRequestVec_.at(index);
            blockRequestVec_.at(index) = nullptr;
            pendingBlockRequestCount_--;
        }
    }


    void processCompletedRequest(BlockReplayStats& stats) {
        std::cout << "log: started completed request processor\n";
        while (!endExperimentFlag(stats)) {
            std::pair<AsyncIORequest*, bool> outputQueuePair = popFromOutputQueue(stats);
            AsyncIORequest* asyncReq = std::get<0>(outputQueuePair);
            bool writeFlag = std::get<1>(outputQueuePair);
            if (asyncReq != nullptr) {
                uint64_t index = asyncReq->getKey();
                uint64_t size = asyncReq->getSize();
                // blockRequestVec_.at(index)->trackIOCB(asyncReq->iocbPtr);
                const std::lock_guard<std::mutex> l(pendingBlockRequestMutex_);
                updateAsyncCompletion(index, size, writeFlag, stats, asyncReq->iocbPtr);
                delete asyncReq;
            }
        }
    }


    std::pair<AsyncIORequest*, bool> popFromOutputQueue(BlockReplayStats& stats) {
        const std::lock_guard<std::mutex> l(outputQueueMutex_);
        std::pair<AsyncIORequest*, bool> outputPair = std::make_pair(nullptr, false);
        uint64_t queueSize = outputQueue_.size();
        if (queueSize > 0) {
            outputPair = outputQueue_.front();
            outputQueue_.pop();
        }
        if (queueSize > stats.maxOutputQueueSize)
            stats.maxOutputQueueSize = queueSize; 
        return outputPair;
    }


    void appendToOutputQueue(AsyncIORequest* req, bool writeFlag) {
        const std::lock_guard<std::mutex> l(outputQueueMutex_);
        outputQueue_.push(std::make_pair(req, writeFlag));
    }


    void processRead(uint64_t index, BlockReplayStats& stats) {
        BlockRequest* req = blockRequestVec_.at(index);
        uint64_t offset = req->getOffset();
        uint64_t size = req->getSize();

        std::vector<std::tuple<uint64_t, uint64_t>> cacheMissVec = getCacheMiss(req, stats);

        uint64_t cacheMissBytes = 0;
        for (std::tuple<uint64_t, uint64_t> miss : cacheMissVec) {
            cacheMissBytes += std::get<1>(miss);
        }

        uint64_t reqTotalIO = req->getTotalIO();
        assert(reqTotalIO>=cacheMissBytes);

        // std::cout << folly::sformat("Read async:{}, Miss bytes:{}, Total IO: {}\n", 
        //                                 cacheMissVec.size(), 
        //                                 cacheMissBytes, 
        //                                 reqTotalIO);

        if (cacheMissBytes == 0) {   
            // no miss all done 
            const std::lock_guard<std::mutex> l(pendingBlockRequestMutex_);
            if (pendingBlockRequestCount_ > stats.maxPendingReq)
                stats.maxPendingReq = pendingBlockRequestCount_;
            totalMissVecSize_ += cacheMissVec.size();
            pendingBlockRequestCount_--;
            stats.blockReqProcessed++;
            delete blockRequestVec_.at(index);
            blockRequestVec_.at(index) = nullptr;
        } else {
            // need to submit IOs to disk for cache misses 
            req->hit(reqTotalIO-cacheMissBytes);
            // AsyncIORequest *ioReq = new AsyncIORequest(cacheMissBytes, backingStoreAlignment, index);
            // appendToOutputQueue(ioReq, false);
                
            for (std::tuple<uint64_t, uint64_t> miss : cacheMissVec) {
                // an async IO request for each and add it to map 
                AsyncIORequest* aioReq = new AsyncIORequest(
                                                std::get<1>(miss), 
                                                backingStoreAlignment, 
                                                index);

                doRead(index, 
                        std::get<0>(miss), 
                        std::get<1>(miss), 
                        stats);
            }

        }
        // else {
        //     if ((reqTotalIO-cacheMissBytes)>0)
        //         // updateHit(req, reqTotalIO-cacheMissBytes);
        //         req->hit(reqTotalIO-cacheMissBytes);
        // }

        // for (std::tuple<uint64_t, uint64_t> miss : cacheMissVec) {
        //     // an async IO request for each and add it to map 
        //     AsyncIORequest* aioReq = new AsyncIORequest(
        //                                     std::get<1>(miss), 
        //                                     backingStoreAlignment, 
        //                                     index);

        //     doRead(index, 
        //             std::get<0>(miss)*config_.replayGeneratorConfig.traceBlockSizeBytes, 
        //             std::get<1>(miss), 
        //             stats);
        // }

        
        // // updateHit(req, size);
        
    }


    void doRead(uint64_t index, uint64_t offset, uint64_t size, BlockReplayStats& stats) {
        const std::lock_guard<std::mutex> l(iocbToAsyncMapMutex_);
        AsyncIORequest *ioReq = new AsyncIORequest(size, backingStoreAlignment, index);
        pendingIOCount_++;
        if (pendingIOCount_ > stats.maxPendingIO)
            stats.maxPendingIO = pendingIOCount_;
        submitAsyncRead(ioReq, offset, size);
        iocbToAsyncMap_.insert(std::pair<iocb*, AsyncIORequest*>(ioReq->iocbPtr, ioReq));
    }


    void doWrite(uint64_t index, uint64_t offset, uint64_t size, BlockReplayStats& stats) {
        const std::lock_guard<std::mutex> l(iocbToAsyncMapMutex_);
        AsyncIORequest *ioReq = new AsyncIORequest(size, backingStoreAlignment, index);
        pendingIOCount_++;
        if (pendingIOCount_ > stats.maxPendingIO)
            stats.maxPendingIO = pendingIOCount_;
        submitAsyncWrite(ioReq, offset, size);
        iocbToAsyncMap_.insert(std::pair<iocb*, AsyncIORequest*>(ioReq->iocbPtr, ioReq));
    }


    void processWrite(uint64_t index, BlockReplayStats& stats) {
        BlockRequest* req = blockRequestVec_.at(index);
        uint64_t offset = req->getOffset();
        uint64_t size = req->getSize();
        uint64_t frontMisalignment = req->getFrontAlignment();
        if (frontMisalignment > 0) {
            // updateHit(req, frontMisalignment);
            uint64_t startPage = req->getStartPage();
            uint64_t startPageOffset = config_.replayGeneratorConfig.pageSizeBytes*startPage;
            // std::cout << folly::sformat("Front read: {},{},{}\n", startPage, startPageOffset, frontMisalignment);
            doRead(index, startPageOffset, 4096, stats);
        }
            
        uint64_t rearMisalignment = req->getRearAlignment();
        if (rearMisalignment > 0) {
            // updateHit(req, rearMisalignment);
            uint64_t endPage = req->getEndPage();
            uint64_t endPageOffset = config_.replayGeneratorConfig.pageSizeBytes*endPage;
            // std::cout << folly::sformat("Rear read: {},{},{}\n", endPage, endPageOffset, rearMisalignment);
            doRead(index, endPageOffset, 4096, stats);
        }

        doWrite(index, offset, size, stats);
        // AsyncIORequest *ioReq = new AsyncIORequest(size, backingStoreAlignment, index);
        // updateHit(req, size);
        // appendToOutputQueue(ioReq);
    }


    void processRequest(BlockReplayStats& stats) {
        while (!endExperimentFlag(stats)) {
            std::tuple<uint64_t, uint64_t, OpType> reqTuple = popFromInputQueue(stats);
            OpType op = std::get<2>(reqTuple);
            uint64_t size = std::get<1>(reqTuple);

            if (size > 0) {
                uint64_t index = getNextBlockReq(reqTuple);
                if (index < maxPendingBlockRequests) {
                    BlockRequest *req = blockRequestVec_.at(index);
                    assert(req->getOp()==op);
                    assert(req->getSize()==size);
                    switch (op) {
                        case OpType::kGet: {
                            req->startLatencyTracking(*cLatBlockReadPercentile_);
                            processRead(index, stats);
                            break;
                        }
                        case OpType::kSet: {
                            req->startLatencyTracking(*cLatBlockWritePercentile_);
                            processWrite(index, stats);
                            break;
                        }
                        default:
                            throw std::runtime_error(
                                folly::sformat("Invalid operation generated: {}", (int)op));
                    } // switch end 
                } else {
                    if (op == OpType::kGet)
                        stats.readBlockRequestDropCount++;
                    else
                        stats.writeBlockRequestDropCount++;
                }
            } 
        }
    }


    std::tuple<uint64_t, uint64_t, OpType> popFromInputQueue(BlockReplayStats& stats) {
        const std::lock_guard<std::mutex> l(inputQueueMutex_);
        std::tuple<uint64_t, uint64_t, OpType> reqTuple = std::make_tuple(0,0,OpType::kGet);
        uint64_t queueSize = inputQueue_.size();
        if (queueSize > 0) {
            reqTuple = inputQueue_.front();
            inputQueue_.pop();
        }
        if (queueSize > stats.maxInputQueueSize)
            stats.maxInputQueueSize = queueSize; 
        return reqTuple;
    }


    void appendToInputQueue(const Request& req, BlockReplayStats& stats) {
        const uint64_t lba = std::stoull(req.key);
        const uint64_t size = *(req.sizeBegin);
        const OpType op = req.getOp();
        std::tuple<uint64_t, uint64_t, OpType> queueItem = std::make_tuple(lba, size, op);

        switch (op) {
            case OpType::kGet: {
                stats.readReqCount++;
                stats.readReqBytes += size; 
                break;
            }
            case OpType::kSet: {
                stats.writeReqCount++;
                stats.writeReqBytes += size; 
                break;
            }
            default:
                throw std::runtime_error(
                    folly::sformat("Invalid operation generated: {}", (int)op));
        }

        const std::lock_guard<std::mutex> l(inputQueueMutex_);
        inputQueue_.push(queueItem);
    }


	// Replays a block trace on a cache allocator. 
	//
	// @param stats       Throughput stats
    // @param fileIndex   The index used to map to resources of the file being replayed 
	void stressByBlockReplay(BlockReplayStats& stats, uint64_t fileIndex) {
		// params for getReq
		std::mt19937_64 gen(folly::Random::rand64());
		std::optional<uint64_t> lastRequestId = std::nullopt;
        while (true) {
            try {
                const Request& req(getReq(fileIndex, gen, lastRequestId)); 
                appendToInputQueue(req, stats);
            } catch (const cachebench::EndOfTrace& ex) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(250));
        }  
        wg_->markFinish();
        stats.replayRuntime = getTestDurationNs();
        replayDoneFlagVec_.at(fileIndex) = true;
        std::cout << "log: replay terminated\n";
    }


    // fetch a request from the workload generator for a particular pool
    // @param pid             the pool id chosen for the request.
    // @param gen             the thread local random number generator to be
    // fed
    //                        to the workload generator  for constructing the
    //                        request.
    // @param lastRequestId   optional information about the last request id
    // that
    //                        was given to this thread by the workload
    //                        generator. This is used to provide continuity by
    //                        some generator implementations.
    const Request& getReq(const PoolId& pid,
                        std::mt19937_64& gen,
                        std::optional<uint64_t>& lastRequestId) {
        const Request& req(wg_->getReq(pid, gen, lastRequestId));
        return req;
    }


    private:
        static std::string genHardcodedString() {
            const std::string s = "The quick brown fox jumps over the lazy dog. ";
            std::string val;
            for (int i = 0; i < 4 * 1024 * 1024; i += s.size()) {
                val += s;
            }
            return val;
        }


    const std::string hardcodedString_;

    const StressorConfig config_; 

    // mutexes 
    mutable std::mutex timeMutex_;
    std::mutex replayDataMutex_;

    std::mutex asyncIOMutex_;

    std::mutex blockRequestQueueMutex_;

    std::mutex iocbMapMutex_;

    std::unique_ptr<GeneratorBase> wg_; 

    std::unique_ptr<CacheT> cache_;

    std::thread stressWorker_;

    // async IO 
    io_context_t* ctx_;
    int backingStoreFileHandle_;

    std::chrono::time_point<std::chrono::system_clock> startTime_;
    std::chrono::time_point<std::chrono::system_clock> endTime_;

    std::vector<BlockReplayStats> blockReplayStatVec_; 
    std::vector<bool> replayDoneFlagVec_; 

    // array of all block requests in the system 
    

    // queue of block request indexes that have not yet started processing 
    std::queue<uint64_t> blockRequestQueue_;

    std::queue<BlockRequest*> blockReqQueue_;

    std::queue<iocb*> asyncIOCompletedQueue_;

    // mapping each async block request to a index of the block request 
    std::map<iocb*, uint64_t> blockRequestMap_;

    // percentile read and write request to the backing store 
    util::PercentileStats *backingStoreReadSizeBytesPercentile_ = new util::PercentileStats();
    util::PercentileStats *backingStoreWriteSizeBytesPercentile_ = new util::PercentileStats();

    // percentile read and write block request size 
    util::PercentileStats *blockReadSizeBytesPercentile_ = new util::PercentileStats();
    util::PercentileStats *blockWriteSizeBytesPercentile_ = new util::PercentileStats();

    // percentile read and write latency of the backing store 
    util::PercentileStats *latBackingReadPercentile_ = new util::PercentileStats();
    util::PercentileStats *latBackingWritePercentile_ = new util::PercentileStats();

    //-------------------------------------------------------------------------------------------//
    uint64_t pendingIOCount_{0};
    uint64_t pendingBlockRequestCount_{0};
    uint64_t totalMissVecSize_{0};

    // input queue of block request to the storage system 
    // each request is a tuple of (lba, size, op[r/w])
    std::queue<std::tuple<uint64_t, uint64_t, OpType>> inputQueue_;
    mutable std::mutex inputQueueMutex_;

    std::vector<BlockRequest*> blockRequestVec_;
    mutable std::mutex pendingBlockRequestMutex_;

    std::queue<std::pair<AsyncIORequest*, bool>> outputQueue_;
    mutable std::mutex outputQueueMutex_;

    std::map<iocb*, AsyncIORequest*> iocbToAsyncMap_;
    mutable std::mutex iocbToAsyncMapMutex_;

    // queue of cache load requests 
    std::queue<std::tuple<uint64_t, uint64_t, OpType>> cacheLoadQueue_;

    util::PercentileStats *sLatBlockReadPercentile_ = new util::PercentileStats();
    util::PercentileStats *sLatBlockWritePercentile_ = new util::PercentileStats();

    // percetile read and write latency of each block request 
    util::PercentileStats *cLatBlockReadPercentile_ = new util::PercentileStats();
    util::PercentileStats *cLatBlockWritePercentile_ = new util::PercentileStats();
    
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
