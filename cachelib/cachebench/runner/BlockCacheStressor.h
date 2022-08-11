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
            blockRequestVec_(config_.inputQueueSize),
            backingRequestVec_(maxConcurrentIO),
            bufferVec_(maxConcurrentIO),
            endTime_{std::chrono::system_clock::time_point::max()} {
        
        // setup the cache 
        cache_ = std::make_unique<CacheT>(cacheConfig);

        // open the file on disk to perform DIRECT IO
        backingStoreFileHandle_ = open(config_.diskFilePath.c_str(), O_DIRECT|O_RDWR, 0644);
        if (backingStoreFileHandle_ == -1) {
            throw std::runtime_error(
                folly::sformat("Could not open file: {} \n", config_.diskFilePath.c_str()));
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
            for (uint64_t i = 0; i<config_.processorThreadCount; ++i) {
                workers.push_back(
                    std::thread([this, blockReplayStats = &blockReplayStatVec_.at(0)]() {
                    processBlockRequest(*blockReplayStats);
                }));
            }
            
            // async IO tracker 
            workers.push_back(
                std::thread([this, blockReplayStats = &blockReplayStatVec_.at(0)]() {
                asyncIOTracker(*blockReplayStats);
            }));

            // process completed block request thread 
            for (uint64_t i = 0; i<config_.asyncIOTrackerThreadCount; ++i) {
                workers.push_back(
                    std::thread([this, blockReplayStats = &blockReplayStatVec_.at(0)]() {
                    processCompletedRequest(*blockReplayStats);
                }));
            }
            
            if (config_.statPrintDelaySec > 0)
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


    void printStatThread(BlockReplayStats& stats) {
        std::cout << "thread:init,printStats \n";
        bool replayDoneFlag = isReplayDone();
        while ((!replayDoneFlag) || (pendingBlockRequestCount_ > 0)) {
            std::this_thread::sleep_for(std::chrono::seconds(config_.statPrintDelaySec));
            printCurrentStats(stats);
            replayDoneFlag = isReplayDone();
        }
        std::cout << "thread:terminate,printStats \n";
    }


    void populateItem(ItemHandle& handle) {
        if (!config_.populateItem) {
            return;
        }
        XDCHECK(handle);
        XDCHECK_LE(cache_->getSize(handle), 4ULL * 1024 * 1024);
        cache_->setStringItem(handle, hardcodedString_);
    }


    void loadKey(const std::string key, BlockReplayStats& stats) {
        auto it = cache_->allocate(0, 
                            key, 
                            config_.pageSizeBytes, 
                            0);
        if (it == nullptr) {
            stats.loadPageFailure++;
        } else {
            populateItem(it);
            cache_->insertOrReplace(it);
        }
    }


    const std::vector<std::tuple<uint64_t, uint64_t>> getCacheMiss(BlockRequest& req) {
        uint64_t size = 0;
        uint64_t offset = 0;
        std::vector<std::tuple<uint64_t, uint64_t>> cacheMissVec;
        for (uint64_t curPage=req.getStartPage(); 
                curPage<=req.getEndPage(); 
                curPage++) {

            const std::string key = std::to_string(curPage);
            auto it = cache_->find(key, AccessMode::kRead);
            if (it == nullptr) {
                // read cache miss detected 
                req.addMissKey(curPage);

                // if no previous read cache miss detected 
                // set the starting offset
                if (size == 0) 
                    offset = curPage*config_.pageSizeBytes;

                // track the size of contigous cache misses 
                size += config_.pageSizeBytes;
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


    void submitAsyncRead(uint64_t index, uint64_t offset, uint64_t size) {
        backingRequestVec_.at(index).startLatencyTracking(*latBackingReadPercentile_);
        io_prep_pread(backingRequestVec_.at(index).iocbPtr_,
            backingStoreFileHandle_,
            (void*) bufferVec_.at(index),
            size,
            offset);
        int ret = io_submit(*ctx_, 1, &backingRequestVec_.at(index).iocbPtr_);
        if (ret < 1) {
            throw std::runtime_error(
                folly::sformat("Error in function io_submit. Return={}\n", ret));
        }
    }


    void submitAsyncWrite(uint64_t index, uint64_t offset, uint64_t size) {
        backingRequestVec_.at(index).startLatencyTracking(*latBackingWritePercentile_);
        io_prep_pwrite(backingRequestVec_.at(index).iocbPtr_,
            backingStoreFileHandle_,
            (void*) bufferVec_.at(index),
            size,
            offset);
        int ret = io_submit(*ctx_, 1, &backingRequestVec_.at(index).iocbPtr_);
        if (ret < 1) {
            throw std::runtime_error(
                folly::sformat("Error in function io_submit. Return={}\n", ret));
        }
    }


    void printCurrentStats(BlockReplayStats& stats) {
        Stats cacheStats = cache_->getStats();
        std::cout << "stat:";

        const uint64_t numSeconds = getTestDurationNs()/static_cast<double>(1e9);
        std::cout << folly::sformat("T={},",
                                    numSeconds);

        uint64_t blockReqCount = stats.readReqCount + stats.writeReqCount;
        std::cout << folly::sformat("BlockReqCount={}/{}/{},",
                                        stats.blockReqProcessed,
                                        pendingBlockRequestCount_,
                                        blockReqCount);

        std::cout << folly::sformat("MaxInputQueueSize={},",
                                        stats.maxInputQueueSize);

        std::cout << folly::sformat("MaxOutputQueueSize={},",
                                        stats.maxOutputQueueSize);
        
        std::cout << folly::sformat("MaxPendingBlock={},", stats.maxPendingReq);
        std::cout << folly::sformat("MaxPendingIO={},",
                                        stats.maxPendingIO);

        const uint64_t ramItemCount = cacheStats.getRAMItemCount();
        std::cout << folly::sformat("t1Size={},", ramItemCount);
        std::cout << folly::sformat("t1HitRate={:3.2f},",cacheStats.getHitRate());
        std::cout << "\n\n";

    }  


    bool isReplayDone() {
        return std::all_of(std::begin(replayDoneFlagVec_), 
                                                std::begin(replayDoneFlagVec_)+config_.numThreads, 
                                                []( const bool v){ return v; } );
    }


    uint64_t findIOCBToAsyncMap(iocb* iocbPtr) {
        const std::lock_guard<std::mutex> l(iocbToAsyncIndexMapMutex_);
        std::map<iocb*, uint64_t>::iterator itr = iocbToAsyncIndexMap_.find(iocbPtr);
        uint64_t index;
        if (itr != iocbToAsyncIndexMap_.end()) {  
            index = itr->second;
            iocbToAsyncIndexMap_.erase(itr);
        } else {
            throw std::runtime_error(
                folly::sformat("No mapping to index found for the IOCB pointer: {} \n", iocbPtr));
        }
        return index;
    }


    // track the list of async IO requests 
    void asyncIOTracker(BlockReplayStats& stats) {
        struct io_event* events = new io_event[maxConcurrentIO];
        struct timespec timeout;
        timeout.tv_sec = 0;
        timeout.tv_nsec = 10; // 1us
        bool replayDoneFlag = isReplayDone();
        while ((!replayDoneFlag) || (pendingBlockRequestCount_ > 0)) {
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

                    uint64_t index = findIOCBToAsyncMap(retiocb);
                    if (op == 0)
                        appendToBackingStoreReturnQueue(index, false);
                    else
                        appendToBackingStoreReturnQueue(index, true);

                }
            }
            replayDoneFlag = isReplayDone();
        }
        delete [] events;
        std::cout << "log: async IO tracker thread terminated \n";
    }


    void removeBlockRequest(uint64_t index) {
        const std::lock_guard<std::mutex> l(pendingBlockRequestMutex_);
        blockRequestVec_.at(index).reset();
        pendingBlockRequestCount_--;
    }


    void removeAsyncIORequest(uint64_t index) {
        const std::lock_guard<std::mutex> l(backingRequestMutex_);
        backingRequestVec_.at(index).reset();
        delete [] bufferVec_.at(index);
        pendingIOCount_--;
    }


    void setKeys(BlockRequest& req, BlockReplayStats& stats) {
        uint64_t startPage = req.getStartPage();
        uint64_t endPage = req.getEndPage();
        OpType op = req.getOp();
        uint64_t size = req.getSize();
        for (uint64_t curPage=startPage; curPage<=endPage; curPage++) {
            const std::string strkey = std::to_string(curPage);
            if (op == OpType::kGet) {
                if (req.checkKeyMiss(curPage)) {
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
                                                                            req.getOffset(),
                                                                            req.getLBA()));
            }
        }
    }


    void updateAsyncCompletion(uint64_t index, uint64_t size, bool writeFlag, BlockReplayStats& stats, iocb* iocbPtr) {
        BlockRequest& req = blockRequestVec_.at(index);
        req.async(size, writeFlag);
        if (req.isBlockRequestProcessed()) {
            stats.blockReqProcessed++;
            setKeys(req, stats);
            blockRequestVec_.at(index).reset();
            pendingBlockRequestCount_--;
        }
    }


    void processCompletedRequest(BlockReplayStats& stats) {
        std::cout << "thread:init,processCompletedRequest \n";
        bool replayDoneFlag = isReplayDone();
        while ((!replayDoneFlag) || (pendingBlockRequestCount_ > 0)) {
            std::pair<uint64_t, bool> backingReturnPair = popFromBackingStoreReturnQueue(stats);
            uint64_t index = std::get<0>(backingReturnPair);
            bool writeFlag = std::get<1>(backingReturnPair);
            if (index < maxConcurrentIO) {
                uint64_t blockRequestIndex = backingRequestVec_.at(index).getBlockRequestIndex();
                uint64_t size = backingRequestVec_.at(index).getSize();
                const std::lock_guard<std::mutex> l(pendingBlockRequestMutex_);
                updateAsyncCompletion(blockRequestIndex, size, writeFlag, stats, backingRequestVec_.at(index).iocbPtr_);
                removeAsyncIORequest(index);
            }
            replayDoneFlag = isReplayDone();
        }
        std::cout << "thread:terminate,processCompletedRequest \n";
    }


    std::pair<uint64_t, bool> popFromBackingStoreReturnQueue(BlockReplayStats& stats) {
        const std::lock_guard<std::mutex> l(backingStoreReturnQueueMutex_);
        std::pair<uint64_t, bool> outPair = std::make_pair(maxConcurrentIO, false);
        uint64_t queueSize = backingStoreReturnQueue_.size();
        if (queueSize > 0) {
            outPair = backingStoreReturnQueue_.front();
            backingStoreReturnQueue_.pop();
        }
        if (queueSize > stats.maxOutputQueueSize)
            stats.maxOutputQueueSize = queueSize; 
        return outPair;
    }


    void appendToBackingStoreReturnQueue(uint64_t index, bool writeFlag) {
        const std::lock_guard<std::mutex> l(backingStoreReturnQueueMutex_);
        backingStoreReturnQueue_.push(std::make_pair(index, writeFlag));
    }


    uint64_t loadAsyncIORequest(uint64_t size, 
            uint64_t backingStoreAlignment, 
            uint64_t blockRequestIndex) {
        const std::lock_guard<std::mutex> l(backingRequestMutex_);
        uint64_t loadIndex = maxConcurrentIO;
        for (uint64_t index=0; index<maxConcurrentIO; index++) {
            if (!backingRequestVec_.at(index).isDataLoaded()) {
                loadIndex = index; 
                backingRequestVec_.at(index).load(size, backingStoreAlignment, index, blockRequestIndex);
                int ret = posix_memalign((void **)&bufferVec_.at(index), backingStoreAlignment, size);
                if (ret != 0) {
                    throw std::runtime_error(
                        folly::sformat("Error in posix_memalign, return: {}\n", ret));
                }
                pendingIOCount_++;
                break;
            }
        }
        return loadIndex;
    }


    void doRead(uint64_t index, uint64_t offset, uint64_t size, BlockReplayStats& stats) {
        const std::lock_guard<std::mutex> l(iocbToAsyncIndexMapMutex_);
        stats.readBackingStoreReqCount++;
        stats.totalReadBackingStoreIO+=size;
        uint64_t asyncIOIndex = loadAsyncIORequest(size, backingStoreAlignment, index);
        while (asyncIOIndex == maxConcurrentIO)
            asyncIOIndex = loadAsyncIORequest(size, backingStoreAlignment, index);
        if (pendingIOCount_ > stats.maxPendingIO)
            stats.maxPendingIO = pendingIOCount_;
        submitAsyncRead(asyncIOIndex, offset, size);
        iocbToAsyncIndexMap_.insert(std::pair<iocb*, uint64_t>(backingRequestVec_.at(asyncIOIndex).iocbPtr_, asyncIOIndex));
        // appendToBackingStoreReturnQueue(asyncIOIndex, false);

    }


    void doWrite(uint64_t index, uint64_t offset, uint64_t size, BlockReplayStats& stats) {
        const std::lock_guard<std::mutex> l(iocbToAsyncIndexMapMutex_);
        stats.writeBackingStoreReqCount++;
        stats.totalWriteBackingStoreIO+=size;
        uint64_t asyncIOIndex = loadAsyncIORequest(size, backingStoreAlignment, index);
        while (asyncIOIndex == maxConcurrentIO)
            asyncIOIndex = loadAsyncIORequest(size, backingStoreAlignment, index);
        if (pendingIOCount_ > stats.maxPendingIO)
            stats.maxPendingIO = pendingIOCount_;
        submitAsyncWrite(asyncIOIndex, offset, size);
        iocbToAsyncIndexMap_.insert(std::pair<iocb*, uint64_t>(backingRequestVec_.at(asyncIOIndex).iocbPtr_, asyncIOIndex));
        // appendToBackingStoreReturnQueue(asyncIOIndex, true);
    }


    void processRead(BlockRequest& req, BlockReplayStats& stats) {
        uint64_t index = req.getKey();
        uint64_t offset = req.getOffset();
        uint64_t size = req.getSize();

        std::vector<std::tuple<uint64_t, uint64_t>> cacheMissVec = getCacheMiss(req);

        uint64_t cacheMissBytes = 0;
        for (std::tuple<uint64_t, uint64_t> miss : cacheMissVec) {
            cacheMissBytes += std::get<1>(miss);
        }

        uint64_t reqTotalIO = req.getTotalIO();
        assert(reqTotalIO>=cacheMissBytes);

        if (cacheMissBytes == 0) {   
            // no miss all done 
            stats.blockReqProcessed++;
            removeBlockRequest(index);
        } else {
            // need to submit IOs to disk for cache misses 
            req.hit(reqTotalIO-cacheMissBytes);
            // AsyncIORequest *ioReq = new AsyncIORequest(cacheMissBytes, backingStoreAlignment, index);
            // appendToOutputQueue(ioReq, false);
                
            for (std::tuple<uint64_t, uint64_t> miss : cacheMissVec) {
                doRead(index, 
                        std::get<0>(miss), 
                        std::get<1>(miss), 
                        stats);
            }
        }        
    }


    void processWrite(BlockRequest& req, BlockReplayStats& stats) {
        uint64_t index = req.getKey();
        uint64_t offset = req.getOffset();
        uint64_t size = req.getSize();
        uint64_t frontMisalignment = req.getFrontAlignment();
        if (frontMisalignment > 0) {
            uint64_t startPage = req.getStartPage();
            uint64_t startPageOffset = config_.pageSizeBytes*startPage;
            doRead(index, startPageOffset, config_.pageSizeBytes, stats);
        }
            
        uint64_t rearMisalignment = req.getRearAlignment();
        if (rearMisalignment > 0) {
            uint64_t endPage = req.getEndPage();
            uint64_t endPageOffset = config_.pageSizeBytes*endPage;
            doRead(index, endPageOffset, config_.pageSizeBytes, stats);
        }

        doWrite(index, offset, size, stats);
    }


    uint64_t popFromBlockInputQueue() {
        const std::lock_guard<std::mutex> l(blockRequestQueueMutex_);
        uint64_t index = config_.inputQueueSize;
        uint64_t queueSize = blockRequestQueue_.size();
        if (queueSize > 0) {
            index = blockRequestQueue_.front();
            blockRequestQueue_.pop();
        }
        return index;
    }


    uint64_t addToPendingBlockRequestVec(const Request& req, BlockReplayStats& stats) {
        const std::lock_guard<std::mutex> l(pendingBlockRequestMutex_);
        uint64_t index = config_.inputQueueSize;
        for (uint64_t reqIndex=0; reqIndex<config_.inputQueueSize; reqIndex++) {
            if (!blockRequestVec_.at(reqIndex).isDataLoaded()) {
                index = reqIndex;
                pendingBlockRequestCount_++;
                if (pendingBlockRequestCount_ > stats.maxPendingReq)
                    stats.maxPendingReq = pendingBlockRequestCount_;
                stats.blockReqCount++;

                const uint64_t lba = std::stoull(req.key);
                const uint64_t size = *(req.sizeBegin);
                const OpType op = req.getOp();
                switch (op) {
                    case OpType::kGet: {
                        stats.readReqCount++;
                        stats.readReqBytes += size; 
                        blockRequestVec_.at(reqIndex).load(lba, 
                                                            size, 
                                                            op,
                                                            config_.pageSizeBytes, 
                                                            config_.traceBlockSizeBytes,
                                                            reqIndex,
                                                            *sLatBlockReadPercentile_);
                        break;
                    }
                    case OpType::kSet: {
                        stats.writeReqCount++;
                        stats.writeReqBytes += size; 
                        blockRequestVec_.at(reqIndex).load(lba, 
                                                            size, 
                                                            op,
                                                            config_.pageSizeBytes, 
                                                            config_.traceBlockSizeBytes,
                                                            reqIndex,
                                                            *sLatBlockWritePercentile_);
                        break;
                    }
                    default:
                        throw std::runtime_error(
                            folly::sformat("Invalid operation generated: {}", (int)op));
                }
                break;
            }
        }
        return index; 
    }


    uint64_t loadBlockRequest(const Request& req, BlockReplayStats& stats) {
        uint64_t index = addToPendingBlockRequestVec(req, stats);
        if (index < config_.inputQueueSize) {
            const std::lock_guard<std::mutex> l(blockRequestQueueMutex_);
            blockRequestQueue_.push(index);
            uint64_t queueSize = blockRequestQueue_.size();
            if (queueSize > stats.maxInputQueueSize)
                stats.maxInputQueueSize = queueSize; 
        }
        return index;
    }


    void processBlockRequest(BlockReplayStats& stats) {
        bool replayCompleted = isReplayDone();
        uint64_t index = popFromBlockInputQueue();
        while ((index < config_.inputQueueSize) || (!replayCompleted)) {
            if (index < config_.inputQueueSize) {
                BlockRequest& req = blockRequestVec_.at(index);
                const OpType op = req.getOp();
                switch (op) {
                    case OpType::kGet: {
                        req.startLatencyTracking(*cLatBlockReadPercentile_);
                        processRead(req, stats);
                        break;
                    }
                    case OpType::kSet: {
                        req.startLatencyTracking(*cLatBlockWritePercentile_);
                        processWrite(req, stats);
                        break;
                    }
                    default:
                        throw std::runtime_error(
                            folly::sformat("Invalid operation generated: {}", (int)op));
                } // switch end 
            }
            replayCompleted = isReplayDone();
            index = popFromBlockInputQueue();
        }
    }


    uint64_t getBlockInputQueueSize() {
        const std::lock_guard<std::mutex> l(blockRequestQueueMutex_);
        return blockRequestQueue_.size();
    }


	// Replays a block trace on a cache allocator. 
	//
	// @param stats       Throughput stats
    // @param fileIndex   The index used to map to resources of the file being replayed 
	void stressByBlockReplay(BlockReplayStats& stats, uint64_t fileIndex) {
        std::cout << "thread:init,replay \n";
		std::mt19937_64 gen(folly::Random::rand64());
		std::optional<uint64_t> lastRequestId = std::nullopt;            
        try {
            const Request& req(getReq(fileIndex, gen, lastRequestId)); 
            std::chrono::time_point<std::chrono::system_clock> lastSubmitTime = std::chrono::system_clock::now();
            std::chrono::time_point<std::chrono::system_clock> curTime; 

            uint64_t ts = req.getTs();
            double iat;
            traceStartTime_ = ts;
            tracePrevTime_ = ts;
            do {
                ts = req.getTs();
                iat = static_cast<double>((ts - tracePrevTime_))/config_.scaleIAT;

                while (getBlockInputQueueSize()>0) {
                    curTime = std::chrono::system_clock::now();
                    if (config_.relativeTiming) {
                        auto timeSinceLastSubmit = std::chrono::duration_cast<std::chrono::microseconds>(curTime - lastSubmitTime).count();
                        if (timeSinceLastSubmit >= iat)
                            break;
                    } else {                            
                        auto timeSinceStart = std::chrono::duration_cast<std::chrono::microseconds>(curTime - startTime_).count();
                        if (timeSinceStart >= (ts-traceStartTime_))
                            break;
                    }
                }

                uint64_t index = loadBlockRequest(req, stats);
                while (index == config_.inputQueueSize) {
                    index = loadBlockRequest(req, stats);
                }
                
                const Request& req(getReq(fileIndex, gen, lastRequestId)); 
                lastSubmitTime = std::chrono::system_clock::now();
                tracePrevTime_ = ts;
            } while (true);
        } catch (const cachebench::EndOfTrace& ex) {
        }
        wg_->markFinish();
        stats.replayRuntime = getTestDurationNs();
        replayDoneFlagVec_.at(fileIndex) = true;
        std::cout << "thread:terminate,replay \n";
    }


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


    // parameters setup during stressor initiation 
    const StressorConfig config_; 
    const std::string hardcodedString_;
    std::unique_ptr<GeneratorBase> wg_; 
    std::unique_ptr<CacheT> cache_;
    std::thread stressWorker_;
    io_context_t* ctx_;
    int backingStoreFileHandle_;

    // parameters related to time 
    mutable std::mutex timeMutex_;
    std::chrono::time_point<std::chrono::system_clock> startTime_;
    std::chrono::time_point<std::chrono::system_clock> endTime_;
    uint64_t traceStartTime_;
    uint64_t tracePrevTime_;

    // tracking system statistics 
    uint64_t pendingBlockRequestCount_{0};
    uint64_t pendingIOCount_{0};
    std::vector<bool> replayDoneFlagVec_; 
    std::vector<BlockReplayStats> blockReplayStatVec_;

    // tracking percentile values 
    util::PercentileStats *backingStoreReadSizeBytesPercentile_ = new util::PercentileStats();
    util::PercentileStats *backingStoreWriteSizeBytesPercentile_ = new util::PercentileStats();

    util::PercentileStats *sLatBlockReadPercentile_ = new util::PercentileStats();
    util::PercentileStats *sLatBlockWritePercentile_ = new util::PercentileStats();

    util::PercentileStats *cLatBlockReadPercentile_ = new util::PercentileStats();
    util::PercentileStats *cLatBlockWritePercentile_ = new util::PercentileStats();

    util::PercentileStats *latBackingReadPercentile_ = new util::PercentileStats();
    util::PercentileStats *latBackingWritePercentile_ = new util::PercentileStats();

    // system data stuctures and its mutexes
    mutable std::mutex pendingBlockRequestMutex_;
    std::vector<BlockRequest> blockRequestVec_;

    mutable std::mutex blockRequestQueueMutex_;
    std::queue<uint64_t> blockRequestQueue_;

    mutable std::mutex backingRequestMutex_;
    std::vector<AsyncIORequest> backingRequestVec_;
    std::vector<char*> bufferVec_;

    mutable std::mutex backingStoreReturnQueueMutex_;
    std::queue<std::pair<uint64_t, bool>> backingStoreReturnQueue_;

    mutable std::mutex iocbToAsyncIndexMapMutex_;
    std::map<iocb*, uint64_t> iocbToAsyncIndexMap_;

};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
