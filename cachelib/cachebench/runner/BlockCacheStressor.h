#pragma once

#include <folly/Random.h>
#include <folly/TokenBucket.h>
#include <libaio.h>

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
constexpr uint64_t maxPendingBlockRequests = 65539;
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
            for (uint64_t i = 0; i<config_.replayGeneratorConfig.processorThreadCount; ++i) {
                // process block request thread  
                workers.push_back(
                    std::thread([this, blockReplayStats = &blockReplayStatVec_.at(0)]() {
                    processBlockRequest(*blockReplayStats);
                }));
            }
            for (uint64_t i = 0; i<config_.replayGeneratorConfig.asyncIOTrackerThreadCount; ++i) {
                // async IO tracker 
                workers.push_back(
                    std::thread([this, blockReplayStats = &blockReplayStatVec_.at(0)]() {
                    asyncIOTracker(*blockReplayStats);
                }));
            }
            workers.push_back(
                std::thread([this, blockReplayStats = &blockReplayStatVec_.at(0)]() {
                    printStatThread(*blockReplayStats);
            }));
            // for (uint64_t i = 0; i<1; ++i) {
            //     // async IO tracker 
            //     workers.push_back(
            //         std::thread([this, blockReplayStats = &blockReplayStatVec_.at(0)]() {
            //         processCompletedAsyncIO(*blockReplayStats);
            //     }));
            // }
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


    util::PercentileStats* getBlockReadLatencyPercentile() const override {
        return blockReadLatencyNsPercentile_;
    }


    util::PercentileStats* getBlockWriteLatencyPercentile() const override {
        return blockWriteLatencyNsPercentile_;
    }


    util::PercentileStats* getBackingStoreReadLatencyPercentile() const override {
        return backingStoreReadLatencyNsPercentile_;
    }


    util::PercentileStats* getBackingStoreWriteLatencyPercentile() const override {
        return backingStoreWriteLatencyNsPercentile_;
    }


    util::PercentileStats* getBlockReadSizePercentile() const override {
        return blockReadSizeBytesPercentile_;
    }


    util::PercentileStats* getBlockWriteSizePercentile() const override {
        return blockWriteSizeBytesPercentile_;
    }


    void printStatThread(BlockReplayStats& stats) {
        while (!isReplayDone()) {
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


    // increment pending IO count if possible 
    bool incPendingIOCount() {
        bool spaceAvailable = true;
        if (pendingIOCount_ > maxConcurrentIO) {
            spaceAvailable = false;
        } else {
            pendingIOCount_++;
        }
        return spaceAvailable;
    }

    
    // get how many block requests are currently pending 
    uint64_t getCurrentPendingBlockRequestCount() {
        uint64_t pendingBlockRequestCount = 0;
        for (uint64_t i=0; i<maxPendingBlockRequests; i++) {
            if (blockRequestVec_.at(i) != nullptr) {
                pendingBlockRequestCount++;
            } 
        }
        return pendingBlockRequestCount;
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


    uint64_t findAndDeleteMap(iocb* iocbPtr) {
        const std::lock_guard<std::mutex> l(iocbMapMutex_);
        std::map<iocb*, uint64_t>::iterator itr = blockRequestMap_.find(iocbPtr);
        uint64_t index;
        if (itr != blockRequestMap_.end()) {  
            index = itr->second;
            blockRequestMap_.erase(itr);
        } else {
            throw std::runtime_error(
                folly::sformat("No mapping to index found for the IOCB pointer: {} \n", iocbPtr));
        }
        pendingIOCount_ --;
        return index;
    }


    // function called when an async IO completes
    void asyncCompletionCallback(iocb* iocbPtr, BlockReplayStats& stats) {
        // std::cout << "async IO completion callback \n"; 
        uint64_t blockReqIndex = findAndDeleteMap(iocbPtr); 
        if (blockRequestVec_.at(blockReqIndex) == nullptr) {
            throw std::runtime_error(
                folly::sformat("Index already deleted -> {}, Size: {}, Pointer: {} \n", 
                                blockReqIndex, blockRequestVec_.size(), iocbPtr));
        }
        
        // lock access to array
        // const std::lock_guard<std::mutex> l(replayDataMutex_); 

        BlockRequest* req = blockRequestVec_.at(blockReqIndex);
        { 
            const std::lock_guard<std::mutex> l(asyncIOMutex_);
            // std::cout << folly::sformat("Calling IO complete on {} \n", this);
            req->ioCompleted(iocbPtr); 
            // std::cout << folly::sformat("IO Completed called {} \n", this);
            // req->printStatus();
            req->checkIfRequestProcessed();
        
            if (req->isRequestProcessed()) {
                uint64_t startPage = req->getStartPage();
                uint64_t endPage = req->getEndPage();
                // std::cout << folly::sformat("Req {} is processed! \n", req);
                for (uint64_t pageKey = startPage; pageKey <= endPage; pageKey++) {
                    const std::string strkey = std::to_string(pageKey);
                    if (req->getOp() == OpType::kGet) {
                        if (req->checkKeyMiss(pageKey)) {
                            loadKey(strkey, stats);
                        }
                        else {
                            stats.readPageHitCount++;
                            cache_->recordAccess(strkey);
                        }
                    } else if (req->getOp() == OpType::kSet) {
                        stats.writePageCount++;
                        loadKey(strkey, stats);
                    } else {
                        throw std::runtime_error(
                            folly::sformat("Operation not supported, only read and write \n"));
                    }
                }
                const std::lock_guard<std::mutex> l(replayDataMutex_);
                delete req;
                blockRequestVec_.at(blockReqIndex) = nullptr;
                pendingBlockRequestCount_--;
            }
        }
    }


    // check if all replay threads and pending IOs have completed 
    bool isReplayDone() {
        const bool replayCompleted = std::all_of(std::begin(replayDoneFlagVec_), 
                                                std::begin(replayDoneFlagVec_)+config_.numThreads, 
                                                []( const bool v){ return v; } );
        
        return (replayCompleted) && (pendingIOCount_ == 0) && (getCurrentQueueSize()==0);
    }


    // track the list of async IO requests 
    void asyncIOTracker(BlockReplayStats& stats) {
        struct io_event* events = new io_event[maxConcurrentIO];
        struct timespec timeout;
        timeout.tv_sec = 0;
        timeout.tv_nsec = 1000; // 1us
        while (!isReplayDone()) {
            int ret = io_getevents(*ctx_, 1, maxConcurrentIO, events, &timeout);
            if (ret > 0) {
                // std::cout << folly::sformat("Returned {} async IOs\n", ret);
                for (int eindex=0; eindex<ret; eindex++) {
                    stats.totalBackingStoreIOReturned++;
                    iocb *retiocb = events[eindex].obj;
                    uint64_t size = retiocb->u.v.nr;
                    int res = events[eindex].res;
                    if (size != res) {
                        std::map<iocb*, uint64_t>::iterator itr = blockRequestMap_.find(retiocb);
                        uint64_t offset = 0;
                        if (itr != blockRequestMap_.end()) {  
                            // found the mapping 
                            uint64_t index = itr->second;
                            offset = blockRequestVec_.at(index)->getOffset();
                        }
                        throw std::runtime_error(folly::sformat("IO Error: Size requested: {} Size returned: {} Res2: {} Offset {} \n", 
                                                                    size, res, events[eindex].res2,
                                                                    offset));
                    }
                    //asyncIOCompletedQueue_.push(retiocb);
                    asyncCompletionCallback(retiocb, stats);
                }
            }
        }
        std::cout << "log: async IO tracker thread terminated \n";
    }


    // get an unused index in the array of BlockRequest objects 
    uint64_t loadBlockRequest(uint64_t offset, 
                            uint64_t size, 
                            OpType op, 
                            uint64_t pageSize) {

        const std::lock_guard<std::mutex> l(replayDataMutex_);
        uint64_t index = maxPendingBlockRequests;
        for (uint64_t reqIndex=0; reqIndex<blockRequestVec_.size(); reqIndex++) {
            if (blockRequestVec_.at(reqIndex) == nullptr) {
                blockRequestVec_.at(reqIndex) = new BlockRequest(offset, 
                                                                    size, 
                                                                    op, 
                                                                    pageSize, 
                                                                    backingStoreAlignment);
                index = reqIndex;
                break;
            }
        }

        if (index < maxPendingBlockRequests)
            blockRequestQueue_.push(index);

        return index;
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
        if (incPendingIOCount()) {
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
        } else {
            
        }
    }


    // submit asyn write IO request to the backing store 
    void submitAsyncWrite(AsyncIORequest *asyncReq, uint64_t offset, uint64_t size) {
        pendingIOCount_++;
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


    // simulate IO to the backing store on a read miss 
    void setupAsyncRead(uint64_t index, uint64_t offset, uint64_t size) {
        // lock access to map and array of BlockRequest objects 
        const std::lock_guard<std::mutex> l(iocbMapMutex_);

        // create an AsyncIORequest object of specified @size 
        AsyncIORequest *ioReq = blockRequestVec_.at(index)->miss(size, backingStoreAlignment);
        ioReq->startLatencyTracking(*backingStoreReadLatencyNsPercentile_);
        submitAsyncRead(ioReq, offset, size);
        blockRequestMap_.insert(std::pair<iocb*, uint64_t>(ioReq->iocbPtr, index));
    }


    // simulate a write to the backing store 
    void setupAsyncWrite(uint64_t index, uint64_t offset, uint64_t size) {
        // lock access to map and array of BlockRequest objects 
        const std::lock_guard<std::mutex> l(iocbMapMutex_);

        // create a write async IO request 
        AsyncIORequest *ioReq = blockRequestVec_.at(index)->miss(size, backingStoreAlignment);
        ioReq->startLatencyTracking(*backingStoreWriteLatencyNsPercentile_);

        // submit the write async IO 
        submitAsyncWrite(ioReq, offset, size);

        // map the iocb pointer to the index of the block request 
        blockRequestMap_.insert(std::pair<iocb*, uint64_t>(ioReq->iocbPtr, index));
    }


    // process a read block request
    void processBlockRead(uint64_t index, BlockReplayStats& stats) {
        BlockRequest *req = blockRequestVec_.at(index);

        // start tracking time taken to complete a read block request 
        req->startLatencyTracking(*blockReadLatencyNsPercentile_);
        uint64_t reqSize = req->getSize();

        // block read stats 
        stats.readReqCount++;
        stats.readReqBytes += reqSize;
        blockReadSizeBytesPercentile_->trackValue(reqSize);

        // update block alignment stats 
        uint64_t frontMisAlignment = req->getFrontMisAlignment(backingStoreAlignment);
        if (frontMisAlignment > 0) {
            stats.readMisalignmentCount += 1;
            stats.misalignmentBytes += frontMisAlignment;
        }

        uint64_t rearMisAlignment = req->getRearMisAlignment(backingStoreAlignment);
        if (rearMisAlignment > 0) {
            stats.readMisalignmentCount += 1;
            stats.misalignmentBytes += rearMisAlignment;
        }

        // track if request is aligned 
        if ((frontMisAlignment == 0) && (rearMisAlignment == 0)) {
            stats.readAlignedCount += 1;
        }

        // submit async IO requests based on cache misses 
        uint64_t missSize = 0;
        std::vector<std::tuple<uint64_t, uint64_t>> cacheMissVec = getCacheMiss(req, stats);
        if (cacheMissVec.size() > 0)
            pendingBlockRequestCount_++;
        
        for (std::tuple<uint64_t, uint64_t> miss : cacheMissVec) {
            ++stats.readBackingStoreReqCount;
            setupAsyncRead(index, std::get<0>(miss), std::get<1>(miss));
            missSize += std::get<1>(miss);
        }

        // update stats based on what portion of block request was a hit 
        if (missSize == 0) {
            stats.readPageHitCount += req->pageCount();
            stats.readBlockHitCount++;
        } else if (missSize == req->getTotalIo()) {
            ++stats.readBlockMissCount;
        } else {
            ++stats.readBlockPartialHitCount;
        }

        // update backing store stats 
        stats.totalBackingStoreIO += missSize;
        stats.totalReadBackingStoreIO += missSize;

        // the remaining bytes that were not misses were cache hits 
        if (missSize < req->getTotalIo())
            req->hit(req->getTotalIo()-missSize);
        
        stats.totalIOProcessed += (req->getTotalIo()-missSize);
        req->checkIfRequestProcessed();
        if (req->isRequestProcessed()) {
            const std::lock_guard<std::mutex> l(replayDataMutex_);
            delete blockRequestVec_.at(index);
            blockRequestVec_.at(index) = nullptr;
        } 
    }


    // process a write block request
    void processBlockWrite(uint64_t index, BlockReplayStats& stats) {

        pendingBlockRequestCount_++;
        stats.writeBackingStoreReqCount++;
        stats.writeReqCount++;

        BlockRequest *req = blockRequestVec_.at(index);
        req->startLatencyTracking(*blockWriteLatencyNsPercentile_);

        uint64_t reqSize = req->getSize();
        stats.writeReqBytes += reqSize;
        stats.totalBackingStoreIO += reqSize;
        stats.totalWriteBackingStoreIO += reqSize;
        blockWriteSizeBytesPercentile_->trackValue(reqSize);

        uint64_t frontMisAlignment = req->getFrontMisAlignment(backingStoreAlignment);
        if (frontMisAlignment > 0) {
            const std::string key = std::to_string(req->getStartPage());
            auto it = cache_->find(key, AccessMode::kRead);
            if (it == nullptr) {
                // page not in cache read from backing store 
                setupAsyncRead(index, req->getStartPage()*req->getPageSize(), frontMisAlignment);
            } else {
                // page in cache, no need for any async IO 
                stats.writeMisalignmentHitCount++;
                req->hit(frontMisAlignment);
                stats.totalIOProcessed += frontMisAlignment;
            }
            stats.writeMisalignmentCount += 1;
            stats.misalignmentBytes += frontMisAlignment;
        } 
 
        uint64_t rearMisAlignment = req->getRearMisAlignment(backingStoreAlignment);
        if (rearMisAlignment > 0) {
            const std::string key = std::to_string(req->getEndPage());
            auto it = cache_->find(key, AccessMode::kRead);
            if (it == nullptr) {
                // page not in cache read from backing store 
                setupAsyncRead(index, req->getOffset()+reqSize, rearMisAlignment);
            } else {
                // page in cache, no need for any async IO 
                stats.writeMisalignmentHitCount++;
                req->hit(rearMisAlignment);
                stats.totalIOProcessed += rearMisAlignment;
            }
            stats.writeMisalignmentCount += 1;
            stats.misalignmentBytes += rearMisAlignment;
        } 

        setupAsyncWrite(index, req->getOffset(), reqSize);

        // check if the request was prefectly aligned 
        if ((frontMisAlignment == 0) && (rearMisAlignment == 0)) {
            stats.writeAlignedCount += 1;
        }

    } 


    void printCurrentStats(BlockReplayStats& stats) {
        Stats cacheStats = cache_->getStats();

        const uint64_t numSeconds = getTestDurationNs()/static_cast<double>(1e9);
        std::cout << folly::sformat("Elasped={},",
                                    numSeconds);

        const uint64_t ramItemCount = cacheStats.getRAMItemCount();
        std::cout << folly::sformat("T1 item count={},",
                                    ramItemCount);

        const uint64_t nvmItemCount = cacheStats.getNVMItemCount();
        std::cout << folly::sformat("T2 item count={},",
                                    nvmItemCount);

        std::cout << folly::sformat("Read Drop={},Write Drop={},",
                                    stats.readBlockRequestDropCount,
                                    stats.writeBlockRequestDropCount);

        std::cout << folly::sformat("Block request count={},",
                                    stats.blockReqCount);

        std::cout << folly::sformat("Block request processed={},",
                                    stats.blockReqProcessed);

        std::cout << folly::sformat("Backing store IO count={},",
                                    stats.totalBackingStoreIO);

        std::cout << folly::sformat("Backing store IO returned={},",
                                    stats.totalBackingStoreIOReturned);

        std::cout << folly::sformat("Pending IO={},",
                                    pendingIOCount_);

        std::cout << folly::sformat("Max pending IO={},",
                                    stats.maxPendingIO);

        std::cout << folly::sformat("Block request queue size={},",
                                    getCurrentQueueSize());

        std::cout << folly::sformat("Max block request queue size={},",
                                    stats.maxQueueSize);

        const double writeRatio = stats.writeReqCount/static_cast<double>(stats.blockReqCount);
        std::cout << folly::sformat("Write request ratio={:.3f}\n \n",
                                    writeRatio);

    }  


    uint64_t getNextBlockRequest() {
        // lock access to map and array of BlockRequest objects 
        const std::lock_guard<std::mutex> l(replayDataMutex_);
        uint64_t index = maxPendingBlockRequests;
        if (getCurrentQueueSize() > 0) {
            index = blockRequestQueue_.front();
            blockRequestQueue_.pop();
        }
        return index; 
    }


    iocb* getNextCompletedAsyncIO() {
        // lock access to map and array of BlockRequest objects 
        const std::lock_guard<std::mutex> l(asyncIOMutex_);
        iocb* iocbPtr = nullptr;
        if (asyncIOCompletedQueue_.size() > 0) {
            iocbPtr = asyncIOCompletedQueue_.front();
            asyncIOCompletedQueue_.pop();
        }
        return iocbPtr; 
    }


    void processCompletedAsyncIO(BlockReplayStats& stats) {
        std::cout << "Completed ASync IO tracker\n";
        while (!isReplayDone()) {
            iocb* iocbPtr = getNextCompletedAsyncIO();
            if (iocbPtr != nullptr) {
                asyncCompletionCallback(iocbPtr, stats);
            }
        }
    }


    uint64_t getNextBlockReq() {
        const std::lock_guard<std::mutex> r_l(replayDataMutex_);
        uint64_t index = maxPendingBlockRequests;
        if (getCurrentQueueSize()>0) {
            for (uint64_t reqIndex=0; reqIndex<blockRequestVec_.size(); reqIndex++) {
                if (blockRequestVec_.at(reqIndex) == nullptr) {
                    blockRequestVec_.at(reqIndex) = blockReqQueue_.front();
                    popBlockRequestQueue();
                    index = reqIndex;
                    break;
                }
            }
        }
        return index;
    }


    uint64_t getCurrentQueueSize() {
        const std::lock_guard<std::mutex> l(blockRequestQueueMutex_);
        return blockReqQueue_.size();
    }


    uint64_t popBlockRequestQueue() {
        const std::lock_guard<std::mutex> l(blockRequestQueueMutex_);
        blockReqQueue_.pop();
    }


    void processBlockRequest(BlockReplayStats& stats) {
        std::cout << "log: process block request initiated!\n";
        while (!isReplayDone()) {
            uint64_t index = getNextBlockReq();
            if (index < maxPendingBlockRequests) {

                BlockRequest* req = blockRequestVec_.at(index);
                uint64_t size = req->getSize();
                OpType op = req->getOp();

                if (op == OpType::kGet) {
                    processBlockRead(index, stats);
                } else if (op == OpType::kSet) {
                    processBlockWrite(index, stats);
                } else {
                    throw std::runtime_error(
                        folly::sformat("Invalid operation! {} Size: {} Off: {}\n", 
                                        (int)op,
                                        size,
                                        req->getOffset()));
                }

                stats.blockReqProcessed++;
            }
            uint64_t queueSize = getCurrentQueueSize();
            if (queueSize > stats.maxQueueSize)
                stats.maxQueueSize = queueSize;
        }
        std::cout << "log: processer thread terminated \n";
    }


    void queueBlockRequest(const Request& req, BlockReplayStats& stats) {
        stats.blockReqCount++; 
        uint64_t size = *(req.sizeBegin);
        stats.reqBytes += size;         
        OpType op = req.getOp();
        uint64_t offset = std::stoul(req.key) * config_.replayGeneratorConfig.traceBlockSizeBytes;
        
        if (pendingIOCount_ > stats.maxPendingIO)
            stats.maxPendingIO = pendingIOCount_;
        
        bool queueSuccess = false; 
        uint64_t queueSize = getCurrentQueueSize();
        if (queueSize < maxBlockReqQueueSize) {
            const std::lock_guard<std::mutex> l(blockRequestQueueMutex_);
            blockReqQueue_.push(new BlockRequest(offset, 
                                                size, 
                                                op, 
                                                config_.replayGeneratorConfig.pageSizeBytes, 
                                                backingStoreAlignment));
            queueSuccess = true; 
        }
        
        switch (op) {
            case OpType::kGet: {
                stats.readReqCount++;
                stats.readReqBytes += size; 
                if (!queueSuccess) {
                    stats.readBlockRequestDropCount++;
                    stats.readBlockRequestDropBytes += size;
                }
                break;
            }
            case OpType::kSet: {
                stats.writeReqCount++;
                stats.writeReqBytes += size; 
                if (!queueSuccess) {
                    stats.writeBlockRequestDropCount++;
                    stats.writeBlockRequestDropBytes += size;
                }
                break;
            }
            default:
                throw std::runtime_error(
                    folly::sformat("Invalid operation generated: {}", (int)op));
        } // switch end 

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
                queueBlockRequest(req, stats);
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

    uint64_t pendingIOCount_{0};
    uint64_t pendingBlockRequestCount_{0};

    // array of all block requests in the system 
    std::vector<BlockRequest*> blockRequestVec_;

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
    util::PercentileStats *backingStoreReadLatencyNsPercentile_ = new util::PercentileStats();
    util::PercentileStats *backingStoreWriteLatencyNsPercentile_ = new util::PercentileStats();

    // percetile read and write latency of each block request 
    util::PercentileStats *blockReadLatencyNsPercentile_ = new util::PercentileStats();
    util::PercentileStats *blockWriteLatencyNsPercentile_ = new util::PercentileStats();

    util::PercentileStats *blockReadTotalLatNsPercentile_ = new util::PercentileStats();
    util::PercentileStats *blockWriteTotalLatNsPercentile_ = new util::PercentileStats();
    
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
