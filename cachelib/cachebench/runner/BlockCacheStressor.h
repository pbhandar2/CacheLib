#pragma once

#include <folly/Random.h>
#include <folly/TokenBucket.h>
#include <libaio.h>

#include <atomic>
#include <cstddef>
#include <iostream>
#include <memory>
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

namespace facebook {
namespace cachelib {
namespace cachebench {

constexpr uint32_t maxPendingBlockRequests = 4096;
constexpr uint32_t maxConcurrentIO = 16384;
constexpr uint32_t backingStoreAlignment = 512;

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
            blockReplayStats_(config_.numThreads),
            wg_(std::move(generator)),
            hardcodedString_(genHardcodedString()),
            replayThreadTerminated_(config_.numThreads),
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
            for (uint64_t i = 0; i < config_.numThreads; ++i) {
                workers.push_back(std::thread([this, blockReplayStats = &blockReplayStats_.at(i), index=i]() {
                    stressByBlockReplay(*blockReplayStats, index);
                }));
            }
            workers.push_back(
                std::thread([this, blockReplayStats = &blockReplayStats_.at(0)]() {
                asyncIOTracker(*blockReplayStats);
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
        for (const auto& stats : blockReplayStats_) {
            res += stats;
        }
        return res;
    }


    uint64_t getTestDurationNs() const override {
        std::lock_guard<std::mutex> l(timeMutex_);
        return std::chrono::nanoseconds{std::min(std::chrono::system_clock::now(), endTime_) - startTime_}
                .count();
    }


    util::PercentileStats* getBackingStoreReadLatencyStat() const override {
        return backingStoreReadLatencyNs_;
    }


    util::PercentileStats* getBackingStoreWriteLatencyStat() const override {
        return backingStoreWriteLatencyNs_;
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
        ++stats.loadCount;
        auto it = cache_->allocate(0, 
                            key, 
                            config_.replayGeneratorConfig.pageSizeBytes, 
                            0);

        if (it == nullptr) {
            ++stats.loadPageFailure;
        } else {
            populateItem(it);
            cache_->insertOrReplace(it);
        }
    }


    // function called when an async IO completes
    void asyncCompletionCallback(iocb* iocbPtr, BlockReplayStats& stats) {
        const std::lock_guard<std::mutex> lock(asyncDataUpdateMutex_);
        std::map<iocb*, uint64_t>::iterator itr = blockRequestMap_.find(iocbPtr);
        if (itr != blockRequestMap_.end()) {  
            uint64_t index = itr->second;
            if (index >= blockRequestVec_.size()) {
                throw std::runtime_error(
                    folly::sformat("AsyncIoCompletionCallback -> Index: {}, Size: {}, Pointer: {} \n", index, blockRequestVec_.size(), iocbPtr));
            }

            blockRequestVec_.at(index)->ioCompleted(iocbPtr);  
            if (blockRequestVec_.at(index)->isRequestProcessed()) {
                uint64_t startPage = blockRequestVec_.at(index)->getStartPage();
                uint64_t endPage = blockRequestVec_.at(index)->getEndPage();
                for(uint64_t pageKey = startPage; 
                        pageKey <= endPage;
                        pageKey++) {
                    
                    stats.totalIOProcessed += config_.replayGeneratorConfig.pageSizeBytes;
                    const std::string strkey = std::to_string(pageKey);
                    if (blockRequestVec_.at(index)->getOp() == OpType::kGet) {
                        if (blockRequestVec_.at(index)->checkKeyMiss(pageKey)) {
                            std::cout << "LOADING KEYYYYY\n";
                            loadKey(strkey, stats);
                        }
                        else {
                            std::cout << "RECORDDDD\n";
                            ++stats.readPageHitCount;
                            cache_->recordAccess(strkey);
                        }
                    } else if (blockRequestVec_.at(index)->getOp() == OpType::kSet) {
                        stats.writePageCount++;
                        loadKey(strkey, stats);
                    } else {
                        throw std::runtime_error(
                            folly::sformat("Operation not supported, only read and write \n"));
                    }
                    
                }
                delete blockRequestVec_.at(index);
                blockRequestVec_.at(index) = nullptr;
            } 

            blockRequestMap_.erase(itr);
            pendingIOCount_ --;

        } else {
            throw std::runtime_error(
                folly::sformat("No mapping to index found for the IOCB pointer: {} \n", iocbPtr));
        }
    }


    // track the list of async IO requests 
    void asyncIOTracker(BlockReplayStats& stats) {
        struct io_event* events = new io_event[maxConcurrentIO];
        struct timespec timeout;
        timeout.tv_sec=0;
        timeout.tv_nsec=1000000; // 1 ms
        while (true) {
            int ret = io_getevents(*ctx_, 1, maxConcurrentIO, events, &timeout);
            if (ret > 0) {
                for (int eindex=0; eindex<ret; eindex++) {
                    iocb *retiocb = events[eindex].obj;
                    uint64_t size = retiocb->u.v.nr;
                    int res = events[eindex].res;
                    if (size != res) {
                        throw std::runtime_error(folly::sformat("IO Error: Size requested: {} Size returned: {} Res2: {}\n", size, res, events[eindex].res2));
                    }
                    asyncCompletionCallback(retiocb, stats);
                }
            }
            const bool all_terminated = std::all_of(std::begin(replayThreadTerminated_), 
                                                    std::begin(replayThreadTerminated_)+config_.numThreads, 
                                                    []( const bool v){ return v; } );
            if ((all_terminated)) {
                break;
            }
        }
    }


    // get an unused index in the array of BlockRequest objects 
    size_t getFreeIndexForBlockRequest(uint64_t offset, uint64_t size, OpType op, uint64_t pageSize) {
        const std::lock_guard<std::mutex> lock(asyncDataUpdateMutex_);
        size_t index = maxPendingBlockRequests;
        for (size_t reqIndex=0; reqIndex<blockRequestVec_.size(); reqIndex++) {
            if (blockRequestVec_.at(reqIndex) == nullptr) {
                blockRequestVec_.at(reqIndex) = new BlockRequest(offset, size, op, pageSize);
                index = reqIndex;
                break;
            }
        }
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
    void backingRead(AsyncIORequest *asyncReq, size_t offset, size_t size) {
        pendingIOCount_++;
        io_prep_pread(asyncReq->iocbPtr,
            backingStoreFileHandle_,
            (void*) asyncReq->buffer,
            asyncReq->size,
            offset);
        int ret = io_submit(*ctx_, 1, &asyncReq->iocbPtr);
        if (ret < 1) {
            throw std::runtime_error(
                folly::sformat("Error in function io_submit. Return={}\n", ret));
        }
    }


    // submit asyn write IO request to the backing store 
    void backingWrite(AsyncIORequest *asyncReq, size_t offset, size_t size) {
        pendingIOCount_++;
        io_prep_pwrite(asyncReq->iocbPtr,
            backingStoreFileHandle_,
            (void*) asyncReq->buffer,
            asyncReq->size,
            offset);
        int ret = io_submit(*ctx_, 1, &asyncReq->iocbPtr);
        if (ret < 1) {
            throw std::runtime_error(
                folly::sformat("Error in function io_submit. Return={}\n", ret));
        }
    }


    // simulate IO to the backing store on a read miss 
    void readMiss(uint64_t index, uint64_t offset, size_t size) {
        const std::lock_guard<std::mutex> lock(asyncDataUpdateMutex_);
        AsyncIORequest *ioReq = blockRequestVec_.at(index)->miss(size);
        ioReq->startLatencyTracking(*backingStoreReadLatencyNs_);
        backingRead(ioReq, offset, size);
        if (index >= blockRequestVec_.size()) {
            throw std::runtime_error(
                folly::sformat("readMiss->Index: {}, Size: {} \n", index, blockRequestVec_.size()));
        }
        blockRequestMap_.insert(std::pair<iocb*, uint64_t>(ioReq->iocbPtr, index));
    }


    // simulate a write to the backing store 
    void write(uint64_t index, uint64_t offset, size_t size) {
        const std::lock_guard<std::mutex> lock(asyncDataUpdateMutex_);
        AsyncIORequest *ioReq = blockRequestVec_.at(index)->miss(size);
        ioReq->startLatencyTracking(*backingStoreWriteLatencyNs_);
        backingWrite(ioReq, offset, size);
        if (index >= blockRequestVec_.size()) {
            throw std::runtime_error(
                folly::sformat("Index: {}, Size: {} \n", index, blockRequestVec_.size()));
        }
        blockRequestMap_.insert(std::pair<iocb*, uint64_t>(ioReq->iocbPtr, index));
    }


    // process a read block request
    void processBlockRead(uint64_t index, BlockReplayStats& stats) {
        BlockRequest *req = blockRequestVec_.at(index);

        // start tracking time taken to complete a read block request 
        req->startLatencyTracking(*blockReadLatencyNs_);
        uint64_t reqSize = req->getSize();

        // block read stats 
        ++stats.readReqCount;
        stats.readReqBytes += reqSize;

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

        // submit async IO requests based on cache misses 
        uint64_t missSize = 0;
        std::vector<std::tuple<uint64_t, uint64_t>> cacheMissVec = getCacheMiss(req, stats);
        for (std::tuple<uint64_t, size_t> miss : cacheMissVec) {
            readMiss(index, std::get<0>(miss), std::get<1>(miss));
            missSize += std::get<1>(miss);
        }

        // the remaining bytes that were not misses were cache hits 
        req->hit(req->getTotalIo()-missSize);

        if (req->isRequestProcessed()) {
            delete blockRequestVec_.at(index);
            blockRequestVec_.at(index) = nullptr;
        }

        if ((frontMisAlignment == 0) && (rearMisAlignment == 0)) {
            stats.readAlignedCount += 1;
        }
    }


    // process a write block request
    void processBlockWrite(uint64_t index, BlockReplayStats& stats) {
        BlockRequest *req = blockRequestVec_.at(index);
        
        // start tracking time taken to complete a write block request 
        req->startLatencyTracking(*blockWriteLatencyNs_);
        uint64_t reqSize = req->getSize();

        // block write stats 
        ++stats.writeReqCount;
        stats.writeReqBytes += reqSize;

        // read request due to misalignment 
        uint64_t frontMisAlignment = req->getFrontMisAlignment(backingStoreAlignment);
        if (frontMisAlignment > 0) {
            readMiss(index, req->getStartPage()*req->getPageSize(), frontMisAlignment);

            stats.writeMisalignmentCount += 1;
            stats.misalignmentBytes += frontMisAlignment;
        } 

        write(index, req->getOffset(), req->getSize());

        // read request due to misalignment 
        uint64_t rearMisAlignment = req->getRearMisAlignment(backingStoreAlignment);
        if (rearMisAlignment > 0) {
            readMiss(index, req->getOffset()+req->getSize(), rearMisAlignment);

            stats.writeMisalignmentCount += 1;
            stats.misalignmentBytes += rearMisAlignment;
        } 

        if (req->isRequestProcessed()) {
            delete blockRequestVec_.at(index);
            blockRequestVec_.at(index) = nullptr;
        }

        if ((frontMisAlignment == 0) && (rearMisAlignment == 0)) {
            stats.writeAlignedCount += 1;
        }
    }   


	// Runs a number of operations on the cache allocator. The actual
	// operations and key/value used are determined by the workload generator
	// initialized.
	//
	// Throughput and Hit/Miss rates are tracked here as well
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
                OpType op = req.getOp();
                uint64_t offset = std::stoul(req.key) * config_.replayGeneratorConfig.traceBlockSizeBytes;
                uint64_t size = *(req.sizeBegin);

                ++stats.blockReqCount; 
                stats.reqBytes += size; 
                
                std::cout << folly::sformat("Count: {} ({}/{}), Off: {}, Size: {}, Pending: {}, Hit: {} LoadFail: {} \n", 
                                                stats.blockReqCount,
                                                stats.readReqCount,
                                                stats.writeReqCount,
                                                offset,
                                                size, 
                                                pendingIOCount_,
                                                stats.readPageHitCount,
                                                stats.loadPageFailure);

                switch (op) {
                case OpType::kGet: {
                    size_t blockReqIndex = getFreeIndexForBlockRequest(offset, 
                                                size, 
                                                op, 
                                                config_.replayGeneratorConfig.pageSizeBytes);

                    if (blockReqIndex < maxPendingBlockRequests) {
                        processBlockRead(blockReqIndex, stats);
                    } else {
                        stats.readBlockRequestFailure++;
                    }

                    break;
                }
                case OpType::kSet: { 
                    size_t blockReqIndex = getFreeIndexForBlockRequest(offset, 
                                                size, 
                                                op, 
                                                config_.replayGeneratorConfig.pageSizeBytes);
                    
                    if (blockReqIndex < maxPendingBlockRequests) {
                        processBlockWrite(blockReqIndex, stats);
                    } else {
                        stats.writeBlockRequestFailure++;
                    }

                    break;
                } default:
                    throw std::runtime_error(
                        folly::sformat("invalid operation generated: {}", (int)op));
                    break;
                } // switch end 
            } catch (const cachebench::EndOfTrace& ex) {
                break;
            }
        }  

        wg_->markFinish();
        while (pendingIOCount_>0) {
            std::cout << "Pending Count: " << pendingIOCount_ << " \n";
            std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // 1 sec
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5000)); // 5 sec
        replayThreadTerminated_.at(fileIndex) = true;
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
        while (true) {
            const Request& req(wg_->getReq(pid, gen, lastRequestId));
            if (config_.checkConsistency && cache_->isInvalidKey(req.key)) {
                continue;
            }
            return req;
        }
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

    const StressorConfig config_; // config for the stress run

    std::vector<BlockReplayStats> blockReplayStats_; // thread local stats

    std::vector<bool> replayThreadTerminated_; // whether the repaly thread should be terminated

    std::unique_ptr<GeneratorBase> wg_; // workload generator

    // string used for generating random payloads
    const std::string hardcodedString_;

    std::unique_ptr<CacheT> cache_;

    // main stressor thread
    std::thread stressWorker_;

    // mutex to protect reading the timestamps.
    mutable std::mutex timeMutex_;

    // start time for the stress test
    std::chrono::time_point<std::chrono::system_clock> startTime_;

    // time when benchmark finished. This is set once the benchmark finishes
    std::chrono::time_point<std::chrono::system_clock> endTime_;

    io_context_t* ctx_;
    uint64_t pendingIOCount_{0};

    int backingStoreFileHandle_;
    std::mutex asyncDataUpdateMutex_;

    std::vector<BlockRequest*> blockRequestVec_;
    std::map<iocb*, uint64_t> blockRequestMap_;

    // TODO: percentile read and write request to the backing store 

    // TODO: percentile read and write block request size 

    // percentile read and write latency of the backing store 
    util::PercentileStats *backingStoreReadLatencyNs_ = new util::PercentileStats();
    util::PercentileStats *backingStoreWriteLatencyNs_ = new util::PercentileStats();

    // percetile read and write latency of each block request 
    util::PercentileStats *blockReadLatencyNs_ = new util::PercentileStats();
    util::PercentileStats *blockWriteLatencyNs_ = new util::PercentileStats();
    
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
