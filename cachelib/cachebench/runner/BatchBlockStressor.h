#pragma once

#include <x86intrin.h>
#include <chrono>
#include <thread>

#include "cachelib/cachebench/runner/tscns.h"
#include "cachelib/cachebench/util/BlockRequest.h"

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/runner/BackingStore.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/BlockReplayGenerator.h"


namespace facebook {
namespace cachelib {
namespace cachebench {


template <typename Allocator>
class BatchBlockStressor : public BlockSystemStressor {
	public:
		using CacheT = Cache<Allocator>;

        // @param cacheConfig   the config to instantiate the cache instance
        // @param config        stress test config
        // @param generator     workload  generator
        BatchBlockStressor(CacheConfig cacheConfig,
                            StressorConfig config,
                            std::unique_ptr<GeneratorBase>&& generator)
                            :   config_(config), 
                                backingStore_(config),
                                lbaSizeByte_(config.blockReplayConfig.lbaSizeByte),
                                blockSizeByte_(config.blockReplayConfig.blockSizeByte),
                                maxPendingBlockRequestCount_(config.blockReplayConfig.maxPendingBlockRequestCount),
                                stressorTerminateFlag_(config.numThreads, false),
                                statsVec_(config.numThreads),
                                hardcodedString_(genHardcodedString()),
                                wg_(std::move(generator)) {
            // setup the cache 
            cache_ = std::make_unique<CacheT>(cacheConfig);

            // initiate spots in the vector containing pending block requests 
            for (uint64_t index=0; index<config_.blockReplayConfig.maxPendingBlockRequestCount; index++)
                pendingBlockReqVec_.push_back(BlockRequest(lbaSizeByte_, blockSizeByte_));
                
            // init the class for generating TSC timestamp from the CPU 
            tscns_.init();
        }


        ~BatchBlockStressor() override { 
            finish(); 
        }


        // start stressing the block storage system 
        void start() override {
            threads_ = std::thread([this] {
                // replay thread 
                std::vector<std::thread> workers;
                for (uint64_t i = 0; i < config_.numThreads; ++i) {
                    workers.push_back(std::thread([this, blockReplayStats = &statsVec_.at(i), index=i]() {
                        replay(*blockReplayStats, index);
                    }));
                }

                // block request processing thread 
                for (uint64_t i = 0; i < config_.blockReplayConfig.blockRequestProcesserThreads; ++i) {
                    workers.push_back(std::thread([this, index=i]() {
                        processBlockStorageRequestThread(index);
                    }));
                }

                // thread that tracks async IO to backing store that eventuallly return 
                for (uint64_t i = 0; i < config_.blockReplayConfig.asyncIOReturnTrackerThreads; ++i) {
                    workers.push_back(std::thread([this]() {
                        processBackingIoReturn();
                    }));
                }

                // stats tracker threads
                for (uint64_t i = 0; i < config_.numThreads; ++i) {
                    workers.push_back(std::thread([this, index=i]() {
                        statTracker(index);
                    }));
                }

                for (auto& worker : workers) {
                    worker.join();
                }
            });
        }

        // wait for worker threads to terminate and cleanup 
        void finish() override {
            if (threads_.joinable()) 
                threads_.join();
                
            wg_->markShutdown();
            cache_->clearCache(config_.maxInvalidDestructorCount);
        }

        BlockReplayStats getStat(uint64_t threadId) const override {
            return statsVec_.at(threadId);
        }

        Stats getCacheStats() const override { return cache_->getStats(); }

        util::PercentileStats* getQueueSizePercentile() const override { return queueSizePercentile_; }

        util::PercentileStats* getPendingBlockReqPercentile() const override { return pendingBlockReqPercentile_; }

        void statSnapshot(uint64_t threadId, std::ostream& ofs, std::string separator) override {
            std::lock_guard<std::mutex> l(statMutex_);

            ofs << folly::sformat("threadId={}{}", threadId, separator);
            statsVec_.at(threadId).timeElapsedNs = getPhysicalTimeElapsedNs();

            BlockReplayStats blockReplayStats = getStat(threadId);
            blockReplayStats.render(ofs, separator);

            auto cacheStats = getCacheStats();
            cacheStats.renderBlockReplay(ofs, separator);

            util::PercentileStats *queuePercentile = getQueueSizePercentile();
            util::PercentileStats *pendingBlockRequestPercentile = getPendingBlockReqPercentile();

            blockReplayStats.renderPercentile(ofs, "queueSize", "", separator, queuePercentile);
            blockReplayStats.renderPercentile(ofs, "pendingBlockReqCount", "", separator, pendingBlockRequestPercentile);
            ofs << "\n";
        }


    // Update stats on completion of backing store request 
    void updateBackingStoreStats(BackingIo backingIo, uint64_t threadId) {
        std::lock_guard<std::mutex> l(statMutex_);

        uint64_t size = backingIo.getSize();
        bool writeFlag = backingIo.getWriteFlag();

        statsVec_.at(threadId).backingReqCount++;
        statsVec_.at(threadId).backingReqByte += size; 

        uint64_t physicalTs = backingIo.getPhysicalTs();
        if (writeFlag) {
            statsVec_.at(threadId).writeBackingReqCount++;
            statsVec_.at(threadId).writeBackingReqByte += size; 
            statsVec_.at(threadId).backingWriteLatencyNsPercentile->trackValue(getCurrentTsNs() - physicalTs);
            statsVec_.at(threadId).backingWriteSizeBytePercentile->trackValue(size);
        } else {
            statsVec_.at(threadId).readBackingReqCount++;
            statsVec_.at(threadId).readBackingReqByte += size; 
            statsVec_.at(threadId).backingReadLatencyNsPercentile->trackValue(getCurrentTsNs() - physicalTs);
            statsVec_.at(threadId).backingReadSizeBytePercentile->trackValue(size);
        }

        statsVec_.at(threadId).backingReqAddAttempt = backingStore_.getBackingReqAddAttempt();
        statsVec_.at(threadId).backingReqAddFailure = backingStore_.getBackingReqAddFailure();
    }


    // Update stats on completion of block request 
    void updateBlockReqStats(uint64_t blockReqIndex, uint64_t threadId, uint64_t readHitFlag) {
        std::lock_guard<std::mutex> l(statMutex_);

        // make sure that when you update stats for a block request, it is loaded 
        if (!pendingBlockReqVec_.at(blockReqIndex).isLoaded())
            throw std::runtime_error(folly::sformat("Updating stats for a block request that is not loaded. asdsa{} \n", readHitFlag));

        statsVec_.at(threadId).blockReqCount++;

        uint64_t physicalTs = pendingBlockReqVec_.at(blockReqIndex).getPhysicalTs();
        uint64_t size = pendingBlockReqVec_.at(blockReqIndex).getSize();
        uint64_t offset = pendingBlockReqVec_.at(blockReqIndex).getOffset();
        uint64_t blockCount = pendingBlockReqVec_.at(blockReqIndex).blockCount();
        uint64_t rearMisalignByte = pendingBlockReqVec_.at(blockReqIndex).getRearMisAlignByte();
        uint64_t frontMisalignByte = pendingBlockReqVec_.at(blockReqIndex).getFrontMisAlignByte();
        bool writeFlag = pendingBlockReqVec_.at(blockReqIndex).getWriteFlag();

        if (writeFlag) {
            statsVec_.at(threadId).writeBlockReqCount++;
            statsVec_.at(threadId).writeBlockReqByte += size;
            statsVec_.at(threadId).writeMisalignByte += (frontMisalignByte + rearMisalignByte);
            // writes can lead to reads due to misalignment 
            if (blockCount > 1) {
                if (frontMisalignByte > 0)
                    statsVec_.at(threadId).readCacheReqCount++;
                if (rearMisalignByte > 0)
                    statsVec_.at(threadId).readCacheReqCount++;
            } else {
                if ((frontMisalignByte > 0) || (rearMisalignByte > 0))
                    statsVec_.at(threadId).readCacheReqCount++;
            }
            statsVec_.at(threadId).writeLatencyNsPercentile->trackValue(getCurrentTsNs() - physicalTs);
        } else {
            statsVec_.at(threadId).readBlockReqCount++;
            statsVec_.at(threadId).readBlockReqByte += size;
            statsVec_.at(threadId).readMisalignByte += (frontMisalignByte + rearMisalignByte);
            statsVec_.at(threadId).readCacheReqCount += blockCount; 
            statsVec_.at(threadId).readLatencyNsPercentile->trackValue(getCurrentTsNs() - physicalTs);
        }

        statsVec_.at(threadId).readHitCount += pendingBlockReqVec_.at(blockReqIndex).getBlockHitCount();
        statsVec_.at(threadId).readHitByte += pendingBlockReqVec_.at(blockReqIndex).getReadHitByte();
        statsVec_.at(pendingBlockReqVec_.at(blockReqIndex).getThreadId()).physicalIatNsPercentile->trackValue(pendingBlockReqVec_.at(blockReqIndex).getIatUs());

        statsVec_.at(threadId).blockReqAddAttempt = blockReqAddAttempt_;
        statsVec_.at(threadId).blockReqAddFailure = blockReqAddFailure_;
    }


    // Periodically snap statistics to a file
    void statTracker(uint64_t threadId) {
        std::string statFilePath = folly::sformat("{}/tsstat_{}.out", config_.blockReplayConfig.statOutputDir, threadId);

        std::ofstream ofs; 
        ofs.open(statFilePath, std::ofstream::out);
        while ((!isReplayDone()) || (pendingBlockReqCount_ > 0)) {
            std::this_thread::sleep_for (std::chrono::seconds(config_.blockReplayConfig.statTrackIntervalSec));
            statSnapshot(threadId, ofs, ",");
        }
        ofs.close();
        std::cout << "Stat thread terminated\n";
    }


    // Check if all replay threads have terminated
    bool isReplayDone() {
        return std::all_of(stressorTerminateFlag_.begin(), stressorTerminateFlag_.end(), [](bool b){ return b; });
    }


    // Get the time difference or latency based on the starting cycle count 
    uint64_t getLatencyNs(uint64_t reqBeginCycleCount) {
        return tscns_.tsc2ns(tscns_.rdtsc()) - tscns_.tsc2ns(reqBeginCycleCount);
    }


    uint64_t getCurrentTsNs() {
        return tscns_.tsc2ns(tscns_.rdtsc());
    }


    // Process block storage requests as the replay threads adds it to the queue 
    void processBlockStorageRequestThread(uint64_t threadId) {
        // terminate when replay is completed and there are no pending block requests in the system 
        while ((!isReplayDone()) || (pendingBlockReqCount_ > 0)) {

            BlockRequest req(lbaSizeByte_, blockSizeByte_);
            {
                std::lock_guard<std::mutex> l(blockRequestQueueMutex_);
                if (queueSize_ > 0) {
                    req = blockRequestQueue_.front();
                    blockRequestQueue_.pop();
                    queueSize_--;
                }
            }

            if (req.isLoaded()) {
                uint64_t pendingBlockRequestIndex;
                if (req.getWriteFlag()) {
                    pendingBlockRequestIndex = processBlockStorageWrite(req);
                } else {
                    pendingBlockRequestIndex = processBlockStorageRead(req);
                }

                // if pending block request is higher than the max value, it means 
                // that the block request is already completed (cache hit)
                if (pendingBlockRequestIndex < maxPendingBlockRequestCount_)
                    submitToBackingStore(pendingBlockRequestIndex);
            }
        }

        // notify backing store that its ok to terminate if no requests are pending 
        backingStore_.setReplayDone();

        // set the time when all processing was completed 
        for (uint64_t index=0; index<config_.numThreads; index++) {
            std::lock_guard<std::mutex> l(statMutex_);
            statsVec_.at(index).replayTimeNs = getPhysicalTimeElapsedNs();
        }

        std::cout << "Block request processer thread terminated \n";
    }


    // Thread tracks backing IO request that return 
    void processBackingIoReturn() {
        while ((!isReplayDone()) || pendingBlockReqCount_ > 0) {
            // pop the index of the backing store IO that has returned 
            uint64_t index = backingStore_.popFromBackingIoReturnQueue();
            if (index == config_.blockReplayConfig.maxPendingBackingStoreIoCount)
                continue; 

            // get the backing IO from backing store 
            BackingIo backingIo = backingStore_.getBackingIo(index);
            uint64_t blockReqIndex = backingIo.getBlockRequestIndex();

            // get the start and end page that is touched by the return backing IO request 
            uint64_t backingIoSize = backingIo.getSize();
            uint64_t backingIoOffset = backingIo.getOffset() + config_.blockReplayConfig.minOffset;
            uint64_t backingIoStartBlock = backingIoOffset/blockSizeByte_;
            uint64_t backingIoEndBlock = (backingIoOffset + backingIoSize - 1)/blockSizeByte_;
            bool backingIoWriteFlag = backingIo.getWriteFlag();

            std::lock_guard<std::mutex> l(pendingBlockReqMutex_);

            // make sure that the block request for which backing IO returned is not completed 
            if (pendingBlockReqVec_.at(blockReqIndex).isComplete()) 
                throw std::runtime_error("Block request already completed when backing IO returned\n");
            
            // get the start and end page of the block request 
            uint64_t blockReqStartBlock = pendingBlockReqVec_.at(blockReqIndex).getStartBlock();
            uint64_t blockReqEndBlock = pendingBlockReqVec_.at(blockReqIndex).getEndBlock();
            uint64_t reqThreadId = pendingBlockReqVec_.at(blockReqIndex).getThreadId();
            bool writeFlag = pendingBlockReqVec_.at(blockReqIndex).getWriteFlag();
            uint64_t blockReqOffset = pendingBlockReqVec_.at(blockReqIndex).getOffset();
            
            if (!writeFlag) {
                // mark the relevant page as completed without updating hit statistics 
                for (uint64_t curBlock=backingIoStartBlock; curBlock<=backingIoEndBlock; curBlock++) 
                    pendingBlockReqVec_.at(blockReqIndex).setCacheHit(curBlock - blockReqStartBlock, false);
            } else {
                if (backingIoWriteFlag) {
                    // it it is a write to backing store then set cache hit at index 1 and do not count it as a hit 
                    pendingBlockReqVec_.at(blockReqIndex).setCacheHit(1, false);
                } else {
                    if (backingIoOffset < blockReqOffset) {
                        // front misalignment read would have an offset lower than start offset of write 
                        pendingBlockReqVec_.at(blockReqIndex).setCacheHit(0, false);
                    } else {
                        // rear misalignment read would have an offset greater than end offset of write 
                        pendingBlockReqVec_.at(blockReqIndex).setCacheHit(2, false);
                    }
                }
            }

            if (pendingBlockReqVec_.at(blockReqIndex).isComplete()) {
                setKeys(blockReqStartBlock, blockReqEndBlock, reqThreadId);
                updateBlockReqStats(blockReqIndex, reqThreadId);
                pendingBlockReqVec_.at(blockReqIndex).reset();
                pendingBlockReqCount_--;
            }
            
            updateBackingStoreStats(backingIo, reqThreadId);
            backingStore_.markCompleted(index);
        }
        std::cout << "IO tracker thoughts terminated \n";
    }


    // submit backing store requests of a given block request 
    void submitToBackingStore(uint64_t pendingBlockRequestIndex) {
        BlockRequest req(lbaSizeByte_, blockSizeByte_);
        std::vector<bool> blockCompletionVec;
        {
            req = pendingBlockReqVec_.at(pendingBlockRequestIndex);
            blockCompletionVec = req.getBlockIoCompletionVec();
        }
        uint64_t threadId = req.getThreadId();
        uint64_t offset = req.getOffset();
        uint64_t size = req.getSize();
        bool writeFlag = req.getWriteFlag(); 

        uint64_t missStartBlock;
        uint64_t numMissBlock = 0;

        uint64_t startBlock = offset/blockSizeByte_;
        uint64_t endBlock = (offset + size - 1)/blockSizeByte_;

        if (writeFlag) {
            uint64_t frontAlignmentByte = offset - (startBlock * blockSizeByte_);
            uint64_t rearAlignmentByte = ((endBlock+1) * blockSizeByte_) - (offset + size);

            // there is misalignment in the front and a miss, so submit a backing read 
            if ((frontAlignmentByte > 0) & (!blockCompletionVec.at(0))) 
                backingStore_.submitBackingStoreRequest(getCurrentTsNs(),
                                                            startBlock*blockSizeByte_, 
                                                            frontAlignmentByte, 
                                                            false, 
                                                            pendingBlockRequestIndex, 
                                                            threadId);

            // submit write request 
            backingStore_.submitBackingStoreRequest(getCurrentTsNs(),
                                                        offset, 
                                                        size, 
                                                        true, 
                                                        pendingBlockRequestIndex, 
                                                        threadId);

            // there is misalignment in the rear and a miss, so submit a backing read 
            if ((rearAlignmentByte > 0) & (!blockCompletionVec.at(2))) 
                backingStore_.submitBackingStoreRequest(getCurrentTsNs(),
                                                            offset + size, 
                                                            rearAlignmentByte, 
                                                            false, 
                                                            pendingBlockRequestIndex, 
                                                            threadId);
        } else {
            // go through each block touched by the block request 
            for (uint64_t curBlock = startBlock; curBlock <= endBlock; curBlock++) {
                if (blockCompletionVec.at(curBlock-startBlock)) {
                    // hit 
                    if (numMissBlock > 0) {
                        backingStore_.submitBackingStoreRequest(getCurrentTsNs(),
                                                                    missStartBlock*blockSizeByte_, 
                                                                    numMissBlock*blockSizeByte_, 
                                                                    false, 
                                                                    pendingBlockRequestIndex, 
                                                                    threadId);
                        numMissBlock = 0;
                    }
                } else {
                    // miss 
                    if (numMissBlock == 0) 
                        missStartBlock = curBlock;
                    numMissBlock++;
                }
            }
            if (numMissBlock > 0) {
                backingStore_.submitBackingStoreRequest(getCurrentTsNs(),
                                                            missStartBlock*blockSizeByte_, 
                                                            numMissBlock*blockSizeByte_, 
                                                            false, 
                                                            pendingBlockRequestIndex, 
                                                            threadId);
            }

        }
    }


    // Load a request from trace with the given attribute to the list of pending block requests 
    uint64_t addToPendingBlockRequestVec(BlockRequest& req) {
        std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
        uint64_t freeBlockReqIndex = maxPendingBlockRequestCount_;
        for (int index=0; index < maxPendingBlockRequestCount_; index++) {
            if (!pendingBlockReqVec_.at(index).isLoaded()) {
                pendingBlockReqVec_.at(index).load(req.getPhysicalTs(), 
                                                    req.getTs(),
                                                    req.getIatUs(),
                                                    req.getLba(),
                                                    req.getSize(),
                                                    req.getWriteFlag(),
                                                    index, 
                                                    req.getThreadId());
                freeBlockReqIndex = index;
                break;
            }
        }

        blockReqAddAttempt_++;
        if (freeBlockReqIndex == maxPendingBlockRequestCount_)
            blockReqAddFailure_++;

        return freeBlockReqIndex; 
    }


    // Set the keys in the provided range 
    void setKeys(uint64_t startBlock, uint64_t endBlock, uint64_t threadId) {
        for (uint64_t blockId=startBlock; blockId<=endBlock; blockId++) {
            const std::string key = folly::sformat("{}{}", blockId, threadId);
            auto it = cache_->allocate(0, 
                                key, 
                                blockSizeByte_, 
                                0);
            if (it != nullptr) {
                XDCHECK(it);
                XDCHECK_LE(cache_->getSize(it), 4ULL * 1024 * 1024);
                cache_->setStringItem(it, hardcodedString_);
                cache_->insertOrReplace(it);
            } 
        }
    }


    // Checks if the block related to the LBA of a replay thread exists in cache 
    // @param blockId       the unique ID of the block being accessed
    // @param threadId      the ID of the replay thread to which the LBA belongs 
    bool blockInCache(uint64_t blockId, uint64_t threadId) {
        bool blockInCache = false;
        // the key = blockId + threadId (e.g. key = '14' + '1' = '141') to differentiate the 
        // same block being accessed by different replay threads which are two different blocks of data
        const std::string key = folly::sformat("{}{}", blockId, threadId);
        auto it = cache_->find(key);
        if (it != nullptr)     
            blockInCache = true; 
        return blockInCache;
    }


    // Process a write block storage request 
    uint64_t processBlockStorageWrite(BlockRequest req) {
        uint64_t blockRequestIndex = addToPendingBlockRequestVec(req);
        while (blockRequestIndex == maxPendingBlockRequestCount_)
            blockRequestIndex = addToPendingBlockRequestVec(req);
        
        uint64_t lba = req.getLba();
        uint64_t offset = req.getOffset();
        uint64_t size = req.getSize();
        uint64_t threadId = req.getThreadId();
        uint64_t startBlock = req.getStartBlock();
        uint64_t endBlock = req.getEndBlock();
        bool writeFlag = req.getWriteFlag();

        uint64_t frontMisalignByte = req.getFrontMisAlignByte();
        uint64_t rearMisalignByte = req.getRearMisAlignByte();

        // remove all the stale keys from the cache if they exist 
        for (int blockId = startBlock; blockId < (endBlock + 1); blockId++) {
            const std::string key = folly::sformat("{}{}", blockId, threadId);
            cache_->remove(key);
        }

        if (frontMisalignByte > 0) {
            if (blockInCache(startBlock, threadId)) {
                {
                    std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
                    pendingBlockReqVec_.at(blockRequestIndex).setCacheHit(0, true);
                }
            }
        }

        if (rearMisalignByte > 0) {
            if (blockInCache(startBlock, threadId)) {
                {
                    std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
                    pendingBlockReqVec_.at(blockRequestIndex).setCacheHit(2, true);
                }
            }
        }

        return blockRequestIndex;
    }


    // Process a read block storage request 
    // @param   req -> the read block request to be processed 
    uint64_t processBlockStorageRead(BlockRequest req) {
        uint64_t pendingBlockRequestIndex = addToPendingBlockRequestVec(req);
        while (pendingBlockRequestIndex == maxPendingBlockRequestCount_) 
            pendingBlockRequestIndex = addToPendingBlockRequestVec(req);
        
        // check if each block related to the block request is in cache 
        bool writeFlag = req.getWriteFlag();
        uint64_t size = req.getSize();
        uint64_t threadId = req.getThreadId();
        uint64_t startBlock = req.getStartBlock();
        uint64_t endBlock = req.getEndBlock();
        uint64_t missStartBlock;
        uint64_t missPageCount = 0;
        bool blockReqCompletionFlag = false; 
        std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
        for (int curBlock = startBlock; curBlock<=endBlock; curBlock++) {
            if (blockInCache(curBlock, threadId)) {
                pendingBlockReqVec_.at(pendingBlockRequestIndex).setCacheHit(curBlock - startBlock, true);;
            }
        }

        // if all the blocks are in cache, the block request is complete 
        if (pendingBlockReqVec_.at(pendingBlockRequestIndex).isComplete()) {
            updateBlockReqStats(pendingBlockRequestIndex, threadId, 0);
            pendingBlockReqVec_.at(pendingBlockRequestIndex).reset();
            pendingBlockReqCount_--;
            pendingBlockRequestIndex = maxPendingBlockRequestCount_;
        }
        
        return pendingBlockRequestIndex;
    }


    // Add a block request with the defined attributes to the queue
    void addToQueue(uint64_t physicalTs, 
                        uint64_t ts,  
                        uint64_t lba, 
                        uint64_t size, 
                        bool writeFlag, 
                        uint64_t threadId) {

        // Since the first block request will have no previous submission 
        // timestamp to refer to when computing IAT, we set it here 
        // right before the first submission is attempted. 
        if (blockReqCount_ == 0)
            updatePrevSubmitTimepoint();
        
        blockRequestQueue_.push(BlockRequest(lbaSizeByte_,
                                                blockSizeByte_,
                                                physicalTs,
                                                ts,
                                                getPhysicalIatNs(),
                                                lba,
                                                size,
                                                writeFlag,
                                                0, // this field not relevant now, so using 0 
                                                threadId));
        blockReqCount_++;
        queueSize_++;
        queueSizePercentile_->trackValue(queueSize_);
        updatePrevSubmitTimepoint();
    }


    // Read the next block request from the trace and return a BlockRequest object 
    BlockRequest loadBlockReq(uint64_t threadId, uint64_t prevTraceTsUs) {
        std::mt19937_64 gen(folly::Random::rand64());
        std::optional<uint64_t> lastRequestId = std::nullopt;
        const Request& req(wg_->getReq(0, gen, lastRequestId));

        uint64_t ts = req.timestamp;
        uint64_t lba = std::stoul(req.key);
        uint64_t size = *(req.sizeBegin);
        uint64_t iatUs = ts - prevTraceTsUs;

        bool writeFlag = false; 
        if (req.getOp() == OpType::kSet) 
            writeFlag = true; 
        
        BlockRequest blockReq = BlockRequest(lbaSizeByte_, 
                                                blockSizeByte_,
                                                getCurrentCycleCount(),
                                                ts,
                                                iatUs,
                                                lba,
                                                size,
                                                writeFlag,
                                                0,
                                                threadId);
        return blockReq;
    }


    // Get the IAT of a block request being submitted right now 
    uint64_t getPhysicalIatNs() {
        std::lock_guard<std::mutex> l(timeMutex_);
        uint64_t currentTimestampNs = tscns_.tsc2ns(tscns_.rdtsc());
        uint64_t prevTimestampNs = tscns_.tsc2ns(prevSubmitCycleCount_);
        if (currentTimestampNs <= prevTimestampNs) 
            return 0;
        else 
            return currentTimestampNs - prevTimestampNs;
    }

    
    // Get how much time has elapsed since the replay started
    uint64_t getPhysicalTimeElapsedNs() {
        std::lock_guard<std::mutex> l(timeMutex_);
        uint64_t currentTimeElapsedNs = tscns_.tsc2ns(tscns_.rdtsc());
        if (currentTimeElapsedNs <= physicalStartTimeNs_) {
            return 0;
        } else 
            return currentTimeElapsedNs - physicalStartTimeNs_;
    }


    // Update the timepoint at which last block request was submitted to the system 
    void updatePrevSubmitTimepoint() {
        std::lock_guard<std::mutex> l(timeMutex_);
        prevSubmitCycleCount_ = tscns_.rdtsc();
    }


    // Get current timestamp in terms of cycles 
    uint64_t getCurrentCycleCount() {
        return tscns_.rdtsc();
    }


    // Upate the replay start timepoint 
    void setPhysicalStartTimestampNs() {
        std::lock_guard<std::mutex> l(timeMutex_);
        physicalStartTimeNs_ = tscns_.tsc2ns(tscns_.rdtsc());
    }


    // increase the pending block request count for a specified value 
    void incPendingBlockReqCount(uint64_t value) {
        std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
        pendingBlockReqCount_ += value; 
        pendingBlockReqPercentile_->trackValue(pendingBlockReqCount_);
    }


    // Submit a batch of block requests 
    void submitBatch(std::vector<BlockRequest>& blockReqVec) {
        std::lock_guard<std::mutex> l(blockRequestQueueMutex_);
        for (BlockRequest req : blockReqVec) {
            addToQueue(getCurrentTsNs(),
                        req.getTs(), 
                        req.getLba(), 
                        req.getSize(), 
                        req.getWriteFlag(), 
                        req.getThreadId());
        }
    }


    // Syncronize the time in trace and replay 
    void timeSync(uint64_t timestampUs, uint64_t iatUs) {
        if (config_.blockReplayConfig.globalClock) {
            // sync the time elapsed in trace and replay 
            uint64_t traceTimeElapsedUs = timestampUs - traceBeginTimestampUs_;
            while (traceTimeElapsedUs*1000 > getPhysicalTimeElapsedNs());
        } else {
            // simulate the IAT of the request 
            while (iatUs*1000 > getPhysicalIatNs());
        }
    }


    // Load the next batch of block requests in the vector provided and return the first 
    // request of the next batch. 
    // @param   blockReqVec -> vector containing the first request of the next barch  
    BlockRequest loadBlockReqBatch(std::vector<BlockRequest>& blockReqVec, uint64_t threadId) {
        BlockRequest nextBlockReq(lbaSizeByte_, blockSizeByte_);
        try {
            if (blockReqVec.size() == 0) { // first request so there is no starting block request for this batch 
                blockReqVec.push_back(loadBlockReq(threadId, 0));
                // set IAT as 0 for the first request to issue it as soon as possible 
                blockReqVec.back().setIatUs(0);
                // store the starting timestamp in the trace to evaluate time elapsed later 
                traceBeginTimestampUs_ = blockReqVec.back().getTs();
                // since the first request is read start the replay timer 
                setPhysicalStartTimestampNs();
            }

            // compute IAT of follwing request and add to batch if IAT is less than minSleepTimeUs
            nextBlockReq = loadBlockReq(threadId, blockReqVec.back().getTs());
            uint64_t traceIatUs = nextBlockReq.getTs() - blockReqVec.back().getTs();
            while (traceIatUs < config_.blockReplayConfig.minSleepTimeUs) { // batch requests together if IAT is low 
                blockReqVec.push_back(nextBlockReq);
                nextBlockReq = loadBlockReq(threadId, blockReqVec.back().getTs());
                traceIatUs = nextBlockReq.getTs() - blockReqVec.back().getTs();
            }
        } catch (const cachebench::EndOfTrace& ex) {
            if (nextBlockReq.isLoaded())
                nextBlockReq.reset();
        }
        return nextBlockReq;
    }


    // Replays a block trace on a Cachelib cache and files in backing store. 
    //
    // @param stats       Block replay stats 
    // @param fileIndex   The index used to map to resources of the file being replayed 
    void replay(BlockReplayStats& stats, uint64_t threadId) {
        std::cout << folly::sformat("Block trace replay thread {} initiated\n", threadId);
        BlockRequest nextBlockReq(lbaSizeByte_, blockSizeByte_); // next block request to submit 
        std::vector<BlockRequest> blockReqVec; // current batch of block requests to submit 
        do {
            // loads the vector with a batch of block request to submit and returns the next block request 
            nextBlockReq = loadBlockReqBatch(blockReqVec, threadId);  
            if (blockReqVec.size() > 0) {
                // time needs to be synced based on the first request of the batch 
                timeSync(blockReqVec.front().getTs(), blockReqVec.front().getIatUs());
                submitBatch(blockReqVec);
                incPendingBlockReqCount(blockReqVec.size());
                // now the next request to be submitted is the first request of the 
                // next batch so clear the vector and add the next block request to the 
                // vector 
                blockReqVec.clear();
                blockReqVec.push_back(nextBlockReq);
            }
        } while (nextBlockReq.isLoaded());

        stats.replayTimeNs = getPhysicalTimeElapsedNs();
        wg_->markFinish();
        stressorTerminateFlag_.at(threadId) = true;     

        std::cout << "Replay thread terminated\n"; 
    }


    // generate the data to be used when loading blocks to cache 
    static std::string genHardcodedString() {
        const std::string s = "The quick brown fox jumps over the lazy dog. ";
        std::string val;
        for (int i = 0; i < 4 * 1024 * 1024; i += s.size()) {
            val += s;
        }
        return val;
    }


    // stressor config and some configuration params that we store in the class 
    const StressorConfig config_; 
    uint64_t lbaSizeByte_;
    uint64_t blockSizeByte_;
    uint64_t maxPendingBlockRequestCount_;

    // worker threads doing replay, block request processing, async IO return processing, stat printing
    std::thread threads_;

    // the string used as data when data is sent to cache 
    const std::string hardcodedString_;

    // NOTE: we have only tried this stressor with BlockReplayGenerator 
    std::unique_ptr<GeneratorBase> wg_; 

    // flag indicating whether each replay thread has terminated 
    std::vector<bool>stressorTerminateFlag_;

    // vector of pending block requests in the system 
    // adjust maxPendingBlockRequestCount in config_.blockReplayConfig to adjust the size 
    mutable std::mutex pendingBlockReqMutex_;
    uint64_t pendingBlockReqCount_ = 0;
    uint64_t blockReqAddFailure_ = 0;
    uint64_t blockReqAddAttempt_ = 0;
    std::vector<BlockRequest> pendingBlockReqVec_;
    util::PercentileStats *pendingBlockReqPercentile_ = new util::PercentileStats();

    // cache and backing store make up a block storage system 
    BackingStore backingStore_;
    std::unique_ptr<CacheT> cache_;

    // vector of stats corresponding to each replay thread 
    mutable std::mutex statMutex_;
    std::vector<BlockReplayStats> statsVec_;
    std::vector<std::ofstream> statsFileStreamVec_;

    // FIFO queue of block requests in the system waiting to be processed 
    // processing threads pop this queue to get the request to process once free 
    mutable std::mutex blockRequestQueueMutex_;
    std::queue<BlockRequest> blockRequestQueue_;
    uint64_t blockReqCount_ = 0;
    uint64_t queueSize_ = 0;
    util::PercentileStats *queueSizePercentile_ = new util::PercentileStats();

    // Variables related to time sync  
    mutable std::mutex timeMutex_;
    TSCNS tscns_; // class to generate TSC timestamps from CPU
    uint64_t traceBeginTimestampUs_;
    uint64_t prevSubmitCycleCount_;
    uint64_t physicalStartTimeNs_;

};


}
}
}