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

        BatchBlockStressor(CacheConfig cacheConfig,
                            StressorConfig config,
                            std::unique_ptr<GeneratorBase>&& generator)
                            :   config_(config), 
                                backingStore_(config),
                                lbaSizeByte_(config.blockReplayConfig.lbaSizeByte),
                                blockSizeByte_(config.blockReplayConfig.blockSizeByte),
                                maxPendingBlockRequestCount_(config.blockReplayConfig.maxPendingBlockRequestCount),
                                replayCompletedFlagVec_(config.numThreads, false),
                                statsVec_(config.numThreads),
                                curBlockReqBatchVec_(config.numThreads),
                                hardcodedString_(genHardcodedString()),
                                wg_(std::move(generator)) {
            /* Constructor for BatchBlockStressor. 

            Args:
                cacheConfig: The configuration of the cache. 
                config: The configuration of the stressor. 
                generator: The workload generator to create block requests. 
            */
            tscns_.init();
            cache_ = std::make_unique<CacheT>(cacheConfig);

            for (uint64_t index=0; index<config_.blockReplayConfig.maxPendingBlockRequestCount; index++)
                pendingBlockReqVec_.push_back(BlockRequest(lbaSizeByte_, blockSizeByte_));
        }


        ~BatchBlockStressor() override { 
            finish(); 
        }


        void start() override {
            // Start the stressor. 
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
                    workers.push_back(std::thread([this, index=i]() {
                        processBackingIoReturn(index);
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


        void finish() override {
            // Cleanup after stressor is terminated. 
            if (threads_.joinable()) 
                threads_.join();
                
            wg_->markShutdown();
            cache_->clearCache(config_.maxInvalidDestructorCount);
        }


        BlockReplayStats getStat(uint64_t threadId) const override {
            // Get statistics of a specific workload. 
            return statsVec_.at(threadId);
        }


        Stats getCacheStats() const override { return cache_->getStats(); }


        util::PercentileStats* getQueueSizePercentile() const override { return queueSizePercentile_; }


        util::PercentileStats* getPendingBlockReqPercentile() const override { return pendingBlockReqCountPStats_; }


        void statSnapshot(uint64_t threadId, 
            std::ostream& ofs, 
            std::string separator, 
            bool renderPercentilesFlag) override {
            /* Write statisitic to a output stream. 

            Args:
                threadId: ID used to identify the statistics to read. 
                ofs: The output stream to write the stats to. 
                separator: The separator used to separate each stat. 
                renderPercentilesFlag: Boolean indicating whether to render percentiles. 
            */
            std::lock_guard<std::mutex> l(statMutex_);

            ofs << folly::sformat("threadId={}{}", threadId, separator);
            ofs << folly::sformat("totalBlockReqReplayed={}{}", totalBlockReqReplayed_, separator);
            ofs << folly::sformat("pendingBlockReqCount={}{}", pendingBlockReqCount_, separator);
            ofs << folly::sformat("queueSize={}{}", queueSize_, separator);
            ofs << folly::sformat("totalIdleTime={}{}", sumIdleTimeNs_, separator);

            statsVec_.at(threadId).timeElapsedNs = getPhysicalTimeElapsedNs();

            BlockReplayStats blockReplayStats = getStat(threadId);
            blockReplayStats.render(ofs, separator, renderPercentilesFlag);

            auto cacheStats = getCacheStats();
            cacheStats.renderBlockReplay(ofs, separator, renderPercentilesFlag);
            ofs << "\n";
        }


    void updateBackingStoreStats(BackingIo backingIo, uint64_t threadId) {
        /* Update the statistics related to backing store. 

        Args:
            backingIo: Backing IO object of the backing IO request that was completed. 
            threadId: Identify the workload to which the backing IO request belongs. 
        */
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
            statsVec_.at(threadId).backingWriteLatencyNsTotal += (getCurrentTsNs() - physicalTs);
        } else {
            statsVec_.at(threadId).readBackingReqCount++;
            statsVec_.at(threadId).readBackingReqByte += size; 
            statsVec_.at(threadId).backingReadLatencyNsPercentile->trackValue(getCurrentTsNs() - physicalTs);
            statsVec_.at(threadId).backingReadSizeBytePercentile->trackValue(size);
            statsVec_.at(threadId).backingReadLatencyNsTotal += (getCurrentTsNs() - physicalTs);
        }

        statsVec_.at(threadId).backingReqAddAttempt = backingStore_.getBackingReqAddAttempt();
        statsVec_.at(threadId).backingReqAddFailure = backingStore_.getBackingReqAddFailure();
    }


    void updateBlockReqStats(uint64_t blockReqIndex, uint64_t threadId) {
        /* Update the statistics related to a block request. 

        Args:
            blockReqIndex: The index of block request that was completed. 
            threadId: Identify the workload to which the backing IO request belongs. 
        */
        std::lock_guard<std::mutex> l(statMutex_);

        blockReqCompleted_ += 1; 
        if (!pendingBlockReqVec_.at(blockReqIndex).isLoaded())
            throw std::runtime_error(folly::sformat("Updating stats for a block request that is not loaded. \n"));

        statsVec_.at(threadId).blockReqCount++;
        uint64_t physicalIatNs = pendingBlockReqVec_.at(blockReqIndex).getPhysicalIatNs();
        uint64_t traceIatUs = pendingBlockReqVec_.at(blockReqIndex).getTraceIatUs();

        uint64_t percentError = 0;
        if (physicalIatNs > (traceIatUs*1000)) {
            percentError = static_cast<uint64_t>(100*(physicalIatNs - (traceIatUs*1000))/physicalIatNs);
        }
        statsVec_.at(threadId).percentIatErrorPstat->trackValue(percentError);

        uint64_t physicalTs = pendingBlockReqVec_.at(blockReqIndex).getPhysicalTimestampNs();
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
            statsVec_.at(threadId).blockWriteSizeBytePercentile->trackValue(size);
            statsVec_.at(threadId).writeLatencyNsTotal += (getCurrentTsNs() - physicalTs);
        } else {
            statsVec_.at(threadId).readBlockReqCount++;
            statsVec_.at(threadId).readBlockReqByte += size;
            statsVec_.at(threadId).readMisalignByte += (frontMisalignByte + rearMisalignByte);
            statsVec_.at(threadId).readCacheReqCount += blockCount; 
            statsVec_.at(threadId).readLatencyNsPercentile->trackValue(getCurrentTsNs() - physicalTs);
            statsVec_.at(threadId).blockReadSizeBytePercentile->trackValue(size);
            statsVec_.at(threadId).readLatencyNsTotal += (getCurrentTsNs() - physicalTs);
        }

        statsVec_.at(threadId).readHitCount += pendingBlockReqVec_.at(blockReqIndex).getBlockHitCount();
        statsVec_.at(threadId).readHitByte += pendingBlockReqVec_.at(blockReqIndex).getReadHitByte();
        statsVec_.at(threadId).physicalIatNsPercentile->trackValue(pendingBlockReqVec_.at(blockReqIndex).getPhysicalIatNs());
        statsVec_.at(threadId).physicalIatNsTotal += pendingBlockReqVec_.at(blockReqIndex).getPhysicalIatNs();
        statsVec_.at(threadId).blockReqAddAttempt = blockReqAddAttempt_;
        statsVec_.at(threadId).blockReqAddFailure = blockReqAddFailure_;
        statsVec_.at(threadId).blockReqAddNoRetryCount = blockReqAddNoRetryCount_;
    }


    void statTracker(uint64_t threadId) {
        /* Periodically track statistics. 
        */
        std::string statFilePath = folly::sformat("{}/tsstat_{}.out", config_.blockReplayConfig.statOutputDir, threadId);

        std::ofstream ofs; 
        ofs.open(statFilePath, std::ofstream::out);
        while ((!isReplayDone()) || (pendingBlockReqCount_ > 0)) {
            std::this_thread::sleep_for (std::chrono::seconds(config_.blockReplayConfig.statTrackIntervalSec));
            statSnapshot(threadId, ofs, ",", false);
        }
        ofs.close();
        logInfo("Stat thread terminated\n", threadId);
    }


    // Check if all replay threads have terminated
    bool isReplayDone() {
        return std::all_of(replayCompletedFlagVec_.begin(), replayCompletedFlagVec_.end(), [](bool b){ return b; });
    }


    // Get the time difference or latency based on the starting cycle count 
    uint64_t getLatencyNs(uint64_t reqBeginCycleCount) {
        return tscns_.tsc2ns(tscns_.rdtsc()) - tscns_.tsc2ns(reqBeginCycleCount);
    }


    uint64_t getCurrentTsNs() {
        return tscns_.tsc2ns(tscns_.rdtsc());
    }


    void processBlockStorageRequestThread(uint64_t threadId) {
        /* Read block requests from the replay queue and process them. 

        Args:
            threadId: Thread ID identifies the workload. 
        */
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
                uint64_t blockReqId;
                if (req.getWriteFlag()) {
                    blockReqId = processBlockStorageWrite(req);
                } else {
                    blockReqId = processBlockStorageRead(req);
                }

                // if pending block request is higher than the max value, it means 
                // that the block request is already completed (cache hit)
                if (blockReqId < maxPendingBlockRequestCount_)
                    submitToBackingStore(blockReqId);
            }
        }

        // notify backing store that its ok to terminate if no requests are pending 
        backingStore_.setReplayDone();

        // set the time when all processing was completed 
        for (uint64_t index=0; index<config_.numThreads; index++) {
            std::lock_guard<std::mutex> l(statMutex_);
            statsVec_.at(index).replayTimeNs = getPhysicalTimeElapsedNs();
        }

        logInfo("Block request processer thread terminated \n", threadId);
    }


    void processBackingIoReturn(uint64_t threadId) {
        /* Tracks backing store IO requests that return and updates the system stats. 
        */
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
            uint64_t backingIostartBlockId = backingIoOffset/blockSizeByte_;
            uint64_t backingIoendBlockId = (backingIoOffset + backingIoSize - 1)/blockSizeByte_;
            bool backingIoWriteFlag = backingIo.getWriteFlag();

            std::lock_guard<std::mutex> l(pendingBlockReqMutex_);

            // make sure that the block request for which backing IO returned is not completed 
            if (pendingBlockReqVec_.at(blockReqIndex).isComplete()) 
                throw std::runtime_error("Block request already completed when backing IO returned\n");
            
            // get the start and end page of the block request 
            uint64_t blockReqStartBlockId = pendingBlockReqVec_.at(blockReqIndex).getStartBlockId();
            uint64_t blockReqEndBlockId = pendingBlockReqVec_.at(blockReqIndex).getEndBlockId();
            uint64_t reqThreadId = pendingBlockReqVec_.at(blockReqIndex).getThreadId();
            bool writeFlag = pendingBlockReqVec_.at(blockReqIndex).getWriteFlag();
            uint64_t blockReqOffset = pendingBlockReqVec_.at(blockReqIndex).getOffset();
            uint64_t threadId = pendingBlockReqVec_.at(blockReqIndex).getThreadId();
            
            if (!writeFlag) {
                // mark the relevant page as completed without updating hit statistics 
                for (uint64_t curBlock=backingIostartBlockId; curBlock<=backingIoendBlockId; curBlock++) 
                    pendingBlockReqVec_.at(blockReqIndex).setCacheHit(curBlock - blockReqStartBlockId, false);

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
                setKeys(blockReqStartBlockId, blockReqEndBlockId, reqThreadId);

                updateBlockReqStats(blockReqIndex, reqThreadId);
                pendingBlockReqVec_.at(blockReqIndex).reset();
                pendingBlockReqCount_--;
                if (pendingBlockReqCount_ == 0)
                    idleStartTimestampNs_ = getPhysicalTimeElapsedNs();
            }
            
            updateBackingStoreStats(backingIo, reqThreadId);
            backingStore_.markCompleted(index);
        }
        logInfo("IO tracker thread terminated \n", threadId);
    }


    void submitToBackingStore(uint64_t blockReqId) {
        /* Submit backing store request required to process a block request. 

        Args:
            blockReqId: The ID of the block request for which we submit backing store request. 
        */
        BlockRequest req(lbaSizeByte_, blockSizeByte_);
        std::vector<bool> blockCompletionVec;
        {
            req = pendingBlockReqVec_.at(blockReqId);
            blockCompletionVec = req.getBlockIoCompletionVec();
        }
        uint64_t threadId = req.getThreadId();
        uint64_t offset = req.getOffset();
        uint64_t size = req.getSize();
        bool writeFlag = req.getWriteFlag(); 

        uint64_t missstartBlockId;
        uint64_t numMissBlock = 0;

        uint64_t startBlockId = offset/blockSizeByte_;
        uint64_t endBlockId = (offset + size - 1)/blockSizeByte_;

        if (writeFlag) {
            uint64_t frontAlignmentByte = offset - (startBlockId * blockSizeByte_);
            uint64_t rearAlignmentByte = ((endBlockId+1) * blockSizeByte_) - (offset + size);

            // there is misalignment in the front and a miss, so submit a backing read 
            if ((frontAlignmentByte > 0) & (!blockCompletionVec.at(0))) 
                backingStore_.submitBackingStoreRequest(getCurrentTsNs(),
                                                            startBlockId*blockSizeByte_, 
                                                            frontAlignmentByte, 
                                                            false, 
                                                            blockReqId, 
                                                            threadId);

            // submit write request 
            backingStore_.submitBackingStoreRequest(getCurrentTsNs(),
                                                        offset, 
                                                        size, 
                                                        true, 
                                                        blockReqId, 
                                                        threadId);

            // there is misalignment in the rear and a miss, so submit a backing read 
            if ((rearAlignmentByte > 0) & (!blockCompletionVec.at(2))) 
                backingStore_.submitBackingStoreRequest(getCurrentTsNs(),
                                                            offset + size, 
                                                            rearAlignmentByte, 
                                                            false, 
                                                            blockReqId, 
                                                            threadId);
        } else {
            // go through each block touched by the block request 
            for (uint64_t curBlock = startBlockId; curBlock <= endBlockId; curBlock++) {
                if (blockCompletionVec.at(curBlock-startBlockId)) {
                    // hit 
                    if (numMissBlock > 0) {
                        backingStore_.submitBackingStoreRequest(getCurrentTsNs(),
                                                                    missstartBlockId*blockSizeByte_, 
                                                                    numMissBlock*blockSizeByte_, 
                                                                    false, 
                                                                    blockReqId, 
                                                                    threadId);
                        numMissBlock = 0;
                    }
                } else {
                    // miss 
                    if (numMissBlock == 0) 
                        missstartBlockId = curBlock;
                    numMissBlock++;
                }
            }
            if (numMissBlock > 0) {
                backingStore_.submitBackingStoreRequest(getCurrentTsNs(),
                                                            missstartBlockId*blockSizeByte_, 
                                                            numMissBlock*blockSizeByte_, 
                                                            false, 
                                                            blockReqId, 
                                                            threadId);
            }

        }
    }


    uint64_t loadToPendingBlockReqVec(BlockRequest& req) {
        /* Load the block request to the list of pending block requests. 

        Args:
            req: The block request that needs to be updated in the list of pending block request. 
        */
        std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
        uint64_t freeBlockReqIndex = maxPendingBlockRequestCount_;
        for (int index=0; index < maxPendingBlockRequestCount_; index++) {
            if (!pendingBlockReqVec_.at(index).isLoaded()) {
                pendingBlockReqVec_.at(index).load(req.getThreadId(),
                    index,
                    req.getPhysicalTimestampNs(), 
                    req.getPhysicalIatNs(),
                    req.getTraceTimestampUs(),
                    req.getTraceIatUs(),
                    req.getLba(),
                    req.getSize(),
                    req.getWriteFlag());
                freeBlockReqIndex = index;
                break;
            }
        }

        blockReqAddAttempt_++;
        if (freeBlockReqIndex == maxPendingBlockRequestCount_)
            blockReqAddFailure_++;

        return freeBlockReqIndex; 
    }


    void setKeys(uint64_t startBlockId, uint64_t endBlockId, uint64_t threadId) {
        /* Set the keys in the cache pertaining to a range of block IDs. 

        Args:
            startBlockId: We start setting blocks starting from this ID. 
            endBlockId: The last block ID to be set before we stop. 
        */
        for (uint64_t blockReqId=startBlockId; blockReqId<=endBlockId; blockReqId++) {
            const std::string key = folly::sformat("{}{}", blockReqId, threadId);
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


    bool blockInCache(uint64_t blockReqId, uint64_t threadId) {
        /* Check if a block is in the cache. 

        Args:
            blockReqId: Block Id of the workload. 
            threadId: ThreadId is used to identify the workload the block belongs to. 
        
        Return:
            blockInCache: Boolean indicating if block is in the cache. 
        */
        bool blockInCache = false;
        // the key = blockReqId + threadId (e.g. key = '14' + '1' = '141') to differentiate the 
        // same block being accessed by different replay threads which are two different blocks of data
        const std::string key = folly::sformat("{}{}", blockReqId, threadId);
        auto it = cache_->find(key);
        if (it != nullptr)     
            blockInCache = true; 
        return blockInCache;
    }


    // Process a write block storage request 
    uint64_t processBlockStorageWrite(BlockRequest req) {
        uint64_t blockRequestIndex = loadToPendingBlockReqVec(req);
        if (blockRequestIndex < maxPendingBlockRequestCount_)
            blockReqAddNoRetryCount_++;
        
        while (blockRequestIndex == maxPendingBlockRequestCount_)
            blockRequestIndex = loadToPendingBlockReqVec(req);
        
        uint64_t lba = req.getLba();
        uint64_t offset = req.getOffset();
        uint64_t size = req.getSize();
        uint64_t threadId = req.getThreadId();
        uint64_t startBlockId = req.getStartBlockId();
        uint64_t endBlockId = req.getEndBlockId();
        bool writeFlag = req.getWriteFlag();

        uint64_t frontMisalignByte = req.getFrontMisAlignByte();
        uint64_t rearMisalignByte = req.getRearMisAlignByte();

        if (startBlockId == endBlockId) {
            if ((frontMisalignByte > 0) || (rearMisalignByte > 0)) {
                if (blockInCache(startBlockId, threadId)) {
                    {
                        std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
                        pendingBlockReqVec_.at(blockRequestIndex).setCacheHit(0, true);
                        pendingBlockReqVec_.at(blockRequestIndex).setCacheHit(2, true);
                    }
                }
            }
        } else {
            if (frontMisalignByte > 0) {
                if (blockInCache(startBlockId, threadId)) {
                    {
                        std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
                        pendingBlockReqVec_.at(blockRequestIndex).setCacheHit(0, true);
                    }
                }
            }

            if (rearMisalignByte > 0) {
                if (blockInCache(endBlockId, threadId)) {
                    {
                        std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
                        pendingBlockReqVec_.at(blockRequestIndex).setCacheHit(2, true);
                    }
                }
            }
        }

        // remove all the stale keys from the cache if they exist 
        for (int blockReqId = startBlockId; blockReqId <= endBlockId; blockReqId++) {
            const std::string key = folly::sformat("{}{}", blockReqId, threadId);
            cache_->remove(key);
        }

        return blockRequestIndex;
    }


    uint64_t processBlockStorageRead(BlockRequest req) {
        // Process the given read block storage request. 
        uint64_t blockReqId = loadToPendingBlockReqVec(req);
        if (blockReqId < maxPendingBlockRequestCount_)
            blockReqAddNoRetryCount_++;

        while (blockReqId == maxPendingBlockRequestCount_) 
            blockReqId = loadToPendingBlockReqVec(req);
        
        bool writeFlag = req.getWriteFlag();
        uint64_t size = req.getSize();
        uint64_t threadId = req.getThreadId();
        uint64_t startBlockId = req.getStartBlockId();
        uint64_t endBlockId = req.getEndBlockId();
        uint64_t missstartBlockId;
        uint64_t missPageCount = 0;
        bool blockReqCompletionFlag = false; 

        std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
        for (int curBlock = startBlockId; curBlock<=endBlockId; curBlock++) {
            if (blockInCache(curBlock, threadId)) 
                pendingBlockReqVec_.at(blockReqId).setCacheHit(curBlock - startBlockId, true);
        }

        // if all the blocks are in cache, the block request is complete 
        if (pendingBlockReqVec_.at(blockReqId).isComplete()) {
            updateBlockReqStats(blockReqId, threadId);
            pendingBlockReqVec_.at(blockReqId).reset();
            pendingBlockReqCount_--;
            blockReqId = maxPendingBlockRequestCount_;
        }
        return blockReqId;
    }


    BlockRequest loadBlockReq(uint64_t threadId, uint64_t prevTraceTsUs) {
        /* Read the next block request from the block trace.
        
        Args:
            threadId: It is used to determine the block trace to read. 
            prevTraceTsUs: The trace timestamp of the previous request used to compute IAT. 
        
        Return:
            blockReq: The next block request in the block trace. 
        
        Raises:
            cachebench::EndOfTrace: If the end of trace file is reached when trying to read the next request. 
        */
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
        
        /* Create a block request object to add to replay queue. We set the block request ID of 
        the block request to a value higher than the maximum possible value of ID because a block 
        request is only assigned an ID once it is picked up for processing. The physical IAT of 
        the block request is computed on submission time so setting it to 0 for now.*/
        BlockRequest blockReq = BlockRequest(threadId,
            maxPendingBlockRequestCount_, 
            lbaSizeByte_, 
            blockSizeByte_,
            getCurrentTimestampNs(),
            0,
            ts,
            iatUs,
            lba,
            size,
            writeFlag);
        return blockReq;
    }


    uint64_t getPhysicalIatNs() {
        /* Get the physical Iat if you were to submit a block request to the system right now.
        
        Return:
            physicalIatNs: The IAT in nanoseconds if you were to submit a block request now. 
        */
        std::lock_guard<std::mutex> l(timeMutex_);
        uint64_t currentTimestampNs = tscns_.tsc2ns(tscns_.rdtsc());
        if (currentTimestampNs <= prevSubmitTimestampNs_) 
            return 0;
        else 
            return currentTimestampNs - prevSubmitTimestampNs_;
    }

    
    uint64_t getPhysicalTimeElapsedNs() {
        /* Get the physical time elapsed in nanoseconds. 

        Return:
            physicalTimeElapsedNs: The physical time elapsed in nanoseconds. 
        */
        uint64_t currentTimeElapsedNs = tscns_.tsc2ns(tscns_.rdtsc());
        uint64_t physicalStartTimestampNs = getPhysicalStartTimestampNs();
        if (currentTimeElapsedNs <= physicalStartTimestampNs) {
            return 0;
        } else 
            return currentTimeElapsedNs - physicalStartTimestampNs;
    }


    uint64_t getPhysicalStartTimestampNs() {
        std::lock_guard<std::mutex> l(timeMutex_);
        return physicalStartTimestampNs_;
    }


    void updatePrevSubmitTimepoint() {
        /* Update the varibale that tracks the physical timestamp of the most latest block 
        request submitted to the system.*/
        std::lock_guard<std::mutex> l(timeMutex_);
        prevSubmitTimestampNs_ = getCurrentTimestampNs();
    }


    uint64_t getPrevSubmitTimestamp() {
        std::lock_guard<std::mutex> l(timeMutex_);
        return prevSubmitTimestampNs_;
    }


    uint64_t getCurrentTimestampNs() {
        /* Get current timestamp in nanoseconds. 

        Return:
            currentTimestampNs: Current timestamp in nanoseconds. 
        */
        return tscns_.tsc2ns(tscns_.rdtsc());
    }


    void setPhysicalStartTimestampNs() {
        // Set the physical timestamp of starting replay.
        std::lock_guard<std::mutex> l(timeMutex_);
        physicalStartTimestampNs_ = tscns_.tsc2ns(tscns_.rdtsc());
    }


    void updateSystemStats(uint64_t threadId) {
        // Update system stats after submitting the most recent batch of block requests. 
        std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
        if (pendingBlockReqCount_ == 0) {
            uint64_t currentPhysicalTs = getPhysicalTimeElapsedNs();
            if (idleStartTimestampNs_ > 0) 
                sumIdleTimeNs_ += (currentPhysicalTs - idleStartTimestampNs_);
            idleStartTimestampNs_ = currentPhysicalTs;
        }
        pendingBlockReqCount_ += curBlockReqBatchVec_.at(threadId).size(); 
        pendingBlockReqCountPStats_->trackValue(pendingBlockReqCount_);
    }


    void submitBlockReqBatch(uint64_t threadId) {
        // Submit the latest batch of block requests read from trace to the system.
        std::lock_guard<std::mutex> l(blockRequestQueueMutex_);
        if (totalBlockReqReplayed_ == 0)
            updatePrevSubmitTimepoint();
            
        for (BlockRequest req : curBlockReqBatchVec_.at(threadId)) {
            /* We create a new block request when adding it to the queue because once the block requests 
            in the vector 'curBlockReqBatchVec_.at(threadId)' is submitted, the vector will be cleared removing all objects. 
            */
            blockRequestQueue_.push(BlockRequest(req.getThreadId(),
                maxPendingBlockRequestCount_,
                lbaSizeByte_,
                blockSizeByte_,
                req.getPhysicalTimestampNs(),
                getPhysicalIatNs(),
                req.getTraceTimestampUs(),
                req.getTraceIatUs(),
                req.getLba(),
                req.getSize(),
                req.getWriteFlag()));
            
            totalBlockReqReplayed_++;
            queueSize_++;
            queueSizePercentile_->trackValue(queueSize_);
            updatePrevSubmitTimepoint();
        }
    }


    BlockRequest loadBlockReqBatch(uint64_t threadId) {
        /* Add block requests (BlockRequest) to the member variable (curBlockReqBatchVec_.at(threadId)) that 
        stores the latest batch of block requests to be submitted to the system. 

        Args:
            threadId: The ID used to identify the block trace file from which to read block requests. 
        
        Return:
            nextBlockReq: The starting block request of the next batch. This object is later added to vector
                'curBlockReqBatchVec_.at(threadId)' once the most recent batch is submitted and the vector cleared. 
        */
        BlockRequest nextBlockReq(lbaSizeByte_, blockSizeByte_);
        try {
            if (curBlockReqBatchVec_.at(threadId).size() == 0) {
                if (totalBlockReqReplayed_ > 0)
                    throw std::runtime_error("Next block request is empty but requests have been replayed.");
                curBlockReqBatchVec_.at(threadId).push_back(loadBlockReq(threadId, traceBeginTimestampUs_));
                // The first block request has no IAT (infinite) so we set it to 0 to issue it ASAP.
                curBlockReqBatchVec_.at(threadId).back().setTraceIatUs(0);
                traceBeginTimestampUs_ = curBlockReqBatchVec_.at(threadId).back().getTraceTimestampUs();
                setPhysicalStartTimestampNs();
            }

            uint64_t prevBlockReqTimestamp = curBlockReqBatchVec_.at(threadId).back().getTraceTimestampUs();
            nextBlockReq = loadBlockReq(threadId, curBlockReqBatchVec_.at(threadId).back().getTraceTimestampUs());
            uint64_t nextBlockReqIatUs = nextBlockReq.getTraceTimestampUs() - prevBlockReqTimestamp;

            if (nextBlockReq.getTraceTimestampUs() < prevBlockReqTimestamp)
                throw std::runtime_error("The timestamp of the next block request is lower than the current request.");
            
            if (nextBlockReqIatUs < config_.blockReplayConfig.minIatUs) {
                curBlockReqBatchVec_.at(threadId).push_back(nextBlockReq);
                prevBlockReqTimestamp = curBlockReqBatchVec_.at(threadId).back().getTraceTimestampUs();
                nextBlockReq = loadBlockReq(threadId, curBlockReqBatchVec_.at(threadId).back().getTraceTimestampUs());
                nextBlockReqIatUs = nextBlockReq.getTraceTimestampUs() - prevBlockReqTimestamp;
            }
        } catch (const cachebench::EndOfTrace& ex) {
        }

        return nextBlockReq;
    }


    void logInfo(std::string infoString, uint64_t threadId) {
        // Log some information to stdout. 
        std::cout << folly::sformat("INFO(threadId={}): {}", threadId, infoString);
    }


    void replay(BlockReplayStats& stats, uint64_t threadId) {
        /* Replay a block storage trace. 

        Args:
            stats: BlockReplayStats object to track statistics for this thread. 
            threadId: The threadID is used to separate statistics of each thread. 
        */
        logInfo(folly::sformat("Init block trace replay.\n"), threadId);

        BlockRequest nextBlockReq = loadBlockReqBatch(threadId);
        while(nextBlockReq.isLoaded()) {
            // Sync the time in the trace and the physical time. 
            if (config_.blockReplayConfig.globalClock) {
                uint64_t traceTimeElapsedUs = curBlockReqBatchVec_.at(threadId).front().getTraceTimestampUs() - traceBeginTimestampUs_;
                uint64_t physicalStartTimestampNs = getPhysicalStartTimestampNs();
                while (traceTimeElapsedUs*1000 > (getCurrentTimestampNs() - physicalStartTimestampNs));
            } else {
                uint64_t prevSubmitTimestampNs = getPrevSubmitTimestamp();
                while (curBlockReqBatchVec_.at(threadId).front().getTraceIatUs()*1000 > (getCurrentTimestampNs()-prevSubmitTimestampNs));
            }
            submitBlockReqBatch(threadId);
            updateSystemStats(threadId);
            curBlockReqBatchVec_.at(threadId).clear();
            curBlockReqBatchVec_.at(threadId).push_back(nextBlockReq);
            nextBlockReq = loadBlockReqBatch(threadId);
        }

        wg_->markFinish();
        replayCompletedFlagVec_.at(threadId) = true; 
        stats.replayTimeNs = getPhysicalTimeElapsedNs();
        
        logInfo("Replay thread terminated\n", threadId);
    }


    static std::string genHardcodedString() {
        const std::string s = "The quick brown fox jumps over the lazy dog. ";
        std::string val;
        for (int i = 0; i < 4 * 1024 * 1024; i += s.size()) {
            val += s;
        }
        return val;
    }


    // parameters related to the workload generator 
    // Note: BlockReplayGenerator is the only workload generator supported. 
    std::unique_ptr<GeneratorBase> wg_; 
    std::vector<std::vector<BlockRequest>> curBlockReqBatchVec_;

    const StressorConfig config_; 
    std::unique_ptr<CacheT> cache_;
    BackingStore backingStore_;
    std::thread threads_;
    std::vector<bool>replayCompletedFlagVec_;

    // system configs that are fixed 
    uint64_t lbaSizeByte_;
    uint64_t blockSizeByte_;
    uint64_t maxPendingBlockRequestCount_;
    const std::string hardcodedString_;

    mutable std::mutex pendingBlockReqMutex_;
    uint64_t pendingBlockReqCount_ = 0;
    std::vector<BlockRequest> pendingBlockReqVec_;
    util::PercentileStats *pendingBlockReqCountPStats_ = new util::PercentileStats();
    
    // Statistics of attempt to submit block request to the system 
    uint64_t blockReqAddAttempt_ = 0;
    uint64_t blockReqAddFailure_ = 0;
    uint64_t blockReqAddNoRetryCount_ = 0; 
    
    // vector of stats corresponding to each replay thread 
    mutable std::mutex statMutex_;
    std::vector<BlockReplayStats> statsVec_;
    std::vector<std::ofstream> statsFileStreamVec_;
    uint64_t blockReqCompleted_ = 0;

    // FIFO queue of block requests in the system waiting to be processed 
    // processing threads pop this queue to get the request to process once free 
    mutable std::mutex blockRequestQueueMutex_;
    uint64_t queueSize_ = 0;
    uint64_t totalBlockReqReplayed_ = 0;
    std::queue<BlockRequest> blockRequestQueue_;
    util::PercentileStats *queueSizePercentile_ = new util::PercentileStats();

    // Variables related to time.  
    mutable std::mutex timeMutex_;
    TSCNS tscns_; 
    uint64_t traceBeginTimestampUs_;
    uint64_t prevSubmitTimestampNs_;
    uint64_t physicalStartTimestampNs_;
    uint64_t sumIdleTimeNs_ = 0;
    uint64_t idleStartTimestampNs_ = 0;

};


}
}
}