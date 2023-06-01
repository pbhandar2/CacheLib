#pragma once

#include <chrono>
#include <thread>

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

class BlockRequest {
    public:

        BlockRequest(uint64_t lbaSizeByte, uint64_t blockSizeByte){
            lbaSizeByte_ = lbaSizeByte;
            blockSizeByte_ = blockSizeByte;
        }

        void load(uint64_t ts, uint64_t lba, uint64_t size, bool writeFlag, uint64_t blockRequestIndex, uint64_t threadId) {
            ts_ = ts; 
            lba_ = lba;
            size_ = size;
            writeFlag_ = writeFlag;
            blockRequestIndex_ = blockRequestIndex;
            threadId_ = threadId;

            offsetByte_ = lba_ * lbaSizeByte_;
            startBlock_ = floor(offsetByte_/blockSizeByte_);
            endBlock_ = floor((offsetByte_ + size_ - 1)/blockSizeByte_);
            frontMisalignByte_ = offsetByte_ - (startBlock_ * blockSizeByte_);
            rearMisalignByte_ = ((endBlock_ * blockSizeByte_) + size_) - (offsetByte_ + size_);

            // A write block request can only have a maximum of 3 backing IO requests:
            // a read due to front misalignment, a write request and a read due to rear 
            // misalignment. So the vector tracking IO completion only needs 3 slots.  
            //
            // A read block request can generate variable number of backing IO requests 
            // based on the byte ranges that are hits and misses. So the vector tracking 
            // the IO completion has a slot for every block touched. 
            if (writeFlag_) 
                blockIoCompletionVec_ = std::vector<bool>(3, false);
            else 
                blockIoCompletionVec_ = std::vector<bool>(endBlock_ - startBlock_ + 1, false);
            
            // if there is no misalignment, then mark the misalignment as done 
            // sometimes the front and rear misalignment are on the same page 
            // for now submitting separate IO for each 
            if ((frontMisalignByte_ == 0) & (writeFlag_))
                blockIoCompletionVec_.at(0) = true; 

            if ((rearMisalignByte_ == 0) & (writeFlag_))
                blockIoCompletionVec_.at(2) = true; 
        }

        bool isLoaded() {
            return size_ > 0;
        }

        void reset() {
            size_ = 0;
            blockHitCount_ = 0;
            readHitByte_ = 0; 
            blockIoCompletionVec_.clear();
        }

        uint64_t getLba() {
            return lba_;
        }

        uint64_t getSize() {
            return size_;
        }

        bool getWriteFlag() {
            return writeFlag_;
        }

        uint64_t getBlockRequestIndex() {
            return blockRequestIndex_;
        }

        uint64_t getThreadId() {
            return threadId_;
        }

        uint64_t getStartBlock() {
            return startBlock_;
        }

        uint64_t getFrontMisAlignByte() {
            return frontMisalignByte_;
        }

        uint64_t getRearMisAlignByte() {
            return rearMisalignByte_;
        }

        uint64_t getEndBlock() {
            return endBlock_;
        }

        uint64_t getOffset() {
            return offsetByte_;
        }

        uint64_t getReadHitByte() {
            return readHitByte_;
        }

        uint64_t getBlockHitCount() {
            return blockHitCount_;
        }

        std::vector<bool> getBlockIoCompletionVec() {
            return blockIoCompletionVec_;
        }

        void setCacheHit(uint64_t blockIndex) {
            if (blockIndex >= blockIoCompletionVec_.size())
                throw std::runtime_error(folly::sformat("IO index {} too high max {} writeFlag: {} \n", blockIndex, 
                                                                                                            blockIoCompletionVec_.size(), 
                                                                                                            writeFlag_));
            blockIoCompletionVec_.at(blockIndex) = true; 
            blockHitCount_++;
            if ((blockIndex == 0) & (frontMisalignByte_ > 0)) {
                readHitByte_ += (blockSizeByte_ - frontMisalignByte_);
            } else if ((blockIndex == (blockIoCompletionVec_.size()-1)) & (rearMisalignByte_ > 0)) {
                readHitByte_ += (blockSizeByte_ - rearMisalignByte_);
            } else {
                readHitByte_ += blockSizeByte_;
            }
        }

        void backingIoReturn(uint64_t backingOffset, uint64_t backingSize, bool backingWriteFlag) {
            uint64_t backingStartBlock = backingOffset/blockSizeByte_;
            uint64_t backingEndBlock = (backingOffset + backingSize - 1)/blockSizeByte_;

            if (backingWriteFlag) {
                // the block IO completion vector has 3 slots for a write block request 
                // 0 -> front misalign read, 1 -> write, 2 -> rear misalign read 
                // so here we just set index 1 to true 
                blockIoCompletionVec_.at(1) = true; 
            } else {
                if (writeFlag_) {
                    // if it is a write block request, the consequent backing reads due to 
                    // misalignment cannot touch more than a single page 
                    if (backingStartBlock == backingEndBlock)
                        throw std::runtime_error("Misalignment read extends to multiple pages!\n");

                    if (backingOffset < offsetByte_) {
                        blockIoCompletionVec_.at(0) = true; 
                    } else {
                        blockIoCompletionVec_.at(2) = true; 
                    }
                } else {
                    for (uint64_t curBlock=backingStartBlock; curBlock<=backingEndBlock; curBlock++) {
                        uint64_t blockIndex = curBlock - startBlock_;
                        blockIoCompletionVec_.at(blockIndex) = true; 
                    }
                }
            }
        }

        bool isComplete() {
            return std::all_of(blockIoCompletionVec_.begin(), blockIoCompletionVec_.end(), [](bool b) { return b; });
        }

    private:
        bool writeFlag_;
        uint64_t size_ = 0;
        uint64_t blockHitCount_ = 0;
        uint64_t readHitByte_ = 0;
        uint64_t lba_;
        uint64_t offsetByte_;
        uint64_t ts_;
        uint64_t lbaSizeByte_;
        uint64_t blockSizeByte_;
        uint64_t blockRequestIndex_;
        uint64_t threadId_;
        uint64_t startBlock_;
        uint64_t endBlock_;
        uint64_t frontMisalignByte_;
        uint64_t rearMisalignByte_;
        std::vector<bool> blockIoCompletionVec_;
};


// A block storage system stressor that uses an instance of CacheLib 
// as cache and libaio to perform async backing store IO 
// when needed. 
template <typename Allocator>
class BlockStorageStressor : public BlockSystemStressor {
	public:
		using CacheT = Cache<Allocator>;
        using SystemTimepoint = std::chrono::time_point<std::chrono::system_clock>;

        // @param cacheConfig   the config to instantiate the cache instance
        // @param config        stress test config
        // @param generator     workload  generator
        BlockStorageStressor(CacheConfig cacheConfig,
                                StressorConfig config,
                                std::unique_ptr<GeneratorBase>&& generator)
                                :   config_(config), 
                                    backingStore_(config),
                                    lbaSizeByte_(config.blockReplayConfig.lbaSizeByte),
                                    blockSizeByte_(config.blockReplayConfig.blockSizeByte),
                                    hardcodedString_(genHardcodedString()),
                                    stressorTerminateFlag_(config.numThreads, false),
                                    statsVec_(config.numThreads),
                                    wg_(std::move(generator)),
                                    statUpdateLockVec_(config.numThreads),
                                    endTime_{std::chrono::system_clock::time_point::max()} {
            // setup the cache 
            cache_ = std::make_unique<CacheT>(cacheConfig);
            for (uint64_t index=0; index<config_.blockReplayConfig.maxPendingBlockRequestCount; index++)
                pendingBlockReqVec_.push_back(BlockRequest(lbaSizeByte_, blockSizeByte_));
        }

        ~BlockStorageStressor() override { 
            finish(); 
        }

        // start stressing the block storage system 
        void start() override {
            {
                std::lock_guard<std::mutex> l(timeMutex_);
                startTime_ = std::chrono::system_clock::now();
            }
            threads_ = std::thread([this] {
                
                std::vector<std::thread> workers;
                for (uint64_t i = 0; i < config_.numThreads; ++i) {
                    workers.push_back(std::thread([this, blockReplayStats = &statsVec_.at(i), index=i]() {
                        stressByBlockReplay(*blockReplayStats, index);
                    }));
                }

                for (uint64_t i = 0; i < config_.blockReplayConfig.blockRequestProcesserThreads; ++i) {
                    workers.push_back(std::thread([this]() {
                        processBlockStorageRequestThread();
                    }));
                }

                for (uint64_t i = 0; i < config_.blockReplayConfig.asyncIOReturnTrackerThreads; ++i) {
                    workers.push_back(std::thread([this]() {
                        processBackingIoReturn();
                    }));
                }

                for (auto& worker : workers) {
                    worker.join();
                }
                {
                    std::lock_guard<std::mutex> l(timeMutex_);
                    endTime_ = std::chrono::system_clock::now();
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

        uint64_t getTestDurationNs() const override {
            std::lock_guard<std::mutex> l(timeMutex_);
            return std::chrono::nanoseconds{std::min(std::chrono::system_clock::now(), endTime_) - startTime_}
                    .count();
        }

    void statUpdate(uint64_t blockReqIndex, uint64_t threadId) {
        uint64_t offset;
        uint64_t size;
        bool writeFlag; 

        offset = pendingBlockReqVec_.at(blockReqIndex).getOffset();
        size = pendingBlockReqVec_.at(blockReqIndex).getSize();
        writeFlag = pendingBlockReqVec_.at(blockReqIndex).getWriteFlag();
        
        std::lock_guard<std::mutex> l(statUpdateLockVec_.at(threadId));
        statsVec_.at(threadId).blockReqCount++;

        if (writeFlag) {
            statsVec_.at(threadId).writeBlockReqCount++;
            statsVec_.at(threadId).writeBlockReqByte += size;
        } else {
            statsVec_.at(threadId).readBlockReqCount++;
            statsVec_.at(threadId).readBlockReqByte += size;
        }
    }

    bool isReplayDone() {
        return std::all_of(stressorTerminateFlag_.begin(), stressorTerminateFlag_.end(), [](bool b){ return b; });
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

    void addToQueue(uint64_t index) {
        std::lock_guard<std::mutex> l(blockRequestQueueMutex_);
        blockRequestQueue_.push(index);
    }

    void setKeys(uint64_t startBlock, uint64_t endBlock) {
        for (uint64_t curBlock=startBlock; curBlock<=endBlock; curBlock++) {
            const std::string key = std::to_string(curBlock);
            auto it = cache_->allocate(0, 
                                key, 
                                blockSizeByte_, 
                                0);
            if (it == nullptr) {
                XDCHECK(it);
                XDCHECK_LE(cache_->getSize(it), 4ULL * 1024 * 1024);
                cache_->setStringItem(it, hardcodedString_);
                cache_->insertOrReplace(it);
            }
        }
    }

    uint64_t popFromQueue() {
        std::lock_guard<std::mutex> l(blockRequestQueueMutex_);
        uint64_t index = config_.blockReplayConfig.maxPendingBlockRequestCount;
        if (blockRequestQueue_.size() > 0) {
            index = blockRequestQueue_.front();
            blockRequestQueue_.pop();
        }
        return index;
    }

    void processBackingIoReturn() {
        while ((!isReplayDone()) || pendingBlockReqCount_ > 0) {
            // pop the index of the backing store IO that has returned 
            uint64_t index = backingStore_.popFromBackingIoReturnQueue();
            if (index == config_.blockReplayConfig.maxPendingBackingStoreIoCount)
                continue; 
            
            BackingIo backingIo = backingStore_.getBackingIo(index);
            int64_t blockRequestIndex = backingIo.getBlockRequestIndex();
            updateIoCompletion(blockRequestIndex, backingIo);
            backingStore_.markCompleted(index);
        }
    }

        void updateIoCompletion(uint64_t blockRequestIndex, BackingIo backingIo) {
            std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
            uint64_t blockRequestStartBlock = pendingBlockReqVec_.at(blockRequestIndex).getStartBlock();
            uint64_t threadId = pendingBlockReqVec_.at(blockRequestIndex).getThreadId();
            uint64_t backingIoOffset = backingIo.getOffset();

            uint64_t backingStoreRequestStartBlock = backingIo.getOffset()/blockSizeByte_;
            uint64_t backingStoreRequestEndBlock = (backingIo.getOffset() + backingIo.getSize() - 1)/blockSizeByte_;
            if (pendingBlockReqVec_.at(blockRequestIndex).getWriteFlag()) {
                if (backingIo.getWriteFlag()) {
                    pendingBlockReqVec_.at(blockRequestIndex).setCacheHit(1);
                } else {
                    if (backingIo.getOffset() < pendingBlockReqVec_.at(blockRequestIndex).getOffset()) {
                        pendingBlockReqVec_.at(blockRequestIndex).setCacheHit(0);
                    } else {
                        pendingBlockReqVec_.at(blockRequestIndex).setCacheHit(2);
                    }
                }
            } else {
                for (uint64_t curBlock = backingStoreRequestStartBlock; curBlock<=backingStoreRequestEndBlock; curBlock++) {
                    pendingBlockReqVec_.at(blockRequestIndex).setCacheHit(curBlock - blockRequestStartBlock);
                }
            }
            if (pendingBlockReqVec_.at(blockRequestIndex).isComplete()) {
                statUpdate(blockRequestIndex, threadId);
                pendingBlockReqVec_.at(blockRequestIndex).reset();
                pendingBlockReqCount_--;
            }
        }

    void processBlockStorageRequestThread() {
        while ((!isReplayDone()) || pendingBlockReqCount_ > 0) {

            uint64_t pendingBlockRequestIndex = popFromQueue();
            if (pendingBlockRequestIndex == config_.blockReplayConfig.maxPendingBlockRequestCount)
                continue;

            std::vector<bool> blockCompletionVec = processBlockRequest(pendingBlockRequestIndex);
            uint64_t threadId;
            uint64_t offset;
            uint64_t size;
            bool writeFlag; 
            {
                std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
                threadId = pendingBlockReqVec_.at(pendingBlockRequestIndex).getThreadId();
                offset = pendingBlockReqVec_.at(pendingBlockRequestIndex).getOffset();
                size = pendingBlockReqVec_.at(pendingBlockRequestIndex).getSize();
                writeFlag = pendingBlockReqVec_.at(pendingBlockRequestIndex).getWriteFlag();
            }
            submitToBackingStore(pendingBlockRequestIndex, threadId, offset, size, writeFlag, blockCompletionVec);
        }
    }

    void submitToBackingStore(uint64_t pendingBlockRequestIndex, 
                                uint64_t threadId, 
                                uint64_t offset, 
                                uint64_t size, 
                                uint64_t writeFlag,
                                std::vector<bool> blockCompletionVec) {
        
        uint64_t missStartBlock;
        uint64_t numMissBlock = 0;

        uint64_t startBlock = offset/blockSizeByte_;
        uint64_t endBlock = (offset + size - 1)/blockSizeByte_;

        if (writeFlag) {
            uint64_t frontAlignmentByte = offset - (startBlock * blockSizeByte_);
            uint64_t rearAlignmentByte = ((endBlock+1) * blockSizeByte_) - (offset + size);

            // there is misalignment in the front and a miss, so submit a backing read 
            if ((frontAlignmentByte > 0) & (!blockCompletionVec.at(0))) 
                backingStore_.submitBackingStoreRequest(startBlock*blockSizeByte_, 
                                                            frontAlignmentByte, 
                                                            false, 
                                                            pendingBlockRequestIndex, 
                                                            threadId);

            // submit write request 
            backingStore_.submitBackingStoreRequest(offset, 
                                                        size, 
                                                        true, 
                                                        pendingBlockRequestIndex, 
                                                        threadId);

            // there is misalignment in the rear and a miss, so submit a backing read 
            if ((rearAlignmentByte > 0) & (!blockCompletionVec.at(2))) 
                backingStore_.submitBackingStoreRequest(offset + size, 
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
                        backingStore_.submitBackingStoreRequest(missStartBlock*blockSizeByte_, 
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
            if (numMissBlock > 0) 
                backingStore_.submitBackingStoreRequest(missStartBlock*blockSizeByte_, 
                                                            numMissBlock*blockSizeByte_, 
                                                            false, 
                                                            pendingBlockRequestIndex, 
                                                            threadId);
        }
    }

    std::vector<bool> processBlockRequest(int64_t index) {
        std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
        if (pendingBlockReqVec_.at(index).getWriteFlag()){
            return processBlockRequestSystemWrite(index);
        } else {
            return processBlockRequestSystemRead(index);
        }
    }

    std::vector<bool> processBlockRequestSystemRead(uint64_t pendingBlockRequestIndex) {
        BlockRequest pendingBlockRequest = pendingBlockReqVec_.at(pendingBlockRequestIndex);

        uint64_t size = pendingBlockRequest.getSize();
        uint64_t threadId = pendingBlockRequest.getThreadId();
        uint64_t startBlock = pendingBlockRequest.getStartBlock();
        uint64_t endBlock = pendingBlockRequest.getEndBlock();
        uint64_t blockRequestIndex = pendingBlockRequest.getBlockRequestIndex();
        bool writeFlag = pendingBlockRequest.getWriteFlag();

        uint64_t missStartBlock;
        uint64_t missPageCount = 0;
        for (int curBlock = startBlock; curBlock<=endBlock; curBlock++) {
            if (blockInCache(curBlock, threadId)) {
                pendingBlockRequest.setCacheHit(curBlock - startBlock);
            }
        }

        if (pendingBlockReqVec_.at(blockRequestIndex).isComplete()) {
            statUpdate(blockRequestIndex, threadId);
            pendingBlockReqVec_.at(blockRequestIndex).reset();
            pendingBlockReqCount_--;
        }

        return pendingBlockReqVec_.at(blockRequestIndex).getBlockIoCompletionVec();
    }

    std::vector<bool> processBlockRequestSystemWrite(uint64_t pendingBlockRequestIndex) {
        BlockRequest pendingBlockRequest = pendingBlockReqVec_.at(pendingBlockRequestIndex);

        uint64_t lba = pendingBlockRequest.getLba();
        uint64_t offset = pendingBlockRequest.getOffset();
        uint64_t size = pendingBlockRequest.getSize();
        uint64_t threadId = pendingBlockRequest.getThreadId();
        uint64_t startBlock = pendingBlockRequest.getStartBlock();
        uint64_t endBlock = pendingBlockRequest.getEndBlock();
        uint64_t blockRequestIndex = pendingBlockRequest.getBlockRequestIndex();
        bool writeFlag = pendingBlockRequest.getWriteFlag();

        uint64_t frontMisalignByte = pendingBlockRequest.getFrontMisAlignByte();
        uint64_t rearMisalignByte = pendingBlockRequest.getRearMisAlignByte();

        // remove all the stale keys from the cache if they exist 
        for (int curBlock = startBlock; curBlock<(endBlock + 1); curBlock++) {
            const std::string key = std::to_string(curBlock);
            cache_->remove(key);
        }

        if (frontMisalignByte > 0) {
            if (blockInCache(startBlock, threadId))
                pendingBlockRequest.setCacheHit(0);
        }

        if (rearMisalignByte > 0) {
            if (blockInCache(endBlock, threadId))
                pendingBlockRequest.setCacheHit(2);
        }

        return pendingBlockReqVec_.at(blockRequestIndex).getBlockIoCompletionVec();
    }

    // Load a request from trace with the given attribute to the list of pending block requests 
    uint64_t addToPendingBlockRequestVec(uint64_t ts, uint64_t lba, uint64_t size, bool writeFlag, uint64_t threadId) {
        std::lock_guard<std::mutex> l(pendingBlockReqMutex_);
        uint64_t freeBlockReqIndex = config_.blockReplayConfig.maxPendingBlockRequestCount;
        if (pendingBlockReqCount_ < freeBlockReqIndex) {
            for (int index=0; index < config_.blockReplayConfig.maxPendingBlockRequestCount; index++) {
                if (!pendingBlockReqVec_.at(index).isLoaded()) {
                    pendingBlockReqVec_.at(index).load(ts, lba, size, writeFlag, index, threadId);
                    freeBlockReqIndex = index;
                    pendingBlockReqCount_++;
                    break;
                }
            }
        }
        return freeBlockReqIndex; 
    }

    // Submit a request from the trace to the block storage system. 
    //
    // @param req   Request from block storage trace 
   SystemTimepoint submitToBlockStorageSystem(const Request& req,    
                                                uint64_t threadId,
                                                BlockReplayStats& stats,
                                                uint64_t traceTimeElapsedUs,
                                                uint64_t prevTraceTimeElapsedUs, 
                                                SystemTimepoint prevSubmitTimepoint,
                                                uint64_t reqCount) {

        uint64_t ts = req.timestamp;
        uint64_t lba = std::stoull(req.key);
        uint64_t size = *(req.sizeBegin);

        bool writeFlag = true; 
        if (req.getOp() == OpType::kGet)
            writeFlag = false;
        
        if (reqCount > 1) 
            replayTimer(stats, traceTimeElapsedUs, prevTraceTimeElapsedUs, prevSubmitTimepoint);
        
        uint64_t submitIndex = addToPendingBlockRequestVec(ts, lba, size, writeFlag, threadId);
        while (submitIndex == config_.blockReplayConfig.maxPendingBlockRequestCount)
            submitIndex = addToPendingBlockRequestVec(ts, lba, size, writeFlag, threadId);

        addToQueue(submitIndex);
        SystemTimepoint newPrevSubmitTimepoint = std::chrono::system_clock::now();
        return newPrevSubmitTimepoint;
    }

    // time the replay according to the trace timestamps 
    void replayTimer(BlockReplayStats& stats, 
                        uint64_t traceTimeElapsedUs, 
                        uint64_t prevTraceTimeElapsedUs, 
                        SystemTimepoint previousSubmitTimepoint) {

        uint64_t replayTimeElapsedNs = getTestDurationNs();
        uint64_t traceTimeElapsedNs = traceTimeElapsedUs * 1000;
        uint64_t traceIatUs = traceTimeElapsedUs - prevTraceTimeElapsedUs;
        uint64_t physicalIatUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - previousSubmitTimepoint).count();
        
        int sleepTimeNs;
        if (config_.blockReplayConfig.globalClock) 
            sleepTimeNs = traceTimeElapsedNs - replayTimeElapsedNs;
        else 
            sleepTimeNs = (traceIatUs - physicalIatUs)*1000;
        
        if (sleepTimeNs > 0) {
            if (sleepTimeNs >= config_.blockReplayConfig.minSleepTimeNs)
                std::this_thread::sleep_for(std::chrono::nanoseconds(sleepTimeNs));
        } else 
            stats.physicalClockAheadCount++;
        
        int64_t timeDiffNs = traceTimeElapsedNs - getTestDurationNs();
        uint64_t physicalClockError = (100*abs(timeDiffNs))/traceTimeElapsedNs;
        physicalIatUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::system_clock::now() - previousSubmitTimepoint).count();

        stats.physicalIatUsPercentile->trackValue(physicalIatUs);
        stats.traceIatUsPercentile->trackValue(traceIatUs);
        stats.physicalClockPercentErrorPercentile->trackValue(physicalClockError);
    }

    // Replays a block trace on a Cachelib cache and files in backing store. 
    //
    // @param stats       Block replay stats 
    // @param fileIndex   The index used to map to resources of the file being replayed 
    void stressByBlockReplay(BlockReplayStats& stats, uint64_t threadId) {
        try {
            uint64_t prevTs;
            uint64_t reqCount = 0;
            SystemTimepoint prevSubmitTimepoint;
            
            std::mt19937_64 gen(folly::Random::rand64());
            std::optional<uint64_t> lastRequestId = std::nullopt;
            const Request& req(wg_->getReq(threadId, gen, lastRequestId));
            uint64_t startTs = req.timestamp;
            while (true) {
                reqCount++;
                uint64_t ts = req.timestamp; 
                prevSubmitTimepoint = submitToBlockStorageSystem(req, threadId, stats, ts-startTs, prevTs-startTs, prevSubmitTimepoint, reqCount);
                prevTs = ts; 
                const Request& req(wg_->getReq(threadId, gen, lastRequestId));
            }
        } catch (const cachebench::EndOfTrace& ex) {
        }
        
        stats.replayTimeNs = getTestDurationNs();
        wg_->markFinish();
        stressorTerminateFlag_.at(threadId) = true;
        // notify backing store if all replay threads are done 
        if (isReplayDone()) 
            backingStore_.setReplayDone();
    }

    static std::string genHardcodedString() {
        const std::string s = "The quick brown fox jumps over the lazy dog. ";
        std::string val;
        for (int i = 0; i < 4 * 1024 * 1024; i += s.size()) {
            val += s;
        }
        return val;
    }

    // worker threads doing replay, block request processing, async IO return processing, stat printing
    std::thread threads_;

    // the string used as data when data is sent to cache 
    const std::string hardcodedString_;

    // NOTE: we have only tried this stressor with BlockReplayGenerator 
    std::unique_ptr<GeneratorBase> wg_; 

    // flag indicating whether each replay thread has terminated 
    std::vector<bool>stressorTerminateFlag_;

    // tracking time elapsed at any point in time during runtime 
    mutable std::mutex timeMutex_;
    std::chrono::time_point<std::chrono::system_clock> startTime_;
    std::chrono::time_point<std::chrono::system_clock> endTime_;

    // vector of pending block requests in the system 
    // adjust maxPendingBlockRequestCount in config_.blockReplayConfig to adjust the size 
    mutable std::mutex pendingBlockReqMutex_;
    uint64_t pendingBlockReqCount_ = 0;
    std::vector<BlockRequest> pendingBlockReqVec_;

    uint64_t lbaSizeByte_;
    uint64_t blockSizeByte_;
    const StressorConfig config_; 
    BackingStore backingStore_;
    std::unique_ptr<CacheT> cache_;
    std::vector<BlockReplayStats> statsVec_;

    // FIFO queue of block requests in the system waiting to be processed 
    // processing threads pop this queue to get the request to process once free 
    mutable std::mutex blockRequestQueueMutex_;
    std::queue<uint64_t> blockRequestQueue_;

    std::vector<std::mutex> statUpdateLockVec_;

};

}
}
}