#pragma once

#include <chrono>
#include <thread>

namespace facebook {
namespace cachelib {
namespace cachebench {

class BlockRequest {
    public:

        BlockRequest(uint64_t lbaSizeByte, uint64_t blockSizeByte){
            lbaSizeByte_ = lbaSizeByte;
            blockSizeByte_ = blockSizeByte;
        }


        BlockRequest(uint64_t lbaSizeByte, 
                        uint64_t blockSizeByte,
                        uint64_t physicalTs, // timestamp of when request was read 
                        uint64_t traceTs, 
                        uint64_t iat, 
                        uint64_t lba, 
                        uint64_t size, 
                        bool writeFlag, 
                        uint64_t blockRequestIndex, 
                        uint64_t threadId){
            lbaSizeByte_ = lbaSizeByte;
            blockSizeByte_ = blockSizeByte;
            load(physicalTs, traceTs, iat, lba, size, writeFlag, blockRequestIndex, threadId);
        }


        void load(uint64_t physicalTs, 
                    uint64_t ts, 
                    uint64_t iat, 
                    uint64_t lba, 
                    uint64_t size, 
                    bool writeFlag, 
                    uint64_t blockRequestIndex, 
                    uint64_t threadId) {
                    
            physicalTs_ = physicalTs;
            traceTs_ = ts; 
            lba_ = lba;
            size_ = size;
            writeFlag_ = writeFlag;
            blockRequestIndex_ = blockRequestIndex;
            threadId_ = threadId;
            iatUs_ = iat; 

            offsetByte_ = lba_ * lbaSizeByte_;
            startBlock_ = floor(offsetByte_/blockSizeByte_);
            endBlock_ = floor((offsetByte_ + size_ - 1)/blockSizeByte_);
            frontMisalignByte_ = offsetByte_ - (startBlock_ * blockSizeByte_);
            rearMisalignByte_ = ((endBlock_+1) * blockSizeByte_) - (offsetByte_ + size_);

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
            
            if (blockIoCompletionVec_.size() == 0)
                throw std::runtime_error("The blockIoCompletionVec is 0 \n");
            
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
            hitsTracked_ = false; 
            size_ = 0;
            blockHitCount_ = 0;
            readHitByte_ = 0; 
            blockIoCompletionVec_.clear();
        }

        uint64_t getPhysicalTs() {
            return physicalTs_;
        }

        uint64_t getTs() {
            return traceTs_;
        }

        uint64_t getIatUs() {
            return iatUs_;
        }

        uint64_t getLba() {
            return lba_;
        }

        uint64_t blockCount() {
            return endBlock_ - startBlock_ + 1;
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

        void setIatUs(uint64_t iatUs) {
            iatUs_ = iatUs; 
        }

        std::vector<bool> getBlockIoCompletionVec() {
            return blockIoCompletionVec_;
        }

        void setCacheHit(uint64_t blockIndex, bool updateHitData) {
            if (blockIndex >= blockIoCompletionVec_.size())
                throw std::runtime_error(folly::sformat("IO index {} too high max {} writeFlag: {} start: {} end: {}, size: {}\n", blockIndex, 
                                                                                                            blockIoCompletionVec_.size(), 
                                                                                                            writeFlag_,
                                                                                                            startBlock_,
                                                                                                            endBlock_, size_));
            blockIoCompletionVec_.at(blockIndex) = true; 
            if (updateHitData) {
                blockHitCount_++;

                if (writeFlag_) {
                    if ((blockIndex == 0) & (frontMisalignByte_ > 0)) {
                        readHitByte_ += frontMisalignByte_;
                    }
                    if ((blockIndex == (blockIoCompletionVec_.size()-1)) & (rearMisalignByte_ > 0)){
                        readHitByte_ += rearMisalignByte_;
                    }
                } else {
                    readHitByte_ += blockSizeByte_;
                    if ((blockIndex == 0) & (frontMisalignByte_ > 0)) {
                        readHitByte_ -= (frontMisalignByte_);
                    }
                    if ((blockIndex == (blockIoCompletionVec_.size()-1)) & (rearMisalignByte_ > 0)){
                        readHitByte_ -= (rearMisalignByte_);
                    }
                } 
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


    uint64_t traceTs_;
    uint64_t physicalTs_;

    bool writeFlag_;
    uint64_t size_ = 0;
    uint64_t blockHitCount_ = 0;
    uint64_t readHitByte_ = 0;
    bool hitsTracked_ = false; 

    uint64_t lba_;
    uint64_t offsetByte_;
    
    uint64_t lbaSizeByte_;
    uint64_t blockSizeByte_;
    uint64_t blockRequestIndex_;
    uint64_t threadId_;
    uint64_t startBlock_;
    uint64_t endBlock_;
    uint64_t frontMisalignByte_;
    uint64_t rearMisalignByte_;
    uint64_t iatUs_;
    std::vector<bool> blockIoCompletionVec_;
};

}
}
}