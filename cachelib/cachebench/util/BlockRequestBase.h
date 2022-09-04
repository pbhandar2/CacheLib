#pragma once

#include <stdio.h>
#include <chrono> 
#include <algorithm> 

#include "cachelib/cachebench/util/Request.h"
#include "cachelib/allocator/Util.h"
#include "cachelib/common/PercentileStats.h"


namespace facebook {
namespace cachelib {
namespace cachebench {


struct AsyncIORequest {
    AsyncIORequest() {}


    ~AsyncIORequest() {
        if (size_ > 0) {
            if (tracker_ != nullptr)
                delete tracker_;
            delete iocbPtr_;
        }
    }


    // load an async IO request made to disk 
    void load(uint64_t offset, uint64_t size, bool writeFlag, uint64_t asyncIOIndex, uint64_t blockRequestIndex) { 
        // when the load is called it should always be when size = 0 (no data is set)
        // should not be overwriting the object 
        if (size_ > 0) 
            throw std::runtime_error(folly::sformat("load: data already loaded in async request, cannot overwrite, size>0 \n"));

        offset_ = offset; 
        size_ = size;
        asyncIOIndex_ = asyncIOIndex;
        blockRequestIndex_ = blockRequestIndex;   
        writeFlag_ = writeFlag;    
        iocbPtr_ = new iocb();
    }


    // async IO has been completed, reset 
    void reset() {
        // should not be reseting an empty object 
        if (size_ == 0) 
            throw std::runtime_error(folly::sformat("reset(): data not loaded in async request, size=0 \n"));
        
        size_ = 0;
        delete iocbPtr_;
        if (tracker_ != nullptr) {
            delete tracker_;
            tracker_ = nullptr;
        }
    }


    void startLatencyTracking(facebook::cachelib::util::PercentileStats& stats) {
        tracker_ = new facebook::cachelib::util::LatencyTracker(stats);
    }


    bool isDataLoaded() {
        return size_ > 0; 
    }


    uint64_t getSize() {
        return size_;
    }


    uint64_t getOffset() {
        return offset_;
    }


    uint64_t getWriteFlag() {
        return writeFlag_;
    }


    uint64_t getAsyncIOIndex() {
        return asyncIOIndex_;
    }


    uint64_t getBlockRequestIndex() {
        return blockRequestIndex_;
    }


    uint64_t size_ = 0;
    uint64_t blockRequestIndex_;
    uint64_t asyncIOIndex_;
    uint64_t offset_;
    bool writeFlag_;
    iocb *iocbPtr_;
    facebook::cachelib::util::LatencyTracker *tracker_ = nullptr;
};


class BlockRequest {
    public:
        BlockRequest() {}


        ~BlockRequest(){
            if (size_ > 0) {
                delete sLatTracker_;
                if (cLatTracker_ != nullptr) {
                    delete cLatTracker_;
                    cLatTracker_ = nullptr;
                }
            }
            missKeyVec_.clear();
            missByteRangeVec_.clear();
        }


        void load(uint64_t lba, 
                    uint64_t size, 
                    OpType op, 
                    uint64_t pageSize, 
                    uint64_t lbaSize,
                    uint64_t key,
                    facebook::cachelib::util::PercentileStats& stats) {
            // when the load is called it should always be when size = 0 (no data is set)
            // should not be overwriting the object 
            if (size_ > 0) 
                throw std::runtime_error(folly::sformat("load: data already loaded, cannot overwrite, size>0 \n"));

            lba_ = lba;
            size_ = size; 
            op_ = op;
            pageSize_ = pageSize; 
            lbaSize_ = lbaSize;
            key_ = key; 

            offset_ = lba_*lbaSize_;
            startPage_ = offset_/pageSize_;
            endPage_ = (offset_ + size_ - 1)/pageSize_; 
            frontAlignment_ = offset_ - (startPage_*pageSize_);
            rearAlignment_ = (endPage_*pageSize_) - offset_ - size_;

            if (op == OpType::kGet) {
                totalIO_ = (endPage_ - startPage_ + 1) * pageSize_;
            } else if (op == OpType::kSet) {
                totalIO_ = size_;
                if (frontAlignment_>0)
                    totalIO_ += pageSize_;
                if ((rearAlignment_ > 0) && ((getPageCount() > 1) || (frontAlignment_ == 0)))
                    totalIO_ += pageSize_;
            } else {
                throw std::runtime_error(folly::sformat("Unknown req in BlockRequest\n"));
            }

            sLatTracker_ = new facebook::cachelib::util::LatencyTracker(stats);
        }


        void reset() {
            // should not be reseting an empty object 
            if (size_ == 0) 
                throw std::runtime_error(folly::sformat("reset(): data not loaded, size=0 \n"));

            size_ = 0;
            delete sLatTracker_;
            if (cLatTracker_ != nullptr)
                delete cLatTracker_;
                cLatTracker_ = nullptr;

            hitBytes_ = 0;
            readAsyncCount_ = 0;
            writeAsyncCount_ = 0;
            readAsyncBytes_ = 0;
            writeAsyncBytes_ = 0;
            totalMissByte_ = 0;
            missCount_ = 0;

            missKeyVec_.clear();
            missByteRangeVec_.clear();
        }


        bool isDataLoaded() {
            return size_>0; 
        }


        void startLatencyTracking(facebook::cachelib::util::PercentileStats& stats) {
            cLatTracker_ = new facebook::cachelib::util::LatencyTracker(stats);
        }
    

        std::string printStatus() {
            std::string out;
            out.append(folly::sformat("Read Async: {},", readAsyncBytes_));
            out.append(folly::sformat("Write Async: {},", writeAsyncBytes_));
            out.append(folly::sformat("Read async count: {},", readAsyncCount_));
            out.append(folly::sformat("Write async count: {},", writeAsyncCount_));
            out.append(folly::sformat("Hit: {},", hitBytes_));   
            out.append(folly::sformat("Miss count: {}", missCount_));       
            return out; 
        }


        uint64_t getKey() {
            return key_;
        }


        bool isBlockRequestProcessed() {
            std::lock_guard<std::mutex> l(updateMutex_);
            bool doneFlag = true; 
            for (auto missByteRange : missByteRangeVec_) {
                if (std::get<3>(missByteRange) == false) {
                    doneFlag = false; 
                    break;
                }   
            }
            return doneFlag; 
        }


        // bool isBlockRequestProcessed() {
        //     std::lock_guard<std::mutex> l(updateMutex_);
        //     bool doneFlag = false; 
        //     if (op_ == OpType::kGet) {
        //         uint64_t totalIOProcessed = hitBytes_+readAsyncBytes_;
        //         if (totalIOProcessed > totalIO_) {
        //             throw std::runtime_error(folly::sformat("Excess read -> {}\n", printStatus()));
        //         } else if (totalIOProcessed == totalIO_) {
        //             doneFlag = true; 
        //         }
        //     } else if (op_ == OpType::kSet) {
        //         uint64_t totalIOProcessed = hitBytes_+writeAsyncBytes_+readAsyncBytes_;
        //         if (totalIOProcessed == totalIO_) {
        //             doneFlag = true; 
        //         } else if (totalIOProcessed > totalIO_) {
        //             throw std::runtime_error(folly::sformat("Excess write -> {}\n", printStatus()));
        //         }
        //     } else {
        //         throw std::runtime_error(folly::sformat("Unknown req isBlockProcessed()\n"));
        //     }

        //     if (missCount_ == returnCount_) {
        //         if (!doneFlag) {
        //             throw std::runtime_error(folly::sformat("Should be done but isn't \n"));
        //         }
        //     }

        //     return doneFlag; 
        // }


        uint64_t getTotalIO() {
            return totalIO_;
        }


        uint64_t getStartPage() {
            return startPage_;
        }


        uint64_t getOffset() {
            return offset_;
        }


        uint64_t getFrontAlignment() {
            return frontAlignment_;
        }


        uint64_t getRearAlignment() {
            return rearAlignment_;
        }


        uint64_t getEndPage() {
            return endPage_;
        }

        uint64_t getPageCount() {
            return endPage_ - startPage_ + 1;
        }


        bool checkKeyMiss(uint64_t key) {
            if (std::find(missKeyVec_.begin(), missKeyVec_.end(), key) != missKeyVec_.end()) {
                return true;
            } else {
                return false; 
            }
        }


        uint64_t getLBA() {
            return lba_;
        }


        OpType getOp() {
            return op_;
        }


        uint64_t getSize() {
            return size_;
        }


        void addMissKey(uint64_t key) {
            std::lock_guard<std::mutex> l(updateMutex_);
            missKeyVec_.push_back(key);
        }


        void hit(uint64_t size) {
            std::lock_guard<std::mutex> l(updateMutex_);
            hitBytes_ += size; 
        }


        void asyncReturn(iocb* iocbPtr, uint64_t asyncOffset, uint64_t asyncSize, bool writeFlag) {
            std::lock_guard<std::mutex> l(updateMutex_);
            bool found = false; 
            uint64_t asyncEndOffset = asyncOffset + asyncSize;
            // find the async IO to which it belongs by iterating over missing byte ranges 
            for (uint64_t missIndex=0; missIndex < missCount_; missIndex++) {
                // get the start and end offset of the current miss byte range 
                auto curMissByteRange = missByteRangeVec_.at(missIndex);
                uint64_t missByteRangeStartOffset = std::get<0>(curMissByteRange);
                uint64_t missByteRangeSize = std::get<1>(curMissByteRange);
                uint64_t missByteRangeWriteFlag = std::get<2>(curMissByteRange);
                uint64_t missByteRangeEndOffset = missByteRangeStartOffset + missByteRangeSize;
                 
                // an async IO can larger than the miss byte range dur to alignment issues 
                // for an async IO to belong to a paritcular miss byte range, the miss byte range has to be contained in async IO
                // the start offset and end offset of the miss byte range must be between start and end offset of async IO 
                bool missWriteFlag = std::get<2>(missByteRangeVec_.at(missIndex));
                if ((asyncOffset <= missByteRangeStartOffset) && (asyncEndOffset >= missByteRangeEndOffset) && (missByteRangeWriteFlag == writeFlag)) {
                    found = true; 
                    // async IO found 
                    // check to make sure this IO has not returned already 
                    if (std::get<3>(missByteRangeVec_.at(missIndex)) == true) {
                        throw std::runtime_error(folly::sformat("IO already returned! \n {} \n", printStatus()));
                    }
                    std::get<3>(missByteRangeVec_.at(missIndex)) = true; 
                    break;
                }
            }

            if (!found)
                throw std::runtime_error(folly::sformat("Could not find miss byte range corresponding to this asycn IO! \n {} \n", printStatus()));

            if (writeFlag) {
                writeAsyncBytes_ += asyncSize;
                writeAsyncCount_++;
            } else {
                readAsyncBytes_ += asyncSize;
                readAsyncCount_++;
            }
        }


        void addCacheMissByteRange(uint64_t offset, uint64_t size, bool writeFlag) {
            std::lock_guard<std::mutex> l(updateMutex_);
            missByteRangeVec_.push_back(std::make_tuple(offset, size, writeFlag, false));
            totalMissByte_ += size; 
            missCount_++;
        }


        std::vector<std::tuple<uint64_t, uint64_t, bool, bool>> getMissByteRangeVec() {
            return missByteRangeVec_;
        }


        std::vector<std::tuple<uint64_t, uint64_t, bool>> getAsyncIO(uint64_t maxDiskOffset) {
            std::lock_guard<std::mutex> l(updateMutex_);
            std::vector<std::tuple<uint64_t, uint64_t, bool>> asyncIOVec;
            // get all async IO request to be submitted for each cache miss / write request 
            for (auto missByteRangeEntry : missByteRangeVec_) {
                
                // write requests should always be offset aligned 
                // read request can be not offset aligned and extra can be read for it,
                // but extra cannot be written for 
                uint64_t offset = std::get<0>(missByteRangeEntry);
                uint64_t alignedOffset = offset - (offset % lbaSize_);

                // we need to read extra bytes because page start offset is not a multiple 
                // of backing store alignment 
                uint64_t offsetGap = offset - alignedOffset;
                uint64_t size = std::get<1>(missByteRangeEntry);
                uint64_t totalSize = offsetGap + size;

                // request size should also be a multiple of backing store alignment 
                uint64_t alignedSize = (totalSize + lbaSize_) - ((totalSize + lbaSize_) % lbaSize_);
                if (((totalSize + lbaSize_) % lbaSize_) == 0)
                    alignedSize = totalSize;
                
                // make sure both offset and size are aligned 
                if ((alignedOffset % lbaSize_ > 0) || (alignedSize % lbaSize_ > 0)) {
                    throw std::runtime_error(folly::sformat("offset and/or size not aligned."));
                }

                bool writeFlag = std::get<2>(missByteRangeEntry);

                if (alignedSize > maxDiskOffset) {
                    throw std::runtime_error(folly::sformat("Size too large for the size of disk file"));
                }

                if (alignedOffset + alignedSize >= maxDiskOffset) {
                    if (alignedOffset > maxDiskOffset) {
                        alignedOffset = alignedOffset % maxDiskOffset;
                    }

                    if (alignedOffset + alignedSize > maxDiskOffset) {
                        alignedOffset = 0;
                    }
                }


                auto asyncIOEntry = std::make_tuple(alignedOffset, alignedSize, writeFlag);
                asyncIOVec.push_back(asyncIOEntry);
            }
            return asyncIOVec;
        }
    
    
    std::vector<std::tuple<uint64_t, uint64_t, bool, bool>> missByteRangeVec_;
    uint64_t totalMissByte_ = 0;
    uint64_t missCount_ = 0;
    uint64_t hitBytes_=0;
    uint64_t readAsyncCount_=0;
    uint64_t writeAsyncCount_=0;
    uint64_t readAsyncBytes_=0;
    uint64_t writeAsyncBytes_=0;

    OpType op_;

    uint64_t key_;
    uint64_t lba_;
    uint64_t size_=0;
    uint64_t offset_;
    uint64_t pageSize_;
    uint64_t lbaSize_;
    uint64_t startPage_;
    uint64_t endPage_;
    uint64_t totalIO_;
    uint64_t frontAlignment_;
    uint64_t rearAlignment_;

    std::vector<uint64_t> missKeyVec_;
    mutable std::mutex updateMutex_;
    facebook::cachelib::util::LatencyTracker *sLatTracker_ = nullptr;
    facebook::cachelib::util::LatencyTracker *cLatTracker_ = nullptr;
};

}
}
}