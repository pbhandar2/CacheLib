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


//  Operations that the block cache stressor supports
//  They translate into the following cachelib operations
//  Write: allocate + insertOrReplace
//  Read: find
//  Remove: remove
enum class BlockOpType {
  kWrite = 0,
  kRead,
  kRemove
};


enum class BlockOpResultType {
  kNop = 0,
  kReadMiss,
  kReadHit,
  kLoadSuccess,
  kLoadFailure
};


struct AsyncIORequest {
    AsyncIORequest() {}

    ~AsyncIORequest() {
        if (size_ > 0) {
            if (tracker_ != nullptr)
                delete tracker_;
            delete iocbPtr_;
        }
    }

    void loader(uint64_t size,             uint64_t asyncIOIndex,
            uint64_t blockRequestIndex) {
        size_ = size;
        asyncIOIndex_ = asyncIOIndex;
        blockRequestIndex_ = blockRequestIndex;  
    }

    void load(uint64_t requestSize, 
            uint64_t backingStoreAlign, 
            uint64_t asyncIOIndex,
            uint64_t blockRequestIndex) { 

        if (size_ > 0) 
            throw std::runtime_error(folly::sformat("load: data already loaded in async request, cannot overwrite, size>0 \n"));

        size_ = requestSize;
        asyncIOIndex_ = asyncIOIndex;
        blockRequestIndex_ = blockRequestIndex;       
        iocbPtr_ = new iocb();
    }


    void reset() {
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
        return size_>0; 
    }


    uint64_t getSize() {
        return size_;
    }


    uint64_t getAsyncIOIndex() {
        return asyncIOIndex_;
    }


    uint64_t getBlockRequestIndex() {
        return blockRequestIndex_;
    }


    iocb *iocbPtr_;
    uint64_t blockRequestIndex_;
    uint64_t asyncIOIndex_;
    uint64_t size_=0;
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
        }


        void load(uint64_t lba, 
                        uint64_t size, 
                        OpType op, 
                        uint64_t pageSize, 
                        uint64_t lbaSize,
                        uint64_t key,
                        facebook::cachelib::util::PercentileStats& stats) {

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
                if (rearAlignment_>0)
                    totalIO_ += pageSize_;
            } else {
                throw std::runtime_error(folly::sformat("Unknown req in BlockRequest\n"));
            }

            sLatTracker_ = new facebook::cachelib::util::LatencyTracker(stats);
        }


        void reset() {
            if (size_ == 0) 
                throw std::runtime_error(folly::sformat("reset(): data not loaded, size=0 \n"));

            size_ = 0;
            delete sLatTracker_;
            if (cLatTracker_ != nullptr)
                delete cLatTracker_;
                cLatTracker_ = nullptr;

            hitBytes_=0;
            readAsyncCount_=0;
            writeAsyncCount_=0;
            readAsyncBytes_=0;
            writeAsyncBytes_=0;
            missKeyVec_.clear();
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
            out.append(folly::sformat("Total IO: {},", totalIO_));        
            return out; 
        }


        uint64_t getKey() {
            return key_;
        }


        bool isBlockRequestProcessed() {
            std::lock_guard<std::mutex> l(updateMutex_);
            bool doneFlag = false; 
            if (op_ == OpType::kGet) {
                uint64_t totalIOProcessed = hitBytes_+readAsyncBytes_;
                if (totalIOProcessed > totalIO_) {
                    throw std::runtime_error(folly::sformat("Excess read -> {}\n", printStatus()));
                } else if (totalIOProcessed == totalIO_) {
                    doneFlag = true; 
                }
            } else if (op_ == OpType::kSet) {
                uint64_t totalIOProcessed = writeAsyncBytes_+readAsyncBytes_;
                if (totalIOProcessed == totalIO_) {
                    doneFlag = true; 
                } else if (totalIOProcessed > totalIO_) {
                    throw std::runtime_error(folly::sformat("Excess write -> {}\n", printStatus()));
                }
            } else {
                throw std::runtime_error(folly::sformat("Unknown req isBlockProcessed()\n"));
            }

            return doneFlag; 
        }


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


        void async(uint64_t size, bool writeFlag) {
            std::lock_guard<std::mutex> l(updateMutex_);
            assert(size<=totalIO_);
            if (writeFlag) {
                writeAsyncBytes_ += size;
                writeAsyncCount_++;
            } else {
                readAsyncBytes_ += size;
                readAsyncCount_++;
            }
        }
    
    
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
    
    uint64_t hitBytes_=0;
    uint64_t readAsyncCount_=0;
    uint64_t writeAsyncCount_=0;
    uint64_t readAsyncBytes_=0;
    uint64_t writeAsyncBytes_=0;

    std::vector<uint64_t> missKeyVec_;
    mutable std::mutex updateMutex_;
    facebook::cachelib::util::LatencyTracker *sLatTracker_ = nullptr;
    facebook::cachelib::util::LatencyTracker *cLatTracker_ = nullptr;
};

}
}
}