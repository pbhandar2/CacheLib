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
    AsyncIORequest(uint64_t requestSize, uint64_t backingStoreAlign, uint64_t key) { 
        size_ = requestSize;       
        key_ = key;
        if (requestSize < backingStoreAlign) {
            throw std::runtime_error(
                folly::sformat("Size: {} < alignment: {} \n", requestSize, backingStoreAlign));
        }
        try {
            int ret = posix_memalign((void**) &buffer, backingStoreAlign, requestSize);
            if (ret != 0) {
                throw std::runtime_error(
                    folly::sformat("Error in posix_memalign, return: {}\n", ret));
            }
        } catch (...) {
            throw std::runtime_error(
                folly::sformat("POSIX memalign failed! align: {} size: {}\n", backingStoreAlign, requestSize));
        }
    }

    ~AsyncIORequest() {
        if (tracker != nullptr)
            delete tracker;
        delete buffer;
        delete iocbPtr;
    }

    void startLatencyTracking(facebook::cachelib::util::PercentileStats& stats) {
        tracker = new facebook::cachelib::util::LatencyTracker(stats);
    }


    uint64_t getSize() {
        return size_;
    }

    uint64_t getKey() {
        return key_;
    }

    iocb *iocbPtr = new iocb(); 
    char *buffer;
    uint64_t key_;
    uint64_t size_;
    facebook::cachelib::util::LatencyTracker *tracker = nullptr;
};


class BlockRequest {
    public:
        BlockRequest(uint64_t lba, 
                        uint64_t size, 
                        OpType op, 
                        uint64_t pageSize, 
                        uint64_t lbaSize,
                        uint64_t key,
                        facebook::cachelib::util::PercentileStats& stats) {

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
            startTime_ = std::chrono::system_clock::now();
        }


        ~BlockRequest(){
            delete sLatTracker_;
            if (cLatTracker_ != nullptr)
                delete cLatTracker_;
        }


        void startLatencyTracking(facebook::cachelib::util::PercentileStats& stats) {
            cLatTracker_ = new facebook::cachelib::util::LatencyTracker(stats);
        }


        void startClatTracking() {
            cStartTime_ = std::chrono::system_clock::now();
        }
    

        std::string printStatus() {
            std::string out;
            out.append(folly::sformat("Read Async: {},", readAsyncBytes_));
            out.append(folly::sformat("Write Async: {},", writeAsyncBytes_));
            out.append(folly::sformat("Read async count: {},", readAsyncCount_));
            out.append(folly::sformat("Write async count: {},", writeAsyncCount_));
            out.append(folly::sformat("Total IO: {},", totalIO_));

            std::string iocbStr;
            for (uint64_t i=0; i<iocbVec_.size(); i++) {
                iocbStr.append(folly::sformat("{}-", iocbVec_.at(i)));
            }
            out.append(folly::sformat("IOCB: {}\n", iocbStr));
        
            return out; 
        }


        int getLatency() {
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now()-startTime_).count();
            return duration; 
        }

        int getClat() {
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now()-cStartTime_).count();
            return duration; 
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
                    // std::cout<<folly::sformat("warning: Excess read -> {}\n", printStatus());
                    // doneFlag = true;
                } else if (totalIOProcessed == totalIO_) {
                    doneFlag = true; 
                }
            } else if (op_ == OpType::kSet) {
                uint64_t totalIOProcessed = writeAsyncBytes_+readAsyncBytes_;
                if (totalIOProcessed == totalIO_) {
                    doneFlag = true; 
                } else if (totalIOProcessed > totalIO_) {
                    throw std::runtime_error(folly::sformat("Excess write -> {}\n", printStatus()));
                    // std::cout<<folly::sformat("warning: Excess write -> {}\n", printStatus());
                    // doneFlag = true; 
                }
            } else {
                throw std::runtime_error(folly::sformat("Unknown req isBlockProcessed()\n"));
            }

            if (doneFlag)
                endTime_ = std::chrono::system_clock::now();

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


        void trackIOCB(iocb* iocbPtr) {
            iocbVec_.push_back(iocbPtr);
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
    uint64_t size_;
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
    std::vector<iocb*> iocbVec_;

    mutable std::mutex updateMutex_;
    facebook::cachelib::util::LatencyTracker *sLatTracker_ = nullptr;
    facebook::cachelib::util::LatencyTracker *cLatTracker_ = nullptr;

    std::chrono::system_clock::time_point startTime_;
    std::chrono::system_clock::time_point cStartTime_;
    std::chrono::system_clock::time_point endTime_;

};

}
}
}