#pragma once

#include <stdio.h>
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
    AsyncIORequest(size_t requestSize, size_t backingStoreAlign) { 
        size_ = requestSize;       
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

    iocb *iocbPtr = new iocb(); 
    char *buffer;
    size_t size_;
    facebook::cachelib::util::LatencyTracker *tracker = nullptr;
};


class BlockRequest {

    public:
        BlockRequest(uint64_t offset, size_t size, OpType op, uint64_t pageSize, size_t alignment) {

            offset_ = offset; 
            size_ = size; 
            op_ = op; 
            pageSize_ = pageSize; 
            alignment_ = alignment;

            startPage_ = offset_/pageSize_;
            endPage_ = (offset_ + size_ - 1)/pageSize_; 

            // the index of the array can be mapped to specific pages 
            // how do we know once everything is completed which 
            // pages do we need to set? 

            if (op_ == OpType::kGet) 
                pendingAsyncIoVec_.resize(endPage_-startPage_+1);    
            else
                pendingAsyncIoVec_.resize(endPage_-startPage_+3); 
            
        }

        ~BlockRequest() {
            if (tracker_ != nullptr)
                delete tracker_;
        }

        uint64_t getStartPage() {
            return startPage_;
        }

        uint64_t getEndPage() {
            return endPage_;
        }

        // get the number of bytes by which the block request is not aligned in the front
        // also make sure either you don't do IO and if you do, 
        // it has to be larger than aligment size (e.g. 512 for HDD)
        size_t getFrontMisAlignment(size_t backingStoreAlignment) {
            size_t minOffset = startPage_*pageSize_;
            if (offset_-minOffset == 0) {
                return 0;
            } else {
                 return std::max(offset_-minOffset, backingStoreAlignment);
            }
        }

        // get the number of bytes by which the block request is not aligned in the rear
        // also make sure either you don't do IO and if you do, 
        // it has to be larger than aligment size (e.g. 512 for HDD)
        uint64_t getRearMisAlignment(uint64_t backingStoreAlignment) {
            uint64_t maxOffset = (endPage_+1)*pageSize_;
            if (maxOffset-offset_-size_ == 0) {
                return 0;
            } else {
                return std::max(maxOffset-offset_-size_, backingStoreAlignment);
            }
        }

        // get the total IO to be done for the block request based on read/write 
        size_t getTotalIo() {
            size_t totalIO;
            if (op_ == OpType::kGet) {
                totalIO =  (endPage_-startPage_+1)*pageSize_;
            } else {
                totalIO = getFrontMisAlignment(alignment_)+getRearMisAlignment(alignment_)+size_;
            }
            return totalIO;
        }

        uint64_t pageCount() {
            return endPage_-startPage_+1;
        }

        size_t getSize() {
            return size_;
        }

        size_t getOffset() {
            return offset_;
        }

        uint64_t getPageSize() {
            return pageSize_;
        }

        bool isRequestProcessed() {
            return requestProcessed_;
        }

        OpType getOp() {
            return op_;
        }

        // get an empty spot in the list of AsyncIORequest objects 
        uint64_t getAsyncIoIndex(iocb *iocbPtr) {
            uint64_t i = pendingAsyncIoVec_.size();
            for (uint64_t index=0; index<pendingAsyncIoVec_.size(); index++) {
                if (pendingAsyncIoVec_.at(index) != nullptr) {
                    if (pendingAsyncIoVec_.at(index)->iocbPtr == iocbPtr) {
                        i = index;
                        break;
                    }
                }
            }
            return i;
        }

        // on completion of async IO, this callback is called 
        void ioCompleted(iocb *iocbPtrDone) {
            std::lock_guard<std::mutex> l(updateMutex_);
            uint64_t i = getAsyncIoIndex(iocbPtrDone);
            if (i == pendingAsyncIoVec_.size()) {
                throw std::runtime_error(
                    folly::sformat("IOCB pointer not found when completed. \n"));
            } 
            ioReturned_++;
            ioProcessed_ += iocbPtrDone->u.v.nr;
            delete pendingAsyncIoVec_.at(i);
            pendingAsyncIoVec_.at(i) = nullptr;
            checkIfRequestProcessed();
        }

        void startLatencyTracking(facebook::cachelib::util::PercentileStats& stats) {
            tracker_ = new facebook::cachelib::util::LatencyTracker(stats);
        }

        // check if all the bytes of block request have been processed 
        // either a hit or miss 
        void checkIfRequestProcessed() {
            if (ioProcessed_ >= getTotalIo()) {
                requestProcessed_ = true;
                if (tracker_ != nullptr) {
                    delete tracker_; 
                    tracker_ = nullptr;
                }
            }
        }


        // initiate an AsyncIORequest on a cache miss 
        AsyncIORequest* miss(size_t size, size_t alignment) {
            std::lock_guard<std::mutex> l(updateMutex_);
            uint64_t index = pendingAsyncIoVec_.size();
            for (uint64_t i=0; i<pendingAsyncIoVec_.size(); i++) {
                if (pendingAsyncIoVec_.at(i) == nullptr) {
                    pendingAsyncIoVec_.at(i) = new AsyncIORequest(size, alignment);
                    index = i;
                    break;
                }
            }

            return pendingAsyncIoVec_.at(index);
        }


        void addMissKey(uint64_t key) {
            missKeyVec_.push_back(key);
        }


        bool checkKeyMiss(uint64_t key) {
            if (std::find(missKeyVec_.begin(), missKeyVec_.end(), key) != missKeyVec_.end()) {
                return true;
            } else {
                return false; 
            }
        }


        // set specified bytes as a cache hit 
        void hit(size_t size) {
            ioProcessed_ += size; 
            checkIfRequestProcessed();
        }

    // properties of the block request 
    OpType op_;
    uint64_t offset_;
    uint64_t size_;
    uint64_t pageSize_;
    uint64_t startPage_;
    uint64_t endPage_;
    size_t alignment_;

    // tracking how much of the IO has been processed 
    // either marked as a hit or request to backing store completed on miss 
    size_t ioProcessed_ = 0;
    uint64_t ioReturned_ = 0;
    bool requestProcessed_ = false;
    std::vector<AsyncIORequest*> pendingAsyncIoVec_;
    std::vector<uint64_t> missKeyVec_;

    // track the latency between starting tracking and the request being processed 
    facebook::cachelib::util::LatencyTracker *tracker_ = nullptr;

    // mutex to control access to pendingAsyncIoVec_ 
    mutable std::mutex updateMutex_;

};

}
}
}