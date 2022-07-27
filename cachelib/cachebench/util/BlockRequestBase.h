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
    AsyncIORequest(uint64_t requestSize, uint64_t backingStoreAlign) { 
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
    uint64_t size_;
    facebook::cachelib::util::LatencyTracker *tracker = nullptr;
};


class BlockRequest {
    public:
        BlockRequest(uint64_t offset, 
                    uint64_t size, 
                    OpType op, 
                    uint64_t pageSize, 
                    uint64_t alignment) {

            offset_ = offset; 
            size_ = size; 
            op_ = op; 
            pageSize_ = pageSize; 
            alignment_ = alignment;

            startPage_ = offset_/pageSize_;
            endPage_ = (offset_ + size_ - 1)/pageSize_; 

            if (op_ == OpType::kGet)  {
                pendingAsyncIoVec_.resize(endPage_-startPage_+2);
                completedIOCBPtr_.resize(endPage_-startPage_+2); 
            }
            else if (op_ == OpType::kSet) {
                pendingAsyncIoVec_.resize(endPage_-startPage_+4);
                completedIOCBPtr_.resize(endPage_-startPage_+4);
            }
            else
                throw std::runtime_error(folly::sformat("OP not recognized \n"));
                
            for (uint64_t i=0; i<pendingAsyncIoVec_.size(); i++){
                pendingAsyncIoVec_.at(i) = nullptr;
                completedIOCBPtr_.at(i) = nullptr;
            }
                
        }


        ~BlockRequest() {
            checkAllDone();
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
        uint64_t getFrontMisAlignment(uint64_t backingStoreAlignment) {
            uint64_t minOffset = startPage_*pageSize_;
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
        uint64_t getTotalIo() {
            uint64_t totalIO;
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


        uint64_t getSize() {
            return size_;
        }


        uint64_t getOffset() {
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
                        return index;
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
                    folly::sformat("No space for pending async IO. {} {} {} {} {} {} {} {} {} \n", 
                                    i, 
                                    ioProcessed_, 
                                    ioReturned_, 
                                    getTotalIo(), 
                                    pendingAsyncIoVec_.at(0),
                                    pageCount(),
                                    missKeyVec_.size(),
                                    hitBytes_,
                                    missBytes_));
            } 
            ioProcessed_ += iocbPtrDone->u.v.nr;
            ioReturned_++;
            
            // std::cout << folly::sformat("Pending RESET at {} FROM: {} ", i, pendingAsyncIoVec_.at(i));
            // std::cout << folly::sformat("Completed RESET at {} FROM: {}\n", i, completedIOCBPtr_.at(i));
            delete iocbPtrDone;
            pendingAsyncIoVec_.at(i) = nullptr;
            completedIOCBPtr_.at(i) = iocbPtrDone;
            // std::cout << folly::sformat("Pending RESET at {} TO: {}", i, pendingAsyncIoVec_.at(i));
            // std::cout << folly::sformat("Completed RESET at {} TO: {} \n", i, completedIOCBPtr_.at(i));

            // checkIfRequestProcessed();
        }


        void startLatencyTracking(facebook::cachelib::util::PercentileStats& stats) {
            tracker_ = new facebook::cachelib::util::LatencyTracker(stats);
        }


        uint64_t currentPendingIOCount() {
            uint64_t ioCount = 0;
            for (uint64_t index=0; index<pendingAsyncIoVec_.size(); index++) {
                if (pendingAsyncIoVec_.at(index) != nullptr) {
                    ioCount += 1;
                }
            }
            return ioCount;
        }


        uint64_t currentCompletedIOCount() {
            uint64_t ioCount = 0;
            for (uint64_t index=0; index<completedIOCBPtr_.size(); index++) {
                if (completedIOCBPtr_.at(index) != nullptr) {
                    ioCount += 1;
                }
            }
            return ioCount;
        }


        std::string getPointerString() {
            std::string out; 
            out.append(folly::sformat("\nPending \n"));
            for (uint64_t index=0; index<pendingAsyncIoVec_.size(); index++) {
                if (pendingAsyncIoVec_.at(index) != nullptr) {
                    out.append(folly::sformat("{}-", pendingAsyncIoVec_.at(index)));
                }
            }
            out.append(folly::sformat("\n---------------------\n"));
            out.append(folly::sformat("\nCompleted \n"));
            for (uint64_t index=0; index<completedIOCBPtr_.size(); index++) {
                if (pendingAsyncIoVec_.at(index) != nullptr) {
                    out.append(folly::sformat("{}-", pendingAsyncIoVec_.at(index)));
                }
            }
            return out; 
        }


        std::string getPrintString() {
            std::string out;
            out.append(folly::sformat("Total IO: {},", getTotalIo()));
            out.append(folly::sformat("Page Count: {},", pageCount()));
            out.append(folly::sformat("Io Returned: {},", ioReturned_));
            out.append(folly::sformat("Io Processed: {},", ioProcessed_));
            out.append(folly::sformat("Async Io Count: {},", missKeyVec_.size()));
            out.append(folly::sformat("Hit Bytes: {},", hitBytes_));
            out.append(folly::sformat("Miss Bytes: {},", missBytes_));
            out.append(folly::sformat("Miss Count: {},", missCount_));
            out.append(folly::sformat("Offset: {},", offset_));
            out.append(folly::sformat("Front align: {},", getFrontMisAlignment(alignment_)));
            out.append(folly::sformat("Back align: {},", getRearMisAlignment(alignment_)));

            if (getOp() == OpType::kGet) {
                out.append(folly::sformat("Op: Read,"));
            } else {
                out.append(folly::sformat("Op: Write,"));
            }

            out.append(folly::sformat("Pending Io Count: {},", currentPendingIOCount()));
            out.append(folly::sformat("Completed Io Count: {}\n", currentCompletedIOCount()));
            return out;
        }


        void printStatus() {
            std::cout << getPrintString();
            // std::cout << getPointerString();
        }


        void checkAllDone() {
            for (uint64_t index=0; index<pendingAsyncIoVec_.size(); index++) {
                if (pendingAsyncIoVec_.at(index) != nullptr) {
                    throw std::runtime_error(folly::sformat("checkAllDone -> {}",getPrintString()));
                }
            }
        }


        // check if all the bytes of block request have been processed 
        // either a hit or miss 
        void checkIfRequestProcessed() {
            // std::lock_guard<std::mutex> l(updateMutex_);
            std::lock_guard<std::mutex> l(updateMutex_);
            if (ioProcessed_ == getTotalIo()) {
                requestProcessed_ = true;
                if (tracker_ != nullptr) {
                    delete tracker_; 
                    tracker_ = nullptr;
                }
            } else if (ioProcessed_ > getTotalIo()) {
                throw std::runtime_error(folly::sformat("checkIfRequestProcessed -> {}",getPrintString()));
            }
        }


        // initiate an AsyncIORequest on a cache miss 
        AsyncIORequest* miss(uint64_t size, uint64_t alignment) {
            std::lock_guard<std::mutex> l(updateMutex_);
            missCount_++;
            uint64_t index = pendingAsyncIoVec_.size();
            for (uint64_t i=0; i<pendingAsyncIoVec_.size(); i++) {
                if (pendingAsyncIoVec_.at(i) == nullptr) {
                    pendingAsyncIoVec_.at(i) = new AsyncIORequest(size, alignment);
                    index = i;
                    missBytes_ += size;
                    break;
                }
            }

            if (index == pendingAsyncIoVec_.size())
                throw std::runtime_error(
                    folly::sformat("Index: {} Size: {} \n", index, pendingAsyncIoVec_.size()));

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
        void hit(uint64_t size) {
            std::lock_guard<std::mutex> l(updateMutex_);
            ioProcessed_ += size; 
            hitBytes_ += size;
            // checkIfRequestProcessed();
        }


    // properties of the block request 
    OpType op_= OpType::kGet;
    uint64_t offset_;
    uint64_t size_;
    uint64_t pageSize_;
    uint64_t startPage_;
    uint64_t endPage_;
    uint64_t alignment_;
    uint64_t missCount_=0;

    // tracking how much of the IO has been processed 
    // either marked as a hit or request to backing store completed on miss 
    uint64_t ioProcessed_ = 0;
    uint64_t ioReturned_ = 0;
    uint64_t hitBytes_ = 0;
    uint64_t missBytes_ = 0;
    bool requestProcessed_ = false;
    std::vector<AsyncIORequest*> pendingAsyncIoVec_;
    std::vector<iocb*> completedIOCBPtr_;
    std::vector<uint64_t> missKeyVec_;

    // track the latency between starting tracking and the request being processed 
    facebook::cachelib::util::LatencyTracker *tracker_ = nullptr;

    // mutex to control access to pendingAsyncIoVec_ 
    std::mutex updateMutex_;



};

}
}
}