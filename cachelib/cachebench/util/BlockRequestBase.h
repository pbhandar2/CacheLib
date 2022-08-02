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
                    facebook::cachelib::util::PercentileStats& stats) {

            lba_ = lba;
            size_ = size; 
            op_ = op;
            pageSize_ = pageSize; 
            lbaSize_ = lbaSize;
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


        ~BlockRequest(){
            delete sLatTracker_;
            if (cLatTracker_ != nullptr)
                delete cLatTracker_;
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

            std::string iocbStr;
            for (uint64_t i=0; i<iocbVec_.size(); i++) {
                iocbStr.append(folly::sformat("{}-", iocbVec_.at(i)));
            }
            out.append(folly::sformat("IOCB: {}\n", iocbStr));
        
            return out; 
        }


        bool isBlockRequestProcessed() {
            std::lock_guard<std::mutex> l(updateMutex_);
            bool doneFlag = false; 
            if (op_ == OpType::kGet) {
                uint64_t totalIOProcessed = hitBytes_+readAsyncBytes_;
                if (totalIOProcessed > totalIO_) {
                    // throw std::runtime_error(folly::sformat("Excess read -> {}\n", printStatus()));
                    std::cout<<folly::sformat("warning: Excess read -> {}\n", printStatus());
                    doneFlag = true;
                } else if (totalIOProcessed == totalIO_) {
                    doneFlag = true; 
                }
            } else if (op_ == OpType::kSet) {
                uint64_t totalIOProcessed = writeAsyncBytes_+readAsyncBytes_;
                if (totalIOProcessed == totalIO_) {
                    doneFlag = true; 
                } else if (totalIOProcessed > totalIO_) {
                    // throw std::runtime_error(folly::sformat("Excess write -> {}\n", printStatus()));
                    std::cout<<folly::sformat("warning: Excess write -> {}\n", printStatus());
                    doneFlag = true; 
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
            // async IO size cannot be larger than the IO of the whole request 
            assert(size<=totalIO_);
            if (writeFlag) {
                writeAsyncBytes_ += size;
                writeAsyncCount_++;
            } else {
                readAsyncBytes_ += size;
                readAsyncCount_++;
            }
        }
    

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
    OpType op_;
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

};


// class BlockRequest {
//     public:
//         BlockRequest(uint64_t lba, 
//                     uint64_t size, 
//                     OpType op, 
//                     uint64_t pageSize, 
//                     uint64_t alignment) {

//             lba_ = lba;
//             offset_ = lba*alignment; 
//             size_ = size; 
//             op_ = op; 
//             pageSize_ = pageSize; 
//             alignment_ = alignment;

//             startPage_ = offset_/pageSize_;
//             endPage_ = (offset_ + size_ - 1)/pageSize_; 

//             if (getFrontMisAlignment(alignment_) > 0) {
//                 frontMisAlignment_ = pageSize_;
//             }

//             if (getRearMisAlignment(alignment_) > 0) {
//                 if (endPage_ > startPage_) 
//                     rearMisAlignment_ = pageSize_;
//             }

//             // frontMisAlignment_ = getFrontMisAlignment(alignment_);
//             // rearMisAlignment_ = getRearMisAlignment(alignment_);

//             if (op_ == OpType::kGet)  {
//                 pendingAsyncIoVec_.resize(endPage_-startPage_+2);
//                 completedIOCBPtr_.resize(endPage_-startPage_+2); 
//                 totalIO_ = (endPage_-startPage_+1) * pageSize_;
//             }
//             else if (op_ == OpType::kSet) {
//                 pendingAsyncIoVec_.resize(endPage_-startPage_+4);
//                 completedIOCBPtr_.resize(endPage_-startPage_+4);
//                 totalIO_ = frontMisAlignment_ + rearMisAlignment_ + size_;
//             }
//             else
//                 throw std::runtime_error(folly::sformat("BlockRequestBase->OP not recognized \n"));
                
//             for (uint64_t i=0; i<pendingAsyncIoVec_.size(); i++){
//                 pendingAsyncIoVec_.at(i) = nullptr;
//                 completedIOCBPtr_.at(i) = nullptr;
//             }
                
//         }


//         ~BlockRequest() {
//             checkAllDone();
//             if (tracker_ != nullptr)
//                 delete tracker_;
//         }


//         uint64_t getStartPage() {
//             return startPage_;
//         }


//         uint64_t getEndPage() {
//             return endPage_;
//         }


//         uint64_t getLBA() {
//             return lba_;
//         }


//         // get the number of bytes by which the block request is not aligned in the front
//         // also make sure either you don't do IO and if you do, 
//         // it has to be larger than aligment size (e.g. 512 for HDD)
//         uint64_t getFrontMisAlignment(uint64_t backingStoreAlignment) {
//             uint64_t minOffset = startPage_*pageSize_;
//             if (offset_-minOffset == 0) {
//                 return 0;
//             } else {
//                  uint64_t raw = std::max(offset_-minOffset, backingStoreAlignment);
//                  return backingStoreAlignment*(raw/backingStoreAlignment);
//             }
//         }


//         // get the number of bytes by which the block request is not aligned in the rear
//         // also make sure either you don't do IO and if you do, 
//         // it has to be larger than aligment size (e.g. 512 for HDD)
//         uint64_t getRearMisAlignment(uint64_t backingStoreAlignment) {
//             uint64_t maxOffset = (endPage_+1)*pageSize_;
//             if (maxOffset-offset_-size_ == 0) {
//                 return 0;
//             } else {
//                 uint64_t raw = std::max(maxOffset-offset_-size_, backingStoreAlignment);
//                 return backingStoreAlignment*(raw/backingStoreAlignment);
//             }
//         }


//         // get the total IO to be done for the block request based on read/write 
//         uint64_t getTotalIo() {
//             uint64_t totalIO;
//             if (op_ == OpType::kGet) {
//                 totalIO =  (endPage_-startPage_+1)*pageSize_;
//             } else {
//                 totalIO = frontMisAlignment_+rearMisAlignment_+size_;
//             }
//             return totalIO;
//         }


//         uint64_t pageCount() {
//             return endPage_-startPage_+1;
//         }


//         uint64_t frontMisAlignment() {
//             return frontMisAlignment_;
//         }


//         uint64_t rearMisAlignment() {
//             return rearMisAlignment_;
//         }


//         uint64_t getSize() {
//             return size_;
//         }


//         uint64_t getOffset() {
//             return offset_;
//         }


//         uint64_t getPageSize() {
//             return pageSize_;
//         }


//         bool isRequestProcessed() {
//             return requestProcessed_;
//         }


//         OpType getOp() {
//             return op_;
//         }


//         // get an empty spot in the list of AsyncIORequest objects 
//         uint64_t getAsyncIoIndex(iocb *iocbPtr) {
//             uint64_t i = pendingAsyncIoVec_.size();
//             for (uint64_t index=0; index<pendingAsyncIoVec_.size(); index++) {
//                 if (pendingAsyncIoVec_.at(index) != nullptr) {
//                     if (pendingAsyncIoVec_.at(index)->iocbPtr == iocbPtr) {
//                         i = index;
//                         return index;
//                     }
//                 }
//             }
//             return i;
//         }


//         // on completion of async IO, this callback is called 
//         void ioCompleted(iocb *iocbPtrDone) {
//             std::lock_guard<std::mutex> l(updateMutex_);
//             uint64_t i = getAsyncIoIndex(iocbPtrDone);
//             if (i == pendingAsyncIoVec_.size()) {
//                 throw std::runtime_error(
//                     folly::sformat("No space for pending async IO. {} {} {} {} {} {} {} {} {} \n", 
//                                     i, 
//                                     ioProcessed_, 
//                                     ioReturned_, 
//                                     getTotalIo(), 
//                                     pendingAsyncIoVec_.at(0),
//                                     pageCount(),
//                                     missKeyVec_.size(),
//                                     hitBytes_,
//                                     missBytes_));
//             } 
//             ioProcessed_ += iocbPtrDone->u.v.nr;
//             ioReturned_++;
            
//             // std::cout << folly::sformat("Pending RESET at {} FROM: {} ", i, pendingAsyncIoVec_.at(i));
//             // std::cout << folly::sformat("Completed RESET at {} FROM: {}\n", i, completedIOCBPtr_.at(i));
//             delete iocbPtrDone;
//             pendingAsyncIoVec_.at(i) = nullptr;
//             completedIOCBPtr_.at(i) = iocbPtrDone;
//             // std::cout << folly::sformat("Pending RESET at {} TO: {}", i, pendingAsyncIoVec_.at(i));
//             // std::cout << folly::sformat("Completed RESET at {} TO: {} \n", i, completedIOCBPtr_.at(i));

//             // checkIfRequestProcessed();
//         }


//         void startLatencyTracking(facebook::cachelib::util::PercentileStats& stats) {
//             tracker_ = new facebook::cachelib::util::LatencyTracker(stats);
//         }


//         uint64_t currentPendingIOCount() {
//             uint64_t ioCount = 0;
//             for (uint64_t index=0; index<pendingAsyncIoVec_.size(); index++) {
//                 if (pendingAsyncIoVec_.at(index) != nullptr) {
//                     ioCount += 1;
//                 }
//             }
//             return ioCount;
//         }


//         uint64_t currentCompletedIOCount() {
//             uint64_t ioCount = 0;
//             for (uint64_t index=0; index<completedIOCBPtr_.size(); index++) {
//                 if (completedIOCBPtr_.at(index) != nullptr) {
//                     ioCount += 1;
//                 }
//             }
//             return ioCount;
//         }


//         std::string getPointerString() {
//             std::string out; 
//             out.append(folly::sformat("\nPending \n"));
//             for (uint64_t index=0; index<pendingAsyncIoVec_.size(); index++) {
//                 if (pendingAsyncIoVec_.at(index) != nullptr) {
//                     out.append(folly::sformat("{}-", pendingAsyncIoVec_.at(index)));
//                 }
//             }
//             out.append(folly::sformat("\n---------------------\n"));
//             out.append(folly::sformat("\nCompleted \n"));
//             for (uint64_t index=0; index<completedIOCBPtr_.size(); index++) {
//                 if (pendingAsyncIoVec_.at(index) != nullptr) {
//                     out.append(folly::sformat("{}-", pendingAsyncIoVec_.at(index)));
//                 }
//             }
//             return out; 
//         }


//         std::string getPrintString() {
//             std::string out;
//             out.append(folly::sformat("Total IO: {},", totalIO_));
//             out.append(folly::sformat("Size: {},", getSize()));
//             out.append(folly::sformat("Page Count: {},", pageCount()));
//             out.append(folly::sformat("Io Returned: {},", ioReturned_));
//             out.append(folly::sformat("Io Processed: {},", ioProcessed_));
//             out.append(folly::sformat("Async Io Count: {},", missKeyVec_.size()));
//             out.append(folly::sformat("Hit Bytes: {},", hitBytes_));
//             out.append(folly::sformat("Miss Bytes: {},", missBytes_));
//             out.append(folly::sformat("Miss Count: {},", missCount_));
//             out.append(folly::sformat("Offset: {},", offset_));
//             out.append(folly::sformat("Front align: {},", getFrontMisAlignment(alignment_)));
//             out.append(folly::sformat("Back align: {},", getRearMisAlignment(alignment_)));
//             out.append(folly::sformat("Async bytes: {},", asyncBytes_));

//             if (getOp() == OpType::kGet) {
//                 out.append(folly::sformat("Op: Read,"));
//             } else {
//                 out.append(folly::sformat("Op: Write,"));
//             }
            
//             std::string hitSize;
//             for (uint64_t i=0; i<hitVec_.size(); i++) {
//                 hitSize.append(folly::sformat("{}-", hitVec_.at(i)));
//             }

//             std::string asyncSize;
//             for (uint64_t i=0; i<asyncVec_.size(); i++) {
//                 asyncSize.append(folly::sformat("{}-", asyncVec_.at(i)));
//             }

//             out.append(folly::sformat("Hits: {},", hitSize));
//             out.append(folly::sformat("Async: {},", asyncSize));
//             out.append(folly::sformat("Pending Io Count: {},", currentPendingIOCount()));
//             out.append(folly::sformat("Completed Io Count: {}\n", currentCompletedIOCount()));
//             return out;
//         }


//         void printStatus() {
//             std::cout << getPrintString();
//             // std::cout << getPointerString();
//         }


//         void checkAllDone() {
//             for (uint64_t index=0; index<pendingAsyncIoVec_.size(); index++) {
//                 if (pendingAsyncIoVec_.at(index) != nullptr) {
//                     throw std::runtime_error(folly::sformat("checkAllDone -> {}",getPrintString()));
//                 }
//             }
//         }


//         // check if all the bytes of block request have been processed 
//         // either a hit or miss 
//         void checkIfRequestProcessed() {
//             // std::lock_guard<std::mutex> l(updateMutex_);
//             std::lock_guard<std::mutex> l(updateMutex_);
//             if (ioProcessed_ == getTotalIo()) {
//                 requestProcessed_ = true;
//                 if (tracker_ != nullptr) {
//                     delete tracker_; 
//                     tracker_ = nullptr;
//                 }
//             } else if (ioProcessed_ > getTotalIo()) {
//                 throw std::runtime_error(folly::sformat("checkIfRequestProcessed -> {}",getPrintString()));
//             }
//         }


//         // initiate an AsyncIORequest on a cache miss 
//         AsyncIORequest* miss(uint64_t size, uint64_t alignment) {
//             std::lock_guard<std::mutex> l(updateMutex_);
//             missCount_++;
//             uint64_t index = pendingAsyncIoVec_.size();
//             for (uint64_t i=0; i<pendingAsyncIoVec_.size(); i++) {
//                 if (pendingAsyncIoVec_.at(i) == nullptr) {
//                     pendingAsyncIoVec_.at(i) = new AsyncIORequest(size, alignment, 0);
//                     index = i;
//                     missBytes_ += size;
//                     break;
//                 }
//             }

//             if (index == pendingAsyncIoVec_.size())
//                 throw std::runtime_error(
//                     folly::sformat("Index: {} Size: {} \n", index, pendingAsyncIoVec_.size()));

//             return pendingAsyncIoVec_.at(index);
//         }


//         bool isBlockRequestProcessed() {
//             bool blockRequestProcessed = false; 
//             std::lock_guard<std::mutex> l(updateMutex_);
//             if (ioProcessed_ == totalIO_) {
//                 blockRequestProcessed = true;
//             } else if (ioProcessed_ > totalIO_) {
//                 throw std::runtime_error(folly::sformat("checkIfRequestProcessed -> {}",getPrintString()));
//             }
//             return blockRequestProcessed;
//         }


//         void addMissKey(uint64_t key) {
//             missKeyVec_.push_back(key);
//         }


//         bool checkKeyMiss(uint64_t key) {
//             if (std::find(missKeyVec_.begin(), missKeyVec_.end(), key) != missKeyVec_.end()) {
//                 return true;
//             } else {
//                 return false; 
//             }
//         }


//         // set specified bytes as a cache hit 
//         void hit(uint64_t size) {
//             std::lock_guard<std::mutex> l(updateMutex_);
//             ioProcessed_ += size; 
//             hitBytes_ += size;
//             hitVec_.push_back(size);
//             // checkIfRequestProcessed();
//         }

//         void asyncReturn(uint64_t size) {
//             std::lock_guard<std::mutex> l(updateMutex_);
//             ioProcessed_ += size; 
//             asyncBytes_ += size; 
//             ioReturned_++;
//             asyncVec_.push_back(size);
//         }

//     uint64_t lba_;


//     // properties of the block request 
//     OpType op_= OpType::kGet;
//     uint64_t offset_;
//     uint64_t size_;
//     uint64_t pageSize_;
//     uint64_t startPage_;
//     uint64_t endPage_;
//     uint64_t alignment_;
//     uint64_t missCount_=0;

//     uint64_t frontMisAlignment_;
//     uint64_t rearMisAlignment_;
//     uint64_t totalIO_;

//     // tracking how much of the IO has been processed 
//     // either marked as a hit or request to backing store completed on miss 
//     uint64_t ioProcessed_ = 0;
//     uint64_t ioReturned_ = 0;
//     uint64_t hitBytes_ = 0;
//     uint64_t missBytes_ = 0;
//     uint64_t asyncBytes_ = 0;
//     bool requestProcessed_ = false;
//     std::vector<AsyncIORequest*> pendingAsyncIoVec_;
//     std::vector<iocb*> completedIOCBPtr_;
//     std::vector<uint64_t> missKeyVec_;

//     // track the latency between starting tracking and the request being processed 
//     facebook::cachelib::util::LatencyTracker *tracker_ = nullptr;

//     // mutex to control access to pendingAsyncIoVec_ 
//     std::mutex updateMutex_;

//     std::vector<uint64_t> hitVec_;
//     std::vector<uint64_t> asyncVec_;

// };

}
}
}