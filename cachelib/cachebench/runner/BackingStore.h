#pragma once

#include <libaio.h>
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/runner/tscns.h"


namespace facebook {
namespace cachelib {
namespace cachebench {


class BackingIo {
    public:
        BackingIo(){}

        void load(uint64_t physicalTs, uint64_t offset, uint64_t size, bool writeFlag, uint64_t blockRequestIndex) {
            physicalTs_ = physicalTs;
            offset_ = offset;
            size_ = size;
            writeFlag_ = writeFlag;
            blockRequestIndex_ = blockRequestIndex;
            iocbPtr_ = new iocb();
        }

        bool isLoaded() {
            return size_ > 0; 
        }

        uint64_t getSize() {
            return size_;
        }

        uint64_t getOffset() {
            return offset_;
        }

        uint64_t getPhysicalTs() {
            return physicalTs_;
        }
        
        bool getWriteFlag() {
            return writeFlag_;
        }

        uint64_t getBlockRequestIndex() {
            return blockRequestIndex_;
        }

        void reset() {
            size_ = 0; 
            delete iocbPtr_;
        }
    
    iocb *iocbPtr_;
    
    private:
        uint64_t physicalTs_;
        uint64_t offset_;
        uint64_t size_ = 0;
        bool writeFlag_;
        uint64_t blockRequestIndex_;
};


class BackingStore {
    public:
        BackingStore(StressorConfig config)
                :   config_(config),
                    bufferVec_(config_.blockReplayConfig.maxPendingBackingStoreIoCount),
                    maxPendingBackingStoreIoCount_(config_.blockReplayConfig.maxPendingBackingStoreIoCount),
                    backingIoVec_(config_.blockReplayConfig.maxPendingBackingStoreIoCount) {
            
            // open each file in the backing storage device 
            for (uint64_t index=0; index<config.blockReplayConfig.backingFiles.size(); index++) {
                backingFileHandleVec_.push_back(open(config_.blockReplayConfig.backingFiles.at(index).c_str(), O_DIRECT|O_RDWR, 0644));
            }

            // initiate an IO context to submit async IO requests 
            ctx_ = new io_context_t();
            int ret = io_setup(maxPendingBackingStoreIoCount_, ctx_);
            if (ret != 0) {
                throw std::runtime_error(
                    folly::sformat("Error in io_setup, return: {}\n", ret));
            }

            ioTrackerThreads_ = std::thread([this] {
                std::vector<std::thread> workers;
                workers.push_back(std::thread([this]() {
                    trackCompletedIo();
                }));
                for (auto& worker : workers) {
                    worker.join();
                }
            });
        }

        ~BackingStore(){
            if (ioTrackerThreads_.joinable()) {
                 ioTrackerThreads_.join();
            }
        }


        // map a IOCB pointer to the index of pening backing IO in the system 
        void addToIocbToBackingStoreIndexMap(std::pair<iocb*, uint64_t> pair) {
            std::lock_guard<std::mutex> l(iocbToBackingIoMapMutex_);
            iocbToBackingIoIndexMap_.insert(pair);
        }


        // submit a request to backing storage 
        void submitBackingStoreRequest(uint64_t physicalTs, uint64_t offset, uint64_t size, bool writeFlag, uint64_t blockRequestIndex, uint64_t threadId) {
            std::lock_guard<std::mutex> l(pendingBackingIoMutex_);
            uint64_t index = submitToBackingStore(physicalTs, offset, size, writeFlag, blockRequestIndex);
            while (index == maxPendingBackingStoreIoCount_)
                index = submitToBackingStore(physicalTs, offset, size, writeFlag, blockRequestIndex);

            int ret = posix_memalign((void **)&bufferVec_.at(index), config_.blockReplayConfig.lbaSizeByte, size);
            if (ret != 0) 
                throw std::runtime_error(folly::sformat("Error in posix_memalign, return: {}\n", ret));

            if (writeFlag)
                io_prep_pwrite(backingIoVec_.at(index).iocbPtr_,
                    backingFileHandleVec_.at(threadId),
                    (void*) bufferVec_.at(index),
                    backingIoVec_.at(index).getSize(),
                    backingIoVec_.at(index).getOffset());
            else
                io_prep_pread(backingIoVec_.at(index).iocbPtr_,
                    backingFileHandleVec_.at(threadId),
                    (void*) bufferVec_.at(index),
                    backingIoVec_.at(index).getSize(),
                    backingIoVec_.at(index).getOffset());

            addToIocbToBackingStoreIndexMap(std::pair<iocb*, uint64_t>(backingIoVec_.at(index).iocbPtr_, index));
            
            ret = io_submit(*ctx_, 1, &backingIoVec_.at(index).iocbPtr_);
            if (ret < 1) 
                throw std::runtime_error(folly::sformat("Error in function io_submit. Return={}\n", ret));
        }


        // pop from the queue of backing IO that have returned but not been processed 
        uint64_t popFromBackingIoReturnQueue() {
            std::lock_guard<std::mutex> l(completedIoMutex_);
            uint64_t index = maxPendingBackingStoreIoCount_;
            if (completedIoIndexQueue_.size() > 0) {
                index = completedIoIndexQueue_.front();
                completedIoIndexQueue_.pop();
            } 
            return index;
        }

        uint64_t getBackingReqAddAttempt() {
            std::lock_guard<std::mutex> l(pendingBackingIoMutex_);
            return backingReqAddAttempt_;
        }

        uint64_t getBackingReqAddFailure() {
            std::lock_guard<std::mutex> l(pendingBackingIoMutex_);
            return backingReqAddFailure_;
        }


        // Get the BackingIo at the specified index 
        const BackingIo getBackingIo(uint64_t index) {
            std::lock_guard<std::mutex> l(pendingBackingIoMutex_);
            return backingIoVec_.at(index);
        }


        // Mark a backing IO at a certain index as completed and reset the spot for reuse 
        void markCompleted(uint64_t index) {
            std::lock_guard<std::mutex> l(pendingBackingIoMutex_);
            backingIoVec_.at(index).reset();
            delete [] bufferVec_.at(index);
            pendingIoCount_--;
        }


        // Mark block trace replay as being done 
        // If there is no pending backing IO requests, threads can terminate 
        void setReplayDone() {
            replayDone_ = true;
        }
    

    private:
        // add a index of pending backing requests to queue 
        void addToCompletedIoIndexQueue(uint64_t index) {
            std::lock_guard<std::mutex> l(completedIoMutex_);
            completedIoIndexQueue_.push(index);
        }


        // thread to track async backing storage requests that return 
        void trackCompletedIo() {
            struct io_event* events = new io_event[maxPendingBackingStoreIoCount_];
            struct timespec timeout;
            timeout.tv_sec = 0;
            timeout.tv_nsec = 100; // 1ns
            while ((!replayDone_) || (pendingIoCount_ > 0)) {
                int ret = io_getevents(*ctx_, 1, maxPendingBackingStoreIoCount_, events, &timeout);
                if (ret <= 0)
                    continue;

                for (int eindex=0; eindex<ret; eindex++) {
                    iocb *retiocb = events[eindex].obj;

                    // property of the async IO that completed 
                    uint64_t size = retiocb->u.v.nr;
                    uint64_t offset = retiocb->u.v.offset;
                    short op = retiocb->aio_lio_opcode;

                    // the result of async IO 
                    uint64_t res = events[eindex].res;
                    if (size != res) 
                        throw std::runtime_error(folly::sformat("Size: {} Return {} Op {}\n", size, res, op));
                    
                    // find the async IO request key 
                    uint64_t backingIoIndex;
                    {
                        std::lock_guard<std::mutex> l(iocbToBackingIoMapMutex_);
                        std::map<iocb*, uint64_t>::iterator itr = iocbToBackingIoIndexMap_.find(retiocb);
                        if (itr != iocbToBackingIoIndexMap_.end()) {  
                            backingIoIndex = itr->second;
                            iocbToBackingIoIndexMap_.erase(itr);
                        } else 
                            throw std::runtime_error(folly::sformat("No mapping to index found for the IOCB pointer: {} \n", retiocb));
                    }
                    {
                        std::lock_guard<std::mutex> l(completedIoMutex_);
                        completedIoIndexQueue_.push(backingIoIndex);
                    }

                }
            }
        }


        // initiate a new backing storage request and add it to vector of pending backing storage request 
        uint64_t submitToBackingStore(uint64_t physicalTs, uint64_t offset, uint64_t size, bool writeFlag, uint64_t blockRequestIndex) {
            uint64_t submitIndex = maxPendingBackingStoreIoCount_;
            if (BackingIoCount_ < maxPendingBackingStoreIoCount_) {
                for (int index=0; index<maxPendingBackingStoreIoCount_; index++) {
                    if (!backingIoVec_.at(index).isLoaded()) {
                        backingIoVec_.at(index).load(physicalTs, offset, size, writeFlag, blockRequestIndex);
                        submitIndex = index; 
                        pendingIoCount_++;
                        break;
                    }
                }
            }

            backingReqAddAttempt_++;
            if (submitIndex == maxPendingBackingStoreIoCount_)
                backingReqAddFailure_++;
            
            return submitIndex;  
        }

        std::thread ioTrackerThreads_;
        const StressorConfig config_;
        io_context_t* ctx_;

        uint64_t BackingIoCount_ = 0;
        uint64_t maxPendingBackingStoreIoCount_;


        // data needed to submit requests to backing store 
        mutable std::mutex pendingBackingIoMutex_;
        std::vector<BackingIo> backingIoVec_;
        std::vector<char*> bufferVec_;
        std::vector<int> backingFileHandleVec_;
        uint64_t backingReqAddFailure_ = 0;
        uint64_t backingReqAddAttempt_ = 0;

        mutable std::mutex iocbToBackingIoMapMutex_;
        std::map<iocb*, uint64_t> iocbToBackingIoIndexMap_;

        uint64_t pendingIoCount_;
        bool replayDone_ = false;

        // data to track async IO to backing store that return 
        mutable std::mutex completedIoMutex_;
        std::queue<uint64_t> completedIoIndexQueue_;


};

}
}
}