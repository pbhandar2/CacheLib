#pragma once

#include "cachelib/cachebench/util/Config.h"


namespace facebook {
namespace cachelib {
namespace cachebench {

class PendingIo {
    public:
        PendingIo(){}

        void load(uint64_t offset, uint64_t size, bool writeFlag, uint64_t blockRequestIndex, uint64_t pendingIoIndex) {
            offset_ = offset;
            size_ = size;
            writeFlag = writeFlag;
            blockRequestIndex_ = blockRequestIndex;
            pendingIoIndex_ = pendingIoIndex;
        }

        bool isLoaded() {
            return size_ > 0; 
        }

        void reset() {
            size_ = 0; 
        }
    
    private:
        uint64_t offset_;
        uint64_t size_ = 0;
        bool writeFlag;
        uint64_t blockRequestIndex_;
        uint64_t pendingIoIndex_;
};


class BackingStore {
    public:
        BackingStore(StressorConfig config)
                : config_(config) {}
        
        void read() {}

        void write() {}
    
    private:
        const StressorConfig config_;

        uint64_t pendingIoCount_ = 0;
        uint64_t maxPendingBackingStoreIoCount_;

        std::vector<PendingIo> pendingIoVec_;

        std::queue<uint64_t> completedIoIndexQueue_;
};

}
}
}