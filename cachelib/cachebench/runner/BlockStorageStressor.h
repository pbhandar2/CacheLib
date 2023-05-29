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

// A block storage system stressor that uses an instance of CacheLib 
// as cache and libaio to perform async backing store IO 
// when needed. 
template <typename Allocator>
class BlockStorageStressor : public BlockSystemStressor {
	public:
		using CacheT = Cache<Allocator>;

        // @param cacheConfig   the config to instantiate the cache instance
        // @param config        stress test config
        // @param generator     workload  generator
        BlockStorageStressor(CacheConfig cacheConfig,
                                StressorConfig config,
                                std::unique_ptr<GeneratorBase>&& generator)
                                :   config_(config), 
                                    backingStore_(config),
                                    stressorTerminateFlag_(config.numThreads, false),
                                    statsVec_(config.numThreads),
                                    wg_(std::move(generator)) {
            // setup the cache 
            cache_ = std::make_unique<CacheT>(cacheConfig);
        }

        ~BlockStorageStressor() override { 
            finish(); 
        }

        void start() override {
            backgroundProcessThreads_ = std::thread([this] {
                std::vector<std::thread> workers;
                for (uint64_t i = 0; i < config_.numThreads; ++i) {
                    workers.push_back(std::thread([this, index=i]() {
                        stressByBlockReplay(index);
                    }));
                }
                for (auto& worker : workers) {
                    worker.join();
                }
            });
        }

        // wait for worker threads to terminate and cleanup 
        void finish() override {
            if (backgroundProcessThreads_.joinable()) 
                backgroundProcessThreads_.join();
                
            wg_->markShutdown();
            cache_->clearCache(config_.maxInvalidDestructorCount);
        }

        BlockReplayStats getReplayStats(uint64_t threadId) const override {
            return statsVec_.at(threadId);
        }

        
    private:
        bool isReplayDone() {
            return std::all_of(stressorTerminateFlag_.begin(), stressorTerminateFlag_.end(), [](bool b){ return b; });
        }

        // Replays a block trace on a Cachelib cache and files in backing store. 
        //
        // @param stats       Block replay stats 
        // @param fileIndex   The index used to map to resources of the file being replayed 
        void stressByBlockReplay(uint64_t threadId) {
            wg_->markFinish();
            stressorTerminateFlag_.at(threadId) = true;

            // notify backing store if all replay threads are done 
            if (isReplayDone()) 
                backingStore_.setReplayDone();
        }

        // worker threads doing replay, block request processing, async IO return processing, stat printing
        std::thread backgroundProcessThreads_;
        
        // class to interact with the backing store through files on backing store devices 
        BackingStore backingStore_;

        // cache created based on the cache config 
        std::unique_ptr<CacheT> cache_;

        // configuration of block storage system stressor 
        const StressorConfig config_; 

        // NOTE: we have only tried this stressor with BlockReplayGenerator 
        std::unique_ptr<GeneratorBase> wg_; 

        // flag indicating whether each replay thread has terminated 
        std::vector<bool>stressorTerminateFlag_;

        std::vector<BlockReplayStats> statsVec_;

};

}
}
}