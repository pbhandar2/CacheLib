#pragma once

#include <chrono>
#include <thread>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/BlockReplayGenerator.h"

using namespace std::chrono_literals;


namespace facebook {
namespace cachelib {
namespace cachebench {


// A block storage system stressor that uses an instance of CacheLib 
// as cache and libaio to perform async backing store IO 
// when needed. 
template <typename Allocator>
class BlockStorageSystemStressor : public Stressor {
	public:
		using CacheT = Cache<Allocator>;

        // @param cacheConfig   the config to instantiate the cache instance
        // @param config        stress test config
        // @param generator     workload  generator
        BlockStorageSystemStressor(CacheConfig cacheConfig,
                                        StressorConfig config,
                                        std::unique_ptr<GeneratorBase>&& generator)
            :   config_(std::move(config)),
                wg_(std::move(generator)),
                blockReplayStatVec_(config.numThreads),
                endTime_{std::chrono::system_clock::time_point::max()} {

            // setup the cache 
            cache_ = std::make_unique<CacheT>(cacheConfig);
        }

        ~BlockStorageSystemStressor() override { 
            finish(); 
        }

        // start stressing the block storage system 
        void start() override {
            {
                std::lock_guard<std::mutex> l(timeMutex_);
                startTime_ = std::chrono::system_clock::now();
            }
            stressWorker_ = std::thread([this] {
                std::vector<std::thread> workers;
                for (uint64_t i = 0; i < config_.numThreads; ++i) {
                    workers.push_back(std::thread([this, blockReplayStats = &blockReplayStatVec_.at(i), index=i]() {
                        stressByBlockReplay(*blockReplayStats, index);
                    }));
                }

                for (auto& worker : workers) {
                    worker.join();
                }
                {
                    std::lock_guard<std::mutex> l(timeMutex_);
                    endTime_ = std::chrono::system_clock::now();
                }
            });
        }

        // wait for worker threads to terminate and cleanup 
        void finish() override {
            if (stressWorker_.joinable()) {
                stressWorker_.join();
            }
            wg_->markShutdown();
            cache_->clearCache(config_.maxInvalidDestructorCount);
        }

        // abort the stress run by indicating to the workload generator and
        // delegating to the base class abort() to stop the test.
        void abort() override {
            wg_->markShutdown();
            Stressor::abort();
        }

        // obtain stats from the cache instance.
        Stats getCacheStats() const override { return cache_->getStats(); }

        // obtain aggregated block replay stats 
        ThroughputStats aggregateThroughputStats() const override {
            ThroughputStats res{};
            return res;
        }

        uint64_t getTestDurationNs() const override {
            std::lock_guard<std::mutex> l(timeMutex_);
            return std::chrono::nanoseconds{std::min(std::chrono::system_clock::now(), endTime_) - startTime_}
                    .count();
        }

    private:
        // Replays a block trace on a cache allocator. 
        //
        // @param stats       Block replay stats 
        // @param fileIndex   The index used to map to resources of the file being replayed 
        void stressByBlockReplay(ThroughputStats& stats, uint64_t fileIndex) {
            wg_->markFinish();
        }

        // worker threads doing replay, block request processing, async IO return processing, stat printing
        std::thread stressWorker_;

        std::unique_ptr<CacheT> cache_;
        const StressorConfig config_; 

        // NOTE: we have only tried this stressor with BlockReplayGenerator 
        std::unique_ptr<GeneratorBase> wg_; 

        // tracking time elapsed at any point in time during runtime 
        mutable std::mutex timeMutex_;
        std::chrono::time_point<std::chrono::system_clock> startTime_;
        std::chrono::time_point<std::chrono::system_clock> endTime_;

        // vector of stats from each replay thread which can be collected 
        // separately and/or aggregated to generate global statistics 
        // TODO: naming it block replay stats for now 
        std::vector<ThroughputStats> blockReplayStatVec_;
};


}
}
}