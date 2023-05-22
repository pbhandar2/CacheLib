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

using namespace std::chrono_literals;


namespace facebook {
namespace cachelib {
namespace cachebench {

class PendingBlockRequest {
    public:
        PendingBlockRequest(){}
}


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
                backingStore_(config),
                stressorTerminateFlag_(config.numThreads, false),
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
            backgroundProcessThreads_ = std::thread([this] {
                
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
            if (backgroundProcessThreads_.joinable()) {
                
                backgroundProcessThreads_.join();
                
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
            // parms for getReq in workload generator 
            std::mt19937_64 gen(folly::Random::rand64());
            std::optional<uint64_t> lastRequestId = std::nullopt;
            while (true) {
                try {
                    const Request& req(wg_->getReq(fileIndex, gen, lastRequestId));

                    if (req.getOp() == OpType::kGet)
                        std::cout << folly::sformat("{}: Read-> TS: {}, LBA: {}, SIZE: {}\n", fileIndex, req.timestamp, req.key, *(req.sizeBegin));
                    else
                        std::cout << folly::sformat("{}: Write-> TS: {}, LBA: {}, SIZE: {}\n", fileIndex, req.timestamp, req.key, *(req.sizeBegin));
                    
                } catch (const cachebench::EndOfTrace& ex) {
                    break;
                }
            }
            wg_->markFinish();
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

        // tracking time elapsed at any point in time during runtime 
        mutable std::mutex timeMutex_;
        std::chrono::time_point<std::chrono::system_clock> startTime_;
        std::chrono::time_point<std::chrono::system_clock> endTime_;

        // vector of stats from each replay thread which can be collected 
        // separately and/or aggregated to generate global statistics 
        // TODO: implement a new stat class called BlockReplayStats
        std::vector<ThroughputStats> blockReplayStatVec_;

        std::vector<PendingBlockRequest> pendingBlockRequestVec_;
        std::queue<uint64_t> blockRequestIndexQueue_;
};


}
}
}