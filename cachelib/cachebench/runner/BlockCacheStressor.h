/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/Random.h>
#include <folly/TokenBucket.h>

#include <atomic>
#include <cstddef>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_set>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/cache/TimeStampTicker.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// Implementation of stressor that uses a workload generator to stress an
// instance of the cache.  All item's value in CacheStressor follows CacheValue
// schema, which contains a few integers for sanity checks use. So it is invalid
// to use item.getMemory and item.getSize APIs.
template <typename Allocator>
class BlockCacheStressor : public Stressor {
	public:
		using CacheT = Cache<Allocator>;
		using Key = typename CacheT::Key;
		using ItemHandle = typename CacheT::ItemHandle;

  // @param cacheConfig   the config to instantiate the cache instance
  // @param config        stress test config
  // @param generator     workload  generator
  BlockCacheStressor(CacheConfig cacheConfig,
                StressorConfig config,
                std::unique_ptr<GeneratorBase>&& generator)
      : config_(std::move(config)),
        throughputStats_(config_.numThreads),
        wg_(std::move(generator)),
        hardcodedString_(genHardcodedString()),
        endTime_{std::chrono::system_clock::time_point::max()} {
    std::cout << folly::sformat("Welcome to BlockCacheStressor! \n");
    cache_ = std::make_unique<CacheT>(cacheConfig);
    if (config_.opRatePerSec > 0) {
      rateLimiter_ = std::make_unique<folly::BasicTokenBucket<>>(
          config_.opRatePerSec, config_.opRatePerSec);
    }
  }

  ~BlockCacheStressor() override { finish(); }

  // Start the stress test by spawning the worker threads and waiting for them
  // to finish the stress operations.
  void start() override {
    {
      std::lock_guard<std::mutex> l(timeMutex_);
      startTime_ = std::chrono::system_clock::now();
    }
    std::cout << folly::sformat("Total {:.2f}M ops to be run",
                                config_.numThreads * config_.numOps / 1e6)
              << std::endl;

    stressWorker_ = std::thread([this] {
      std::vector<std::thread> workers;
      for (uint64_t i = 0; i < config_.numThreads; ++i) {
        workers.push_back(
            std::thread([this, throughputStats = &throughputStats_.at(i)]() {
              stressByBlockReplay(*throughputStats);
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

  // Block until all stress workers are finished.
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

  // obtain aggregated throughput stats for the stress run so far.
  ThroughputStats aggregateThroughputStats() const override {
    ThroughputStats res{};
    for (const auto& stats : throughputStats_) {
      res += stats;
    }

    return res;
  }

  void renderWorkloadGeneratorStats(uint64_t elapsedTimeNs,
                                    std::ostream& out) const override {
    wg_->renderStats(elapsedTimeNs, out);
  }

  void renderWorkloadGeneratorStats(
      uint64_t elapsedTimeNs, folly::UserCounters& counters) const override {
    wg_->renderStats(elapsedTimeNs, counters);
  }

  uint64_t getTestDurationNs() const override {
    std::lock_guard<std::mutex> l(timeMutex_);
    return std::chrono::nanoseconds{
        std::min(std::chrono::system_clock::now(), endTime_) - startTime_}
        .count();
  }

 private:
  static std::string genHardcodedString() {
    const std::string s = "The quick brown fox jumps over the lazy dog. ";
    std::string val;
    for (int i = 0; i < 4 * 1024 * 1024; i += s.size()) {
      val += s;
    }
    return val;
  }

  folly::SharedMutex& getLock(Key key) {
    auto bucket = MurmurHash2{}(key.data(), key.size()) % locks_.size();
    return locks_[bucket];
  }

  // TODO maintain state on whether key has chained allocs and use it to only
  // lock for keys with chained items.
  auto chainedItemAcquireSharedLock(Key key) {
    using Lock = std::shared_lock<folly::SharedMutex>;
    return lockEnabled_ ? Lock{getLock(key)} : Lock{};
  }
  auto chainedItemAcquireUniqueLock(Key key) {
    using Lock = std::unique_lock<folly::SharedMutex>;
    return lockEnabled_ ? Lock{getLock(key)} : Lock{};
  }

  // populate the input item handle according to the stress setup.
  void populateItem(ItemHandle& handle) {
    if (!config_.populateItem) {
      return;
    }
    XDCHECK(handle);
    XDCHECK_LE(cache_->getSize(handle), 4ULL * 1024 * 1024);
    if (cache_->consistencyCheckEnabled()) {
      cache_->setUint64ToItem(handle, folly::Random::rand64(rng));
    } else {
      cache_->setStringItem(handle, hardcodedString_);
    }
  }

	// Runs a number of operations on the cache allocator. The actual
	// operations and key/value used are determined by the workload generator
	// initialized.
	//
	// Throughput and Hit/Miss rates are tracked here as well
	//
	// @param stats       Throughput stats
	void stressByBlockReplay(ThroughputStats& stats) {
		std::cout << "Stressor=Block Cache Stressor, Workload Generator=BlockReplay \n";
	}

  // inserts key into the cache if the admission policy also indicates the
  // key is worthy to be cached.
  //
  // @param pid         pool id to insert the key
  // @param stats       reference to the stats structure.
  // @param key         the key to be inserted
  // @param size        size of the cache value
  // @param ttlSecs     ttl for the value
  // @param featureMap  feature map for admission policy decisions.
  OpResultType setKey(
      PoolId pid,
      ThroughputStats& stats,
      const std::string* key,
      size_t size,
      uint32_t ttlSecs,
      const std::unordered_map<std::string, std::string>& featureMap) {
    // check the admission policy first, and skip the set operation
    // if the policy returns false
    if (config_.admPolicy && !config_.admPolicy->accept(featureMap)) {
      return OpResultType::kSetSkip;
    }

    ++stats.set;
    auto it = cache_->allocate(pid, *key, size, ttlSecs);
    if (it == nullptr) {
      ++stats.setFailure;
      return OpResultType::kSetFailure;
    } else {
      populateItem(it);
      cache_->insertOrReplace(it);
      return OpResultType::kSetSuccess;
    }
  }

  // fetch a request from the workload generator for a particular pool
  // @param pid             the pool id chosen for the request.
  // @param gen             the thread local random number generator to be
  // fed
  //                        to the workload generator  for constructing the
  //                        request.
  // @param lastRequestId   optional information about the last request id
  // that
  //                        was given to this thread by the workload
  //                        generator. This is used to provide continuity by
  //                        some generator implementations.

  const Request& getReq(const PoolId& pid,
                        std::mt19937_64& gen,
                        std::optional<uint64_t>& lastRequestId) {
    while (true) {
      const Request& req(wg_->getReq(pid, gen, lastRequestId));
      if (config_.checkConsistency && cache_->isInvalidKey(req.key)) {
        continue;
      }
      return req;
    }
  }

  void limitRate() {
    if (!rateLimiter_) {
      return;
    }
    rateLimiter_->consumeWithBorrowAndWait(1);
  }

  const StressorConfig config_; // config for the stress run

  std::vector<ThroughputStats> throughputStats_; // thread local stats

  std::unique_ptr<GeneratorBase> wg_; // workload generator

  // locks when using chained item and moving.
  std::array<folly::SharedMutex, 1024> locks_;

  // if locking is enabled.
  std::atomic<bool> lockEnabled_{false};

  // memorize rng to improve random performance
  folly::ThreadLocalPRNG rng;

  // string used for generating random payloads
  const std::string hardcodedString_;

  std::unique_ptr<CacheT> cache_;

  // Ticker that syncs the time according to trace timestamp.
  std::shared_ptr<TimeStampTicker> ticker_;

  // main stressor thread
  std::thread stressWorker_;

  // mutex to protect reading the timestamps.
  mutable std::mutex timeMutex_;

  // start time for the stress test
  std::chrono::time_point<std::chrono::system_clock> startTime_;

  // time when benchmark finished. This is set once the benchmark finishes
  std::chrono::time_point<std::chrono::system_clock> endTime_;

  // Token bucket used to limit the operations per second.
  std::unique_ptr<folly::BasicTokenBucket<>> rateLimiter_;

  // Whether flash cache has been warmed up
  bool hasNvmCacheWarmedUp_{false};
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
