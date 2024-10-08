/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

#include <folly/Benchmark.h>

#include <atomic>
#include <memory>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Config.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

struct BlockReplayStats {

  uint64_t blockReqCount{0};
  uint64_t readBlockReqCount{0};
  uint64_t writeBlockReqCount{0};
  uint64_t readBlockReqByte{0};
  uint64_t writeBlockReqByte{0};

  uint64_t readHitByte{0};
  uint64_t readMisalignByte{0};
  uint64_t writeMisalignByte{0};
  uint64_t readCacheReqCount{0};
  uint64_t readHitCount{0};

  uint64_t backingReqCount{0};
  uint64_t readBackingReqCount{0};
  uint64_t writeBackingReqCount{0};
  uint64_t backingReqByte{0};
  uint64_t readBackingReqByte{0};
  uint64_t writeBackingReqByte{0};

  uint64_t replayTimeNs{0};
  uint64_t timeElapsedNs{0};

  uint64_t blockReqAddAttempt{0};
  uint64_t blockReqAddFailure{0};

  uint64_t backingReqAddAttempt{0};
  uint64_t backingReqAddFailure{0};
  uint64_t blockReqAddNoRetryCount{0};

  // NOTE: this can overflow, not sure what to do 
  // Back of the envelop calculation it can support, 1 bil request with 9 second latency, then it will overflow 
  uint64_t readLatencyNsTotal{0};
  uint64_t writeLatencyNsTotal{0};

  uint64_t backingReadLatencyNsTotal{0};
  uint64_t backingWriteLatencyNsTotal{0};
  uint64_t physicalIatNsTotal{0};

  util::PercentileStats *backingReadSizeBytePercentile = new util::PercentileStats();
  util::PercentileStats *backingWriteSizeBytePercentile = new util::PercentileStats();

  util::PercentileStats *backingReadLatencyNsPercentile = new util::PercentileStats();
  util::PercentileStats *backingWriteLatencyNsPercentile = new util::PercentileStats();

  util::PercentileStats *blockReadSizeBytePercentile = new util::PercentileStats();
  util::PercentileStats *blockWriteSizeBytePercentile = new util::PercentileStats();

  util::PercentileStats *readLatencyNsPercentile = new util::PercentileStats();
  util::PercentileStats *writeLatencyNsPercentile = new util::PercentileStats();

  util::PercentileStats *physicalIatNsPercentile = new util::PercentileStats();
  util::PercentileStats *percentIatErrorPstat = new util::PercentileStats();

  void render(std::ostream& out, folly::StringPiece delimiter, bool renderPercentileFlag) const;
  void renderPercentile(std::ostream& out, 
                        folly::StringPiece describe, 
                        folly::StringPiece unit, 
                        folly::StringPiece delimiter, 
                        util::PercentileStats *stats) const;

};

// Stats to track the throughput of the stress run. This is updated on every
// stressor thread and aggregated once the stress test finishes.
struct ThroughputStats {
  uint64_t set{0}; // number of set operations
  uint64_t setFailure{0};
  uint64_t get{0};
  uint64_t getMiss{0};
  uint64_t del{0};
  uint64_t update{0};      // number of in-place updates
  uint64_t updateMiss{0};  // number of in-place updates with key missing
  uint64_t delNotFound{0}; // deletes for non-existent key
  uint64_t addChained{0};
  uint64_t addChainedFailure{0};
  uint64_t couldExistOp{0};
  uint64_t couldExistOpFalse{0};
  // current number of ops executed. Read periodically to track progress
  uint64_t ops{0};

  // operator overload to aggregate multiple instances of ThroughputStats, one
  // from each  thread
  ThroughputStats& operator+=(const ThroughputStats& other);

  // convenience method to print the final throughput and hit ratio to stdout.
  void render(uint64_t elapsedTimeNs, std::ostream& out) const;

  // convenience method to fetch throughput information as counters.
  void render(uint64_t, folly::UserCounters&) const;
};

// forward declaration for the workload generator.
class GeneratorBase;

// Skeleton interface for a workload stressor. All stressors implement this
// interface.
class BlockSystemStressor {
  public:
  // create a stressor according to the passed in config and return through an
  // opaque base class instance.
  static std::unique_ptr<BlockSystemStressor> makeBlockSystemStressor(
        const CacheConfig& cacheConfig, const StressorConfig& stressorConfig);

  virtual ~BlockSystemStressor() {}

  virtual Stats getCacheStats() const = 0;

  virtual util::PercentileStats* getQueueSizePercentile() const = 0;
  virtual util::PercentileStats* getPendingBlockReqPercentile() const = 0;
  
  // start the stress run.
  virtual void start() = 0;

  // wait until the stress run finishes
  virtual void finish() = 0;

  virtual void statSnapshot(uint64_t threadId, std::ostream& ofs, std::string separator, bool renderPStats) = 0;

  virtual BlockReplayStats getStat(uint64_t threadId) const = 0;

  // Called when stop request from user is captured. instead of stop the load
  // test immediately, the method sets the state "stopped_" to true. Actual
  // stop logic is in somewhere else.
  void stopTest() { stopped_.store(true, std::memory_order_release); }

 private:
  // status that indicates if the runner has indicated the stress test to be
  // stopped before completion.
  std::atomic<bool> stopped_{false};
};

// Skeleton interface for a workload stressor. All stressors implement this
// interface.
class Stressor {
 public:
  // create a stressor according to the passed in config and return through an
  // opaque base class instance.
  static std::unique_ptr<Stressor> makeStressor(
      const CacheConfig& cacheConfig, const StressorConfig& stressorConfig);

  virtual ~Stressor() {}

  // report the stats from the cache  while the stress test is being run.
  virtual Stats getCacheStats() const = 0;

  // aggregate the throughput related stats at any given point in time.
  virtual ThroughputStats aggregateThroughputStats() const = 0;

  // ouputs workload generator specific stats to either an output stream or to
  // an output counter map
  virtual void renderWorkloadGeneratorStats(uint64_t /*elapsedTimeNs*/,
                                            std::ostream& /*out*/) const {}
  virtual void renderWorkloadGeneratorStats(
      uint64_t /*elapsedTimeNs*/, folly::UserCounters& /*counters*/) const {}

  // get the duration the test has run so far. If the test is finished, this
  // is not expected to change.
  virtual uint64_t getTestDurationNs() const = 0;

  // start the stress run.
  virtual void start() = 0;

  // wait until the stress run finishes
  virtual void finish() = 0;

  // abort the run
  virtual void abort() { stopTest(); }

  virtual bool isBlockReplay() { return blockReplay_; };

  virtual void setBlockReplay() { blockReplay_ = true; };

 protected:
  // check whether the load test should stop. e.g. user interrupt the
  // cachebench.
  bool shouldTestStop() { return stopped_.load(std::memory_order_acquire); }

  bool blockReplay_ = false;

  // Called when stop request from user is captured. instead of stop the load
  // test immediately, the method sets the state "stopped_" to true. Actual
  // stop logic is in somewhere else.
  void stopTest() { stopped_.store(true, std::memory_order_release); }

 private:
  // status that indicates if the runner has indicated the stress test to be
  // stopped before completion.
  std::atomic<bool> stopped_{false};
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
