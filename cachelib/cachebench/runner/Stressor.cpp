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

#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/cachebench/runner/AsyncCacheStressor.h"
#include "cachelib/cachebench/runner/BatchBlockStressor.h"
#include "cachelib/cachebench/runner/CacheStressor.h"
#include "cachelib/cachebench/runner/FastShutdown.h"
#include "cachelib/cachebench/runner/IntegrationStressor.h"
#include "cachelib/cachebench/workload/KVReplayGenerator.h"
#include "cachelib/cachebench/workload/OnlineGenerator.h"
#include "cachelib/cachebench/workload/PieceWiseReplayGenerator.h"
#include "cachelib/cachebench/workload/WorkloadGenerator.h"
#include "cachelib/common/Utils.h"
#include "cachelib/cachebench/workload/BlockReplayGenerator.h"


namespace facebook {
namespace cachelib {
namespace cachebench {


void BlockReplayStats::render(std::ostream& out, folly::StringPiece delimiter) const {
  auto getPercent = [](uint64_t count, uint64_t total) {
    return (total==0) ? 0.0 : static_cast<double>((100*count))/static_cast<double>(total);
  };

  out << folly::sformat("timeElapsed_sec={}{}", timeElapsedNs/1e9, delimiter);
  out << folly::sformat("replayTime_sec={}{}", replayTimeNs/1e9, delimiter);
  // block request count 
  out << folly::sformat("blockReqCount={}{}", blockReqCount, delimiter);
  out << folly::sformat("readBlockReqCount={}{}", readBlockReqCount, delimiter);
  out << folly::sformat("writeBlockReqCount={}{}", writeBlockReqCount, delimiter);
  // block request byte 
  out << folly::sformat("totalBlockReqByte={}{}", readBlockReqByte+writeBlockReqByte, delimiter);
  out << folly::sformat("readBlockReqByte={}{}", readBlockReqByte, delimiter);
  out << folly::sformat("writeBlockReqByte={}{}", writeBlockReqByte, delimiter);
  // misliagnment bytes 
  out << folly::sformat("readMisalignByte={}{}", readMisalignByte, delimiter);
  out << folly::sformat("writeMisalignByte={}{}", writeMisalignByte, delimiter);
  // read byte hit rate 
  out << folly::sformat("readHitByte={}{}", readHitByte, delimiter);
  out << folly::sformat("readByteHitRate={:.5f}{}", getPercent(readHitByte, readBlockReqByte+writeMisalignByte), delimiter);
  // block cache hit rate 
  out << folly::sformat("readCacheReqCount={}{}", readCacheReqCount, delimiter);
  out << folly::sformat("readCacheHitCount={}{}", readHitCount, delimiter);
  out << folly::sformat("readCacheHitRate={:.5f}{}", getPercent(readHitCount, readCacheReqCount), delimiter);
  // block req add attempt/failure 
  out << folly::sformat("blockReqAddAttempt={}{}", blockReqAddAttempt, delimiter);
  out << folly::sformat("blockReqAddFailure={}{}", blockReqAddFailure, delimiter);

  renderPercentile(out, "physicalIat", "ns", delimiter, physicalIatNsPercentile);
  renderPercentile(out, "blockReadLatency", "ns", delimiter, readLatencyNsPercentile);
  renderPercentile(out, "blockWriteLatency", "ns", delimiter, writeLatencyNsPercentile);

  // backing stats 
  out << folly::sformat("backingReqCount={}{}", backingReqCount, delimiter);
  out << folly::sformat("backingReqByte={}{}", backingReqByte, delimiter);
  out << folly::sformat("readBackingReqCount={}{}", readBackingReqCount, delimiter);
  out << folly::sformat("readBackingReqByte={}{}", readBackingReqByte, delimiter);
  out << folly::sformat("writeBackingReqCount={}{}", writeBackingReqCount, delimiter);
  out << folly::sformat("writeBackingReqByte={}{}", writeBackingReqByte, delimiter);
  out << folly::sformat("backingReqAddAttempt={}{}", backingReqAddAttempt, delimiter);
  out << folly::sformat("backingReqAddFailure={}{}", backingReqAddFailure, delimiter);

  // backing read/write latency 
  renderPercentile(out, "backingReadLatency", "ns", delimiter, backingReadLatencyNsPercentile);
  renderPercentile(out, "backingWriteLatency", "ns", delimiter, backingWriteLatencyNsPercentile);

  renderPercentile(out, "backingReadSize", "byte", delimiter, backingReadSizeBytePercentile);
  renderPercentile(out, "backingWriteSize", "byte", delimiter, backingWriteSizeBytePercentile);

}


void BlockReplayStats::renderPercentile(std::ostream& out, 
                                          folly::StringPiece describe, 
                                          folly::StringPiece unit, 
                                          folly::StringPiece delimiter, 
                                          util::PercentileStats *stats) const {
    auto printLatencies =
        [&out](folly::StringPiece cat, folly::StringPiece unit, folly::StringPiece delimiter,
                const util::PercentileStats::Estimates& latency) {
        auto fmtLatency = [&out, &cat, &unit, &delimiter](folly::StringPiece pct, uint64_t val) {
            out << folly::sformat("{}_{}_{}={}{}", cat, pct, unit, val, delimiter);
        };

        fmtLatency("avg", latency.avg);
        fmtLatency("p0", latency.p0);
        fmtLatency("p5", latency.p5);
        fmtLatency("p10", latency.p10);
        fmtLatency("p25", latency.p25);
        fmtLatency("p50", latency.p50);
        fmtLatency("p75", latency.p75);
        fmtLatency("p90", latency.p90);
        fmtLatency("p95", latency.p95);
        fmtLatency("p99", latency.p99);
        fmtLatency("p999", latency.p999);
        fmtLatency("p9999", latency.p9999);
        fmtLatency("p99999", latency.p99999);
        fmtLatency("p999999", latency.p999999);
        fmtLatency("p100", latency.p100);
      };

  util::PercentileStats::Estimates est = stats->estimate();
  printLatencies(describe, unit, delimiter, est);

}


ThroughputStats& ThroughputStats::operator+=(const ThroughputStats& other) {
  set += other.set;
  setFailure += other.setFailure;
  get += other.get;
  getMiss += other.getMiss;
  del += other.del;
  update += other.update;
  updateMiss += other.updateMiss;
  delNotFound += other.delNotFound;
  addChained += other.addChained;
  addChainedFailure += other.addChainedFailure;
  couldExistOp += other.couldExistOp;
  couldExistOpFalse += other.couldExistOpFalse;
  ops += other.ops;

  return *this;
}

void ThroughputStats::render(uint64_t elapsedTimeNs, std::ostream& out) const {
  const double elapsedSecs = elapsedTimeNs / static_cast<double>(1e9);

  const uint64_t setPerSec = util::narrow_cast<uint64_t>(set / elapsedSecs);
  const double setSuccessRate =
      set == 0 ? 0.0 : 100.0 * (set - setFailure) / set;

  const uint64_t getPerSec = util::narrow_cast<uint64_t>(get / elapsedSecs);
  const double getSuccessRate = get == 0 ? 0.0 : 100.0 * (get - getMiss) / get;

  const uint64_t delPerSec = util::narrow_cast<uint64_t>(del / elapsedSecs);
  const double delSuccessRate =
      del == 0 ? 0.0 : 100.0 * (del - delNotFound) / del;

  const uint64_t updatePerSec =
      util::narrow_cast<uint64_t>(update / elapsedSecs);
  const double updateSuccessRate =
      update == 0 ? 0.0 : 100.0 * (update - updateMiss) / update;

  const uint64_t addChainedPerSec =
      util::narrow_cast<uint64_t>(addChained / elapsedSecs);
  const double addChainedSuccessRate =
      addChained == 0 ? 0.0
                      : 100.0 * (addChained - addChainedFailure) / addChained;

  const uint64_t couldExistPerSec =
      util::narrow_cast<uint64_t>(couldExistOp / elapsedSecs);
  const double couldExistSuccessRate =
      couldExistOp == 0
          ? 0.0
          : 100.0 * (couldExistOp - couldExistOpFalse) / couldExistOp;

  out << std::fixed;
  out << folly::sformat("{:10}: {:.2f} million", "Total Ops", ops / 1e6)
      << std::endl;
  out << folly::sformat("{:10}: {:,}", "Total sets", set) << std::endl;

  auto outFn = [&out](folly::StringPiece k1, uint64_t v1, folly::StringPiece k2,
                      double v2) {
    out << folly::sformat("{:10}: {:9,}/s, {:10}: {:6.2f}%", k1, v1, k2, v2)
        << std::endl;
  };
  outFn("get", getPerSec, "success", getSuccessRate);
  outFn("couldExist", couldExistPerSec, "success", couldExistSuccessRate);
  outFn("set", setPerSec, "success", setSuccessRate);
  outFn("del", delPerSec, "found", delSuccessRate);
  if (update > 0) {
    outFn("update", updatePerSec, "success", updateSuccessRate);
  }
  if (addChained > 0) {
    outFn("addChained", addChainedPerSec, "success", addChainedSuccessRate);
  }
}

void ThroughputStats::render(uint64_t elapsedTimeNs,
                             folly::UserCounters& counters) const {
  const double elapsedSecs = elapsedTimeNs / static_cast<double>(1e9);

  const uint64_t setPerSec = util::narrow_cast<uint64_t>(set / elapsedSecs);
  const double setSuccessRate =
      set == 0 ? 0.0 : 100.0 * (set - setFailure) / set;

  const uint64_t getPerSec = util::narrow_cast<uint64_t>(get / elapsedSecs);
  const double getSuccessRate = get == 0 ? 0.0 : 100.0 * (get - getMiss) / get;

  const uint64_t delPerSec = util::narrow_cast<uint64_t>(del / elapsedSecs);
  const double delSuccessRate =
      del == 0 ? 0.0 : 100.0 * (del - delNotFound) / del;

  const uint64_t addChainedPerSec =
      util::narrow_cast<uint64_t>(addChained / elapsedSecs);
  const double addChainedSuccessRate =
      addChained == 0 ? 0.0
                      : 100.0 * (addChained - addChainedFailure) / addChained;

  counters["ops"] = ops;
  counters["total_sets"] = set;
  counters["get_per_sec"] = getPerSec;
  counters["get_suc_rate"] = util::narrow_cast<uint64_t>(getSuccessRate);
  counters["set_per_sec"] = setPerSec;
  counters["set_suc_rate"] = util::narrow_cast<uint64_t>(setSuccessRate);
  counters["del_per_sec"] = delPerSec;
  counters["del_suc_rate"] = util::narrow_cast<uint64_t>(delSuccessRate);
  counters["addChain_per_sec"] = addChainedPerSec;
  counters["addChain_suc_rate"] =
      util::narrow_cast<uint64_t>(addChainedSuccessRate);
}

namespace {
std::unique_ptr<GeneratorBase> makeGenerator(const StressorConfig& config) {
  if (config.generator == "piecewise-replay") {
    return std::make_unique<PieceWiseReplayGenerator>(config);
  } else if (config.generator == "replay") {
    return std::make_unique<KVReplayGenerator>(config);
  } else if (config.generator.empty() || config.generator == "workload") {
    // TODO: Remove the empty() check once we label workload-based configs
    // properly
    return std::make_unique<WorkloadGenerator>(config);
  } else if (config.generator == "online") {
    return std::make_unique<OnlineGenerator>(config);
  } else if (config.generator == "block-replay") {
    return std::make_unique<BlockReplayGenerator>(config);
  } else {
    throw std::invalid_argument("Invalid config");
  }
}
} // namespace


std::unique_ptr<Stressor> Stressor::makeStressor(
    const CacheConfig& cacheConfig, const StressorConfig& stressorConfig) {
    
  // make the workload generator first 
  auto generator = makeGenerator(stressorConfig);

  if (stressorConfig.name == "high_refcount") {
    return std::make_unique<HighRefcountStressor>(cacheConfig,
                                                  stressorConfig.numOps);
  } else if (stressorConfig.name == "cachelib_map") {
    return std::make_unique<CachelibMapStressor>(cacheConfig,
                                                 stressorConfig.numOps);
  } else if (stressorConfig.name == "cachelib_range_map") {
    return std::make_unique<CachelibRangeMapStressor>(cacheConfig,
                                                      stressorConfig.numOps);
  } else if (stressorConfig.name == "fast_shutdown") {
    return std::make_unique<FastShutdownStressor>(cacheConfig,
                                                  stressorConfig.numOps);
  } else if (stressorConfig.name == "async") {
    if (stressorConfig.generator != "workload" &&
        !stressorConfig.generator.empty()) {
      // async model has not been tested with other generators
      throw std::invalid_argument(folly::sformat(
          "Async cache stressor only works with workload generator currently. "
          "generator: {}",
          stressorConfig.generator));
    }

    if (cacheConfig.allocator == "LRU") {
      // default allocator is LRU, other allocator types should be added here
      return std::make_unique<AsyncCacheStressor<LruAllocator>>(
          cacheConfig, stressorConfig, std::move(generator));
    } else if (cacheConfig.allocator == "LRU2Q") {
      return std::make_unique<AsyncCacheStressor<Lru2QAllocator>>(
          cacheConfig, stressorConfig, std::move(generator));
    }
  } else {
    auto generator = makeGenerator(stressorConfig);
    if (cacheConfig.allocator == "LRU") {
      // default allocator is LRU, other allocator types should be added here
      return std::make_unique<CacheStressor<LruAllocator>>(
          cacheConfig, stressorConfig, std::move(generator));
    } else if (cacheConfig.allocator == "LRU2Q") {
      return std::make_unique<CacheStressor<Lru2QAllocator>>(
          cacheConfig, stressorConfig, std::move(generator));
    }
  }
  throw std::invalid_argument("Invalid config");
}

std::unique_ptr<BlockSystemStressor> BlockSystemStressor::makeBlockSystemStressor(
    const CacheConfig& cacheConfig, const StressorConfig& stressorConfig) {
  // make the workload generator first 
  auto generator = makeGenerator(stressorConfig);
  if (cacheConfig.allocator == "LRU") {
    // default allocator is LRU, other allocator types should be added here
    return std::make_unique<BatchBlockStressor<LruAllocator>>(
        cacheConfig, stressorConfig, std::move(generator));
  } else if (cacheConfig.allocator == "LRU2Q") {
    return std::make_unique<BatchBlockStressor<Lru2QAllocator>>(
        cacheConfig, stressorConfig, std::move(generator));
  } else {
    throw std::runtime_error("Allocator can be LRU or LRU2Q \n");
  }
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
