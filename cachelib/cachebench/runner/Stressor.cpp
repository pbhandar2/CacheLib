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

#include "cachelib/cachebench/runner/Stressor.h"

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/cachebench/runner/CacheStressor.h"
#include "cachelib/cachebench/runner/BlockCacheStressor.h"
#include "cachelib/cachebench/runner/FastShutdown.h"
#include "cachelib/cachebench/runner/IntegrationStressor.h"
#include "cachelib/cachebench/workload/OnlineGenerator.h"
#include "cachelib/cachebench/workload/PieceWiseReplayGenerator.h"
#include "cachelib/cachebench/workload/ReplayGenerator.h"
#include "cachelib/cachebench/workload/BlockTraceReplay.h"
#include "cachelib/cachebench/workload/WorkloadGenerator.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
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
  ops += other.ops;

  return *this;
}

BlockReplayStats& BlockReplayStats::operator+=(const BlockReplayStats& other) {

  // workload stats   
  readReqCount += other.readReqCount;
  writeReqCount += other.writeReqCount;

  readReqBytes += other.readReqBytes;
  writeReqBytes += other.writeReqBytes;

  // system stats 
  experimentRuntime = std::max(other.experimentRuntime, experimentRuntime);
  replayRuntime = std::max(other.replayRuntime, replayRuntime); 

  maxPendingReq = std::max(other.maxPendingReq, maxPendingReq);
  maxPendingIO = std::max(other.maxPendingIO, maxPendingIO);
  loadPageFailure += other.loadPageFailure;

  maxInputQueueSize = std::max(other.maxInputQueueSize, maxInputQueueSize);
  maxOutputQueueSize = std::max(other.maxOutputQueueSize, maxOutputQueueSize);

  readBlockReqProcessed += other.readBlockReqProcessed;
  writeBlockReqProcessed += other.writeBlockReqProcessed;
  readBlockIOProcessed += other.readBlockIOProcessed;
  writeBlockIOProcessed += other.writeBlockIOProcessed;

  backingReadReqCount += other.backingReadReqCount; 
  backingWriteReqCount += other.backingWriteReqCount;
  backingReadIORequested += other.backingReadIORequested;
  backingWriteIORequested += other.backingWriteIORequested;

  backingReadReqProcessed += other.backingReadReqProcessed;
  backingWriteReqProcessed += other.backingWriteReqProcessed;
  backingReadIOProcessed += other.backingReadIOProcessed;
  backingWriteIOProcessed += other.backingWriteIOProcessed;

  return *this;
}

void BlockReplayStats::render(uint64_t elapsedTimeNs, std::ostream& out) const {

  auto getPercent = [](uint64_t count, uint64_t total) {
    return (total==0) ? 0.0 : static_cast<double>((100*count))/static_cast<double>(total);
  };

  // runtime stats 
  const double elapsedSecs = elapsedTimeNs / static_cast<double>(1e9);
  const double replayElapsedSecs = replayRuntime / static_cast<double>(1e9);
  const double experimentRuntimeSecs = experimentRuntime / static_cast<double>(1e9);
  std::cout << "\n\n";
  out << folly::sformat("totalTime_s={}\n", elapsedSecs);
  out << folly::sformat("replayTime_s={}\n", replayElapsedSecs);
  out << folly::sformat("experimentTime_s={}\n", experimentRuntimeSecs);

  // performance stats 
  auto readBandwidth = (experimentRuntimeSecs==0.0) ? 0.0 : readReqBytes/experimentRuntimeSecs;
  auto writeBandwidth = (experimentRuntimeSecs==0.0) ? 0.0 : writeReqBytes/experimentRuntimeSecs;
  auto bandwidth = (experimentRuntimeSecs==0.0) ? 0.0 : (readReqBytes+writeReqBytes)/experimentRuntimeSecs;

  out << folly::sformat("bandwidth_byte/s={:6.2f}\n", bandwidth);
  out << folly::sformat("readBandwidth_byte/s={:6.2f}\n", readBandwidth);
  out << folly::sformat("writeBandwidth_byte/s={:6.2f}\n", writeBandwidth);
  out << folly::sformat("maxBlockRequestQueue={}\n", maxInputQueueSize);
  out << folly::sformat("maxAsyncIOCompletionQueue={}\n", maxOutputQueueSize);
  out << folly::sformat("maxPendingIO={}\n", maxPendingIO);
  out << folly::sformat("loadPageFailureCount={}\n", loadPageFailure);

  const double blockRequestSuccess = getPercent(readBlockReqProcessed+writeBlockReqProcessed, readReqCount+writeReqCount);
  out << folly::sformat("blockReqCount={}\n", readReqCount+writeReqCount);
  out << folly::sformat("blockReadReqCount={}\n", readReqCount);
  out << folly::sformat("blockWriteReqCount={}\n", writeReqCount);
  out << folly::sformat("blockRequestSuccess={:3.2f} \n", blockRequestSuccess);
  out << folly::sformat("blockIORequested_byte={} \n", readReqBytes+writeReqBytes);
  out << folly::sformat("blockIOProcessed_byte={} \n", readBlockIOProcessed+writeBlockIOProcessed);
  
  out << folly::sformat("backingReqCount={}\n", backingReadReqCount+backingWriteReqCount);
  out << folly::sformat("backingWriteReqCount={}\n", backingWriteReqCount);
  out << folly::sformat("backingIORequested_byte={}\n", backingReadIORequested+backingWriteIORequested);
  out << folly::sformat("backingIOProcessed_byte={} \n", backingReadIOProcessed+backingWriteIOProcessed);
  out << folly::sformat("backingWriteIORequested_byte={}\n", backingWriteIORequested);

}


void BlockReplayStats::renderPercentile(std::ostream& out, folly::StringPiece describe, folly::StringPiece unit, util::PercentileStats *stats) const {
    auto printLatencies =
        [&out](folly::StringPiece cat, folly::StringPiece unit,
                const util::PercentileStats::Estimates& latency) {
        auto fmtLatency = [&out, &cat, &unit](folly::StringPiece pct, uint64_t val) {
            out << folly::sformat("{}_{}_{}={}\n", cat, pct, unit, val);
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
  printLatencies(describe, unit, est);

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
    return std::make_unique<ReplayGenerator>(config);
  } else if (config.generator == "multi-replay") {
    return std::make_unique<BlockTraceReplay>(config);
  } else if (config.generator.empty() || config.generator == "workload") {
    // TODO: Remove the empty() check once we label workload-based configs
    // properly
    return std::make_unique<WorkloadGenerator>(config);
  } else if (config.generator == "online") {
    return std::make_unique<OnlineGenerator>(config);

  } else {
    throw std::invalid_argument("Invalid config");
  }
}
} // namespace

std::unique_ptr<Stressor> Stressor::makeStressor(
    const CacheConfig& cacheConfig, const StressorConfig& stressorConfig) {
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

std::unique_ptr<BlockCacheStressorBase> BlockCacheStressorBase::makeBlockCacheStressor(
    const CacheConfig& cacheConfig, const StressorConfig& stressorConfig) {
    auto generator = makeGenerator(stressorConfig);
    return std::make_unique<BlockCacheStressor<LruAllocator>>(cacheConfig, stressorConfig, std::move(generator));
}


} // namespace cachebench
} // namespace cachelib
} // namespace facebook
