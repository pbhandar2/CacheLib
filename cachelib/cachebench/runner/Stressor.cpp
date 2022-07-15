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
  // types of block request 
  blockReqCount += other.blockReqCount;
  readReqCount += other.readReqCount;
  writeReqCount += other.writeReqCount;

  // total IO size (bytes) of different types of request 
  reqBytes += other.reqBytes;
  readReqBytes += other.readReqBytes;
  writeReqBytes += other.writeReqBytes;

  // total pages accessed 
  readPageCount += other.readPageCount;
  writePageCount += other.writePageCount;

  // total IO processed by the cache system 
  totalIOProcessed += other.totalIOProcessed;

  // stats on request alignment 
  readMisalignmentCount += other.readMisalignmentCount;
  writeMisalignmentCount += other.writeMisalignmentCount;
  misalignmentBytes += other.misalignmentBytes;
  writeAlignedCount += other.writeAlignedCount;
  readAlignedCount += other.readAlignedCount;

  // read and write page hit 
  readPageHitCount += other.readPageHitCount;
  writePageHitCount += other.writePageHitCount;

  // read and write block request hit 
  readBlockHitCount += other.readBlockHitCount;
  readBlockPartialHitCount += other.readBlockPartialHitCount;
  readBlockMissCount += other.readBlockMissCount;
  writeMisalignmentHitCount += other.writeMisalignmentHitCount;
  
  // backing store request count 
  readBackingStoreReqCount += other.readBackingStoreReqCount;
  writeBackingStoreReqCount += other.writeBackingStoreReqCount;

  // backing store IO processed 
  totalBackingStoreIO += other.totalBackingStoreIO;
  totalReadBackingStoreIO += other.totalReadBackingStoreIO;
  totalWriteBackingStoreIO += other.totalWriteBackingStoreIO;

  readBackingStoreFailureCount += other.readBackingStoreFailureCount;
  writeBackingStoreFailureCount += other.writeBackingStoreFailureCount;

  loadCount += other.loadCount;
  loadPageFailure += other.loadPageFailure;

  // track if any block request or async IO were dropped 
  // due to no space in queue 
  readBlockRequestDropCount += other.readBlockRequestDropCount;
  readBlockRequestDropBytes += other.readBlockRequestDropBytes;
  writeBlockRequestDropCount += other.writeBlockRequestDropCount;
  writeBlockRequestDropBytes += other.writeBlockRequestDropBytes;

  backingStoreRequestDropCount += other.backingStoreRequestDropCount;

  return *this;
}

void BlockReplayStats::render(uint64_t elapsedTimeNs, std::ostream& out) const {
  const double elapsedSecs = elapsedTimeNs / static_cast<double>(1e9);
  out << folly::sformat("Block request count: {} \n", blockReqCount);
  out << folly::sformat("Total IO requested (bytes): {} \n", reqBytes);
  out << folly::sformat("Read block request count: {} \n", readReqCount);
  out << folly::sformat("Read block request drop count: {}\n", readBlockRequestDropCount);
  out << folly::sformat("Read block request total IO dropped (bytes): {}\n", readBlockRequestDropBytes);
  out << folly::sformat("Total read IO requested (bytes): {} \n", readReqBytes);
  out << folly::sformat("Write block request count: {} \n", writeReqCount);
  out << folly::sformat("Write block request dropped: {}\n", writeBlockRequestDropCount);
  out << folly::sformat("Write block request total IO dropped (bytes): {}\n", writeBlockRequestDropBytes);
  out << folly::sformat("Total write IO requested (bytes): {} \n", writeReqBytes);
  out << folly::sformat("Page read count: {} \n", readPageCount);
  out << folly::sformat("Page write Count: {} \n", writePageCount);
  out << folly::sformat("Total IO processed (bytes) : {} \n", totalIOProcessed);
  out << folly::sformat("Read aligned block request count: {}\n", readAlignedCount);
  out << folly::sformat("Write aligned block request count: {}\n", writeAlignedCount);
  out << folly::sformat("Read misalignment count: {} \n", readMisalignmentCount);
  out << folly::sformat("Write misalignment count: {} \n", writeMisalignmentCount);
  out << folly::sformat("Total misalignment IO processed (bytes): {} \n", misalignmentBytes);
  out << folly::sformat("Backing store read request count: {} \n", readBackingStoreReqCount);
  out << folly::sformat("Backing store write request count: {} \n", writeBackingStoreReqCount);
  out << folly::sformat("Backing store IO processed: {} \n", totalBackingStoreIO);
  out << folly::sformat("Backing store read IO processed: {} \n", totalReadBackingStoreIO);
  out << folly::sformat("Backing store write IO processed: {} \n", totalWriteBackingStoreIO);
  out << folly::sformat("Read page hit count: {} \n", readPageHitCount);
  out << folly::sformat("Read block request hit count: {} \n", readBlockHitCount);
  out << folly::sformat("Read block request miss count: {} \n", readBlockMissCount);
  out << folly::sformat("Read block request partial hit count: {} \n", readBlockPartialHitCount);
  out << folly::sformat("Write misalignment hit count: {} \n", writeMisalignmentHitCount);
  out << folly::sformat("Cache load page count: {} \n", loadCount);
  out << folly::sformat("Cache load page failure count: {} \n", loadPageFailure);

  const double readHitRatio = readPageHitCount/(double) readPageCount;
  if (readPageCount > 0)
    out << folly::sformat("Read page hit rate: {:6.2f}\n", readHitRatio);
  else
    out << folly::sformat("Read page hit rate: 0.0\n");

  auto readBandwidth = readReqBytes/elapsedSecs;
  auto writeBandwidth = writeReqBytes/elapsedSecs;
  auto bandwidth = (readReqBytes+writeReqBytes)/elapsedSecs;
  auto readIOPS = readPageCount/elapsedSecs;
  auto writeIOPS = writePageCount/elapsedSecs;
  auto overallIOPS = (readPageCount+writePageCount)/elapsedSecs;
  auto blockReqIOPS = blockReqCount/elapsedSecs;
  auto readBlockReqIOPS = readReqCount/elapsedSecs;
  auto writeBlockReqIOPS = writeReqCount/elapsedSecs;

  out << folly::sformat("Bandwidth (bytes/sec): {:6.2f}\n", bandwidth);
  out << folly::sformat("Read bandwidth (bytes/sec): {:6.2f}\n", readBandwidth);
  out << folly::sformat("Write bandwidth (bytes/sec): {:6.2f}\n", writeBandwidth);
  out << folly::sformat("Page IOPS: {:6.2f}\n", overallIOPS);
  out << folly::sformat("Read page IOPS: {:6.2f}\n", readIOPS);
  out << folly::sformat("Write page IOPS: {:6.2f}\n", writeIOPS);
  out << folly::sformat("Block request IOPS: {:6.2f}\n", blockReqIOPS);
  out << folly::sformat("Read block request IOPS: {:6.2f}\n", readBlockReqIOPS);
  out << folly::sformat("Write block request IOPS: {:6.2f}\n", writeBlockReqIOPS);
  out << folly::sformat("Runtime(sec): {} \n", elapsedSecs);
  
}


void BlockReplayStats::renderPercentile(std::ostream& out, folly::StringPiece describe, util::PercentileStats *stats) const {
  auto printLatencies =
      [&out](folly::StringPiece cat,
              const util::PercentileStats::Estimates& latency) {
        auto fmtLatency = [&out, &cat](folly::StringPiece pct,
                                        double val) {
          out << folly::sformat("{:20} {:8} : {:>10.2f} ns\n", cat, pct,
                                val);
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
  printLatencies(describe, est);

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
