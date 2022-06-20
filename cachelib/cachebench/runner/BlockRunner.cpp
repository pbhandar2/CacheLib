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

#include "cachelib/cachebench/runner/BlockRunner.h"
#include "cachelib/cachebench/runner/BlockCacheStressor.h"

namespace facebook {
namespace cachelib {
namespace cachebench {


BlockRunner::BlockRunner(const CacheBenchConfig& config)
    : stressor_{BlockCacheStressorBase::makeBlockCacheStressor(config.getCacheConfig(),
                                       config.getStressorConfig())} {}


bool BlockRunner::run(std::chrono::seconds progressInterval,
                        const std::string& progressStatsFile) {
    
    std::cout << "Runner: BlockRunner \n";

    BlockReplayProgressTracker tracker{*stressor_, progressStatsFile};
    stressor_->start();

    if (!tracker.start(progressInterval)) {
        throw std::runtime_error("Cannot start ProgressTracker.");
    }

    stressor_->finish();
    uint64_t durationNs = stressor_->getTestDurationNs();
    BlockReplayStats replayStats = stressor_->aggregateBlockReplayStats();


    // std::cout << "== Test Results ==\n== Allocator Stats ==" << std::endl;
    auto cacheStats = stressor_->getCacheStats();
    cacheStats.render(std::cout);
    // replayStats.render(durationNs, std::cout);
    replayStats.renderPercentile(std::cout, "Backing Store Read Latency Percentile", stressor_->getBackingStoreReadLatencyStat());
    replayStats.renderPercentile(std::cout, "Backing Store Write Latency Percentile", stressor_->getBackingStoreWriteLatencyStat());
    tracker.stop();
    stressor_.reset();
    return true; 
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
