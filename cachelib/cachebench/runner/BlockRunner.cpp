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
#include "cachelib/cachebench/runner/Stressor.h"
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

    auto cacheStats = stressor_->getCacheStats();

    // render block replay statistics 
    replayStats.render(durationNs, std::cout);

    replayStats.renderPercentile(std::cout, 
                                    "Block Read sLat (ns)", 
                                    stressor_->getTotalBlockReadLatencyPercentile());

    replayStats.renderPercentile(std::cout, 
                                    "Block Write sLat (ns)", 
                                    stressor_->getTotalBlockWriteLatencyPercentile());

    // replayStats.renderPercentile(std::cout, 
    //                                 "Backing Store Write Latency Percentile", 
    //                                 stressor_->getBackingStoreWriteLatencyPercentile());

    // replayStats.renderPercentile(std::cout,
    //                                 "Read block request size",
    //                                 stressor_->getBlockReadSizePercentile());

    // replayStats.renderPercentile(std::cout,
    //                                 "Write block request size",
    //                                 stressor_->getBlockWriteSizePercentile());

    // replayStats.renderPercentile(std::cout,
    //                                 "Block read latency",
    //                                 stressor_->getBlockReadLatencyPercentile());

    // replayStats.renderPercentile(std::cout,
    //                                 "Block write latency",
    //                                 stressor_->getBlockWriteLatencyPercentile());

    // cacheStats.blockRender(std::cout);

    tracker.stop();
    stressor_.reset();
    return true; 
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
