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
    
    std::cout << "runner:init,BlockRunner \n";

    BlockReplayProgressTracker tracker{*stressor_, progressStatsFile};
    stressor_->start();

    if (!tracker.start(progressInterval)) {
        throw std::runtime_error("Cannot start ProgressTracker.");
    }

    stressor_->finish();
    tracker.stop();
    // just to make sure the stat print thread and final output dont overlap 
    uint64_t durationNs = stressor_->getTestDurationNs();
    BlockReplayStats replayStats = stressor_->aggregateBlockReplayStats();
    auto cacheStats = stressor_->getCacheStats();

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // render block replay statistics 
    replayStats.render(durationNs, std::cout);
    replayStats.renderPercentile(std::cout, 
                                    "blockReadSlat", 
                                    "ns",
                                    stressor_->sLatBlockReadPercentile());

    replayStats.renderPercentile(std::cout, 
                                    "blockWriteSlat",
                                    "ns", 
                                    stressor_->sLatBlockWritePercentile());

    replayStats.renderPercentile(std::cout, 
                                    "blockReadClat", 
                                    "ns",
                                    stressor_->cLatBlockReadPercentile());

    replayStats.renderPercentile(std::cout, 
                                    "blockWriteClat",
                                    "ns", 
                                    stressor_->cLatBlockWritePercentile());

    replayStats.renderPercentile(std::cout, 
                                    "backingReadLat",
                                    "ns", 
                                    stressor_->latBackingReadPercentile());

    replayStats.renderPercentile(std::cout, 
                                    "backingWriteLat",
                                    "ns", 
                                    stressor_->latBackingWritePercentile());

    replayStats.renderPercentile(std::cout, 
                                    "backingReadSize",
                                    "byte", 
                                    stressor_->backingReadSizePercentile());

    replayStats.renderPercentile(std::cout, 
                                    "backingWriteSize",
                                    "byte", 
                                    stressor_->backingWriteSizePercentile());

    replayStats.renderPercentile(std::cout, 
                                    "iatWaitDuration",
                                    "us", 
                                    stressor_->iatWaitDurationPercentile());

    replayStats.renderPercentile(std::cout, 
                                    "loadDuration",
                                    "us", 
                                    stressor_->loadDurationPercentile());
    cacheStats.blockRender(std::cout);
    std::cout << "runner:terminate,BlockRunner \n";
    stressor_.reset();
    return true; 
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
