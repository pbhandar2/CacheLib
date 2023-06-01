#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/runner/BlockSystemRunner.h"

namespace facebook {
namespace cachelib {
namespace cachebench {


BlockSystemRunner::BlockSystemRunner(const CacheBenchConfig& config)
    : stressor_{BlockSystemStressor::makeBlockSystemStressor(config.getCacheConfig(),
                                        config.getStressorConfig())} {
    stressorConfig_ = config.getStressorConfig();
}

bool BlockSystemRunner::run() {
    stressor_->start();
    stressor_->finish();

    uint64_t numThreads = stressorConfig_.numThreads;
    for (uint64_t threadId = 0; threadId < stressorConfig_.numThreads; threadId++) {
        BlockReplayStats stats = stressor_->getStat(threadId);
        stats.render(std::cout, "\n");
    }

    return true; 
}

}
}
}