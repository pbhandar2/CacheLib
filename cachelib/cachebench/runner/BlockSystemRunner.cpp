#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/runner/BlockSystemRunner.h"

namespace facebook {
namespace cachelib {
namespace cachebench {


BlockSystemRunner::BlockSystemRunner(const CacheBenchConfig& config)
    : stressor_{BlockSystemStressor::makeBlockSystemStressor(config.getCacheConfig(),
                                        config.getStressorConfig())} {
}

bool BlockSystemRunner::run() {
    stressor_->start();
    stressor_->finish();

    BlockReplayStats stats = stressor_->getStat(0);
    stats.render(std::cout, "\n");

    return true; 
}

}
}
}