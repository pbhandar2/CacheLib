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
    std::cout << "WELCOME TO BLOCK RUNNER \n";
    stressor_->start();
    stressor_->finish();
    return true; 
}

}
}
}