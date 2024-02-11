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
    BlockReplayStats stats;
    uint64_t numThreads = stressorConfig_.numThreads;
    for (uint64_t threadId = 0; threadId < numThreads; threadId++) {
        std::string statFilePath = folly::sformat("{}/stat_{}.out", stressorConfig_.blockReplayConfig.statOutputDir, threadId);
        std::ofstream ofs;
        ofs.open(statFilePath, std::ofstream::out);
        stressor_->statSnapshot(threadId, ofs, "\n", true);
    }

    return true; 
}


}
}
}
