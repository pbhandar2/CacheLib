#pragma once

#include "cachelib/cachebench/util/Config.h"


namespace facebook {
namespace cachelib {
namespace cachebench {

class BlockTraceReplayGenerator {
    public:
        BlockTraceReplayGenerator(const StressorConfig& config)
                                    :   config_(config) {
            for (int traceIndex=0; traceIndex < config_.blockReplayConfig.traces.size(); traceIndex++) {
                streamVec_.at(traceIndex).open(config_.blockReplayConfig.traces.at(traceIndex));
            }
        }

        ~BlockTraceReplayGenerator() {

        }

    StressorConfig config_;

    // vector of streams for each trace file 
    std::vector<std::ifstream> streamVec_;

    std::vector<BlockRequest>
    
};

}
}
}