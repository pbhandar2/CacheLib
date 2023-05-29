#pragma once

#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/util/Config.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class BlockSystemRunner {
    public:
        // @param config                the configuration for the cachebench run. This
        //                              contains both the stressor configuration and
        //                              the cache configuration.
        BlockSystemRunner(const CacheBenchConfig& config);

        bool run();
    private:
        // instance of the stressor.
        std::unique_ptr<BlockSystemStressor> stressor_;
};

}
}
}