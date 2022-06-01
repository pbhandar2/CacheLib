#pragma once

#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"

constexpr int maxMultiReplayThreadCount = 16;

namespace facebook {
namespace cachelib {
namespace cachebench {

class MultiReplayGeneratorBase : public GeneratorBase {
  public:
    explicit MultiReplayGeneratorBase(const StressorConfig& config)
      : config_(config),
        streamList_(config.numThreads) {
      
      if (config.checkConsistency) {
        throw std::invalid_argument(folly::sformat(
            "Cannot replay traces with consistency checking enabled"));
      }

      if (config.numThreads > maxMultiReplayThreadCount) {
        throw std::invalid_argument(folly::sformat(
            "Too many threads, maximum number of threads is {}", maxMultiReplayThreadCount));
      }

      for (int i=0; i<config_.numThreads; i++) {
        bufferList_[i] = new char[kIfstreamBufferSize];
        streamList_.at(i).open(config_.replayGeneratorConfig.traceList.at(i));
        streamList_.at(i).rdbuf()->pubsetbuf(bufferList_[i], kIfstreamBufferSize);
      }      
    }

  virtual ~MultiReplayGeneratorBase() {
    for (int i=0; i<config_.numThreads; i++) {
      streamList_[i].close();
    }
  }

  const std::vector<std::string>& getAllKeys() const {
    throw std::logic_error("ReplayGenerator has no keys precomputed!");
  }

  protected:
    void resetTraceFileToBeginning(int fileIndex) {
      streamList_.at(fileIndex).clear();
      streamList_.at(fileIndex).seekg(0, std::ios::beg);
    }
    const StressorConfig config_;
    char *bufferList_[maxMultiReplayThreadCount];
    std::vector<std::ifstream> streamList_;

};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
