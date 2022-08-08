#pragma once

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/MultiReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {


class BlockTraceReplay : public MultiReplayGeneratorBase {

  public:
    explicit BlockTraceReplay(const StressorConfig& config)
      : MultiReplayGeneratorBase(config),
        lbaVec_(config.numThreads, ""),
        opVec_(config.numThreads, OpType::kGet),
        sizeVec_(config.numThreads, config.pageSizeBytes),
        tsVec_(config.numThreads, 0),
        minLBA_(config.replayGeneratorConfig.minLBA) {
          for (int i=0; i<config.numThreads; i++) {
            requestVec_.push_back(Request(lbaVec_.at(i), sizeVec_.begin(), sizeVec_.end(), opVec_.at(i), tsVec_.at(i)));
          }
        }

  virtual ~BlockTraceReplay() {}

  const Request& getReq(
    uint8_t fileIndex,
    std::mt19937_64&,
    std::optional<uint64_t> lastRequestId = std::nullopt) override;

  private:
    uint64_t minLBA_;
    std::vector<std::string> lbaVec_;
    std::vector<OpType> opVec_;
    std::vector<size_t> sizeVec_;
    std::vector<uint64_t> tsVec_;
    std::vector<Request> requestVec_;

};


const Request& BlockTraceReplay::getReq(uint8_t fileIndex,
                                       std::mt19937_64&,
                                       std::optional<uint64_t>) {

    std::string row;
    if (!std::getline(streamList_.at(fileIndex), row)) {
        throw cachelib::cachebench::EndOfTrace("");
    } 

    std::stringstream row_stream(row);
    std::string token;

    // Timestamp 
    std::getline(row_stream, token, ',');
    uint64_t ts = std::stoul(token);
    tsVec_.at(fileIndex) = ts;
    requestVec_.at(fileIndex).setTs(ts);

    // LBA 
    std::getline(row_stream, token, ',');
    uint64_t lba = std::stoul(token) - minLBA_;
    lbaVec_.at(fileIndex) = std::to_string(lba);
    requestVec_.at(fileIndex).key = std::to_string(lba);

    // Operation (read/write)
    std::getline(row_stream, token, ',');
    if (token == "r") {
      opVec_.at(fileIndex) = OpType::kGet;
      requestVec_.at(fileIndex).setOp(OpType::kGet);
    } else if (token == "w") {
      opVec_.at(fileIndex) = OpType::kSet;
      requestVec_.at(fileIndex).setOp(OpType::kSet);
    } else {
      throw std::invalid_argument("Unrecognized Operation in the trace file!");
    }

    // Request size 
    std::getline(row_stream, token, ',');
    sizeVec_.at(fileIndex) = std::stoul(token);
    *(requestVec_.at(fileIndex).sizeBegin) = sizeVec_.at(fileIndex);

    return requestVec_.at(fileIndex);
    
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook