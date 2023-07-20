#pragma once

#include <fstream>

#include <folly/Format.h>
#include <folly/Random.h>

#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class BlockReplayGenerator : public GeneratorBase {
    public:
        explicit BlockReplayGenerator(const StressorConfig& config)
            :   config_(config),
                streamVec_(config_.blockReplayConfig.traces.size()),
                lineCountVec_(config_.blockReplayConfig.traces.size(), 0),
                lastTsVec_(config_.blockReplayConfig.traces.size(), 0),
                lastOpVec_(config_.blockReplayConfig.traces.size(), OpType::kGet),
                lastLbaVec_(config_.blockReplayConfig.traces.size(), std::string(" ")),
                lastSizeVec_(config_.blockReplayConfig.traces.size(), std::vector<size_t>{0}) {
            for (int traceIndex=0; traceIndex < config_.blockReplayConfig.traces.size(); traceIndex++) {
                lastRequestVec_.push_back(Request(lastLbaVec_.at(traceIndex), lastSizeVec_.at(traceIndex).begin(), lastSizeVec_.at(traceIndex).end(), lastOpVec_.at(traceIndex), lastTsVec_.at(traceIndex)));
                streamVec_.at(traceIndex).open(config_.blockReplayConfig.traces.at(traceIndex));
            }
        }

        virtual ~BlockReplayGenerator() {
        }

        // not needed for block trace replay 
        const std::vector<std::string>& getAllKeys() const override {
            throw std::logic_error("no keys precomputed!");
        }

        // getReq generates the next request from the trace file.
        const Request& getReq(
            uint8_t,
            std::mt19937_64&,
            std::optional<uint64_t> lastRequestId = std::nullopt) override;

    private:
        const StressorConfig config_;

        // vector of streams for each trace file 
        std::vector<std::ifstream> streamVec_;

        // vector of line count 
        std::vector<uint64_t> lineCountVec_;

        // all the params of previous request from each trace file 
        std::vector<Request> lastRequestVec_;
        std::vector<std::string> lastLbaVec_;
        std::vector<OpType> lastOpVec_;
        std::vector<std::vector<size_t>> lastSizeVec_;
        std::vector<uint64_t> lastTsVec_;
};


// get the next block request 
const Request& BlockReplayGenerator::getReq(uint8_t fileIndex, std::mt19937_64&, std::optional<uint64_t>) {
    if (fileIndex >= streamVec_.size())
        throw std::runtime_error(folly::sformat("{} traces but index {} was requested.\n", streamVec_.size(), fileIndex));
    
    std::string line("");
    if (!std::getline(streamVec_.at(fileIndex), line)) {
        throw cachelib::cachebench::EndOfTrace("End of trace replay \n");
    }

    lineCountVec_.at(fileIndex)++;
    std::stringstream row_stream(line);
    std::string token;

    // Timestamp 
    std::getline(row_stream, token, ',');
    uint64_t ts = uint64_t (std::stoul(token)/config_.blockReplayConfig.replayRate);
    lastTsVec_.at(fileIndex) = ts;

    // LBA
    std::getline(row_stream, token, ',');
    lastLbaVec_.at(fileIndex) = token;

    // Operation (read/write)
    std::getline(row_stream, token, ',');
    OpType op;
    if (token == "r")
        op = OpType::kGet;
    else if (token == "w")
        op = OpType::kSet;
    else
        throw std::invalid_argument(folly::sformat("Unrecognized Operation {} in the trace file!", token));
    lastOpVec_.at(fileIndex) = op;

    // Size 
    std::getline(row_stream, token, ',');
    uint64_t size = std::stoul(token);
    lastSizeVec_.at(fileIndex).at(0) = size;

    // update the request 
    lastRequestVec_.at(fileIndex).setOp(op);
    *(lastRequestVec_.at(fileIndex).sizeBegin) = lastSizeVec_.at(fileIndex).at(0);
    lastRequestVec_.at(fileIndex).timestamp = lastTsVec_.at(fileIndex);
    lastRequestVec_.at(fileIndex).key = lastLbaVec_.at(fileIndex);

    return lastRequestVec_.at(fileIndex);
}

}
}
}