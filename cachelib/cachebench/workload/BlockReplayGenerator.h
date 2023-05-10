#pragma once

#include <iostream>
#include <fstream>

#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/logging/xlog.h>

#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {


// This class reads a file of a given path 
class FileReader {
    public:
        explicit FileReader(std::string path)
                : path_(path) {
            infile_.open(path);
            if (infile_.is_open()) {
                std::cout << "File opened successfully: " << path << std::endl;
            } else {
                std::cout << "Failed to open file: " << path << std::endl;
            }
        }

        virtual ~FileReader() {
            if (infile_.is_open()) {
                infile_.close();
                infile_.clear();
            }
        }

        // get the next line of the trace
        std::string getNextLine() {
            std::string line;
            if (!std::getline(infile_, line)) 
                throw cachelib::cachebench::EndOfTrace(folly::sformat("End of trace: {}", path_));
            return line; 
        }
    
    private:
        std::string path_; 
        std::ifstream infile_;
};


class BlockReplayGenerator : public GeneratorBase {
    public:

        explicit BlockReplayGenerator(const StressorConfig& config)
        :   config_(config) {
            for (int traceIndex=0; traceIndex < config_.blockReplayConfig.traces.size(); traceIndex++) {
                readers_.push_back(new FileReader(config_.blockReplayConfig.traces.at(traceIndex)));
            }
        }

        virtual ~BlockReplayGenerator() {
            readers_.clear();
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
        uint64_t requestCount_{0};
        const StressorConfig config_;
        std::vector<FileReader*> readers_;
};


// get the next block request 
const Request& BlockReplayGenerator::getReq(uint8_t fileIndex, std::mt19937_64&, std::optional<uint64_t>) {
    if (fileIndex >= readers_.size())
        throw std::runtime_error(folly::sformat("There are {} traces but index {} was requested in BlockReplayGenerator.\n", readers_.size(), fileIndex));

    std::string line = readers_.at(fileIndex)->getNextLine();

    // split the file 
    std::vector<folly::StringPiece> fields;
    folly::split(",", line, fields);

    std::cout << folly::sformat("FIELDS HAVE {}\n", fields.size());

    // split line 
    // uint64_t ts = folly::tryTo<uint64_t>(fields[0]);
    // uint64_t lba = folly::tryTo<size_t>(fields[1]);
    // std::string op = folly::tryTo<std::string>(fields[2]);
    // std::string size = folly::tryTo<size_t>(fields[3]);

    // std::cout << folly::sformat("{}\n", line);
    // std::cout << folly::sformat("{},{},{},{}\n", ts, lba, op, size);

    // create the same fake request for now 
    std::string oneHitKey = "key";
    std::vector<size_t> sizes{100};
    const Request& req(Request(oneHitKey, sizes.begin(), sizes.end(), OpType::kGet, 100));

    requestCount_++;
    return req;
}

}
}
}