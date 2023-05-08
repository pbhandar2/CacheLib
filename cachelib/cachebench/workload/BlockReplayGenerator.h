#pragma once

#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {


// a class to read a block trace file 
// multiple FileReader classes will be used when replaying multiple files 
class FileReader {
    public:
        explicit FileReader(std::string path)
            : path_(path) {

            }
    
    protected:
        std::string path_; 
};


class BlockReplayGenerator : public GeneratorBase {
    public:
        explicit BlockReplayGenerator(const StressorConfig& config)
        :   config_(config) {
        }

        virtual ~BlockReplayGenerator() {}

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
        uint64_t requestCount_{0};
};


// get the next block request 
const Request& BlockReplayGenerator::getReq(uint8_t, std::mt19937_64&, std::optional<uint64_t>) {
    if (requestCount_ > 0)
        throw cachelib::cachebench::EndOfTrace("Test stopped or EOF reached");

    requestCount_++;
    std::string oneHitKey = "key";
    std::vector<size_t> sizes{100};
    Request tempReq(oneHitKey, sizes.begin(), sizes.end(), OpType::kGet, 100);
    const Request& req(Request(oneHitKey, sizes.begin(), sizes.end(), OpType::kGet, 100));
    return req;
}

}
}
}