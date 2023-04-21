#pragma once

namespace facebook {
namespace cachelib {
namespace cachebench {

// BlockReplayGenerator generates block requests 
// from a block storage trace(s). 
class BlockReplayGenerator : public ReplayGeneratorBase {
    public:
        // input format is: ts,key,op,size
        enum SampleFields { TS = 0, KEY, OP, SIZE, END };

        explicit KVReplayGenerator(const StressorConfig& config)
            : ReplayGeneratorBase(config) {
        }

        virtual ~KVReplayGenerator() {
            XCHECK(shouldShutdown());
        }

        // getReq generates the next request from the trace file.
        const Request& getReq(
            uint8_t,
            std::mt19937_64&,
            std::optional<uint64_t> lastRequestId = std::nullopt) override;
        
        void renderStats(uint64_t, std::ostream& out) const override {
            out << std::endl << "== KVReplayGenerator Stats ==" << std::endl;

            out << folly::sformat("{}: {:.2f} million (parse error: {})",
                            "Total Processed Samples",
                            (double)parseSuccess.load() / 1e6, parseError.load())
            << std::endl;
        }

        void notifyResult(uint64_t requestId, OpResultType result) override;

        void markFinish() override { getStressorCtx().markFinish(); }

        // Parse the request from the trace line and set the ReqWrapper
        bool parseRequest(const std::string& line, std::unique_ptr<ReqWrapper>& req);
    
    private:
        // Used to signal end of file as EndOfTrace exception
        std::atomic<bool> eof{false};
};

inline bool BlockReplayGenerator::parseRequest(const std::string& line,
                                            std::unique_ptr<ReqWrapper>& req) {
}

const Request& BlockReplayGenerator::getReq(uint8_t,
                                         std::mt19937_64&,
                                         std::optional<uint64_t>) {
    throw cachelib::cachebench::EndOfTrace("Test stopped or EOF reached");
    return nullptr;
}

void BlockReplayGenerator::notifyResult(uint64_t requestId, OpResultType) {
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook