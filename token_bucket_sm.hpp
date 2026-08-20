#pragma once
#include <algorithm>
#include <map>
#include <mutex>
#include <sstream>
#include <string>

#include "raft/state_machine.hpp"

namespace ratelimiter {

// Replicated token-bucket rate limiter.
//
// Every node runs its own copy of this state machine, but because Raft
// guarantees the exact same sequence of commands is applied everywhere,
// every node reaches the exact same allow/deny decision for the exact same
// request -- that's the whole point of routing rate-limit decisions through
// consensus instead of just keeping local, per-node counters that would
// drift under partition.
//
// Determinism note: "now" cannot be read from the wall clock inside Apply(),
// because different nodes would apply the same entry at different wall-clock
// instants. Instead the timestamp is chosen once by the leader and baked
// into the command string, so every replica computes an identical result.
class TokenBucketSM : public raft::StateMachine {
public:
    // command format: "CHECK <key> <capacity> <refillPerSec> <cost> <timestampMs>"
    std::string Apply(raft::Index /*index*/, const std::string& command) override {
        std::istringstream iss(command);
        std::string op, key;
        double capacity, refillPerSec, cost, timestampMs;
        iss >> op >> key >> capacity >> refillPerSec >> cost >> timestampMs;
        if (op != "CHECK") return "ERROR unknown_op";

        std::lock_guard<std::mutex> lk(mu_);
        Bucket& b = buckets_[key];
        if (!b.initialized) {
            b.tokens = capacity;
            b.lastRefillMs = timestampMs;
            b.initialized = true;
        } else {
            double elapsedSec = std::max(0.0, (timestampMs - b.lastRefillMs) / 1000.0);
            b.tokens = std::min(capacity, b.tokens + elapsedSec * refillPerSec);
            b.lastRefillMs = timestampMs;
        }

        if (b.tokens >= cost) {
            b.tokens -= cost;
            std::ostringstream oss;
            oss << "ALLOWED remaining=" << b.tokens;
            return oss.str();
        }
        std::ostringstream oss;
        oss << "DENIED remaining=" << b.tokens;
        return oss.str();
    }

    double TokensRemaining(const std::string& key) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = buckets_.find(key);
        return it == buckets_.end() ? -1.0 : it->second.tokens;
    }

private:
    struct Bucket {
        bool initialized = false;
        double tokens = 0;
        double lastRefillMs = 0;
    };
    std::mutex mu_;
    std::map<std::string, Bucket> buckets_;
};

}  // namespace ratelimiter
