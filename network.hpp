#pragma once
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <utility>

#include "raft/types.hpp"

namespace raft {

class RaftNode;

// Network simulates the unreliable channel between nodes. It is the single
// point of truth for "who can currently talk to whom", so failure scenarios
// (partitions, packet loss, latency, dead nodes) are all injected here
// rather than inside RaftNode -- RaftNode only ever sees ordinary RPC
// timeouts, exactly as it would over a real socket.
class Network {
public:
    void RegisterNode(NodeId id, RaftNode* node) {
        std::lock_guard<std::mutex> lk(mu_);
        nodes_[id] = node;
    }

    // --- failure injection controls -------------------------------------

    // Split the cluster into two groups that cannot reach each other.
    // Nodes not mentioned keep talking to everyone.
    void Partition(std::set<NodeId> groupA) {
        std::lock_guard<std::mutex> lk(mu_);
        partitionGroupA_ = std::move(groupA);
        partitioned_ = true;
    }

    void HealPartition() {
        std::lock_guard<std::mutex> lk(mu_);
        partitioned_ = false;
        partitionGroupA_.clear();
    }

    void CrashNode(NodeId id) {
        std::lock_guard<std::mutex> lk(mu_);
        down_.insert(id);
    }

    void ReviveNode(NodeId id) {
        std::lock_guard<std::mutex> lk(mu_);
        down_.erase(id);
    }

    void SetPacketLossPercent(int pct) {
        std::lock_guard<std::mutex> lk(mu_);
        lossPct_ = pct;
    }

    void SetLatencyMs(int minMs, int maxMs) {
        std::lock_guard<std::mutex> lk(mu_);
        minLatency_ = minMs;
        maxLatency_ = maxMs;
    }

    // Returns false if the message should be dropped (partition, crash, or
    // simulated packet loss). Otherwise sleeps for a simulated latency and
    // returns true. This is called by the *caller* of an RPC, i.e. it
    // models both the outbound and inbound leg of one message.
    bool CanDeliver(NodeId from, NodeId to) {
        int sleepMs = 0;
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (down_.count(from) || down_.count(to)) return false;
            if (partitioned_) {
                bool aHasFrom = partitionGroupA_.count(from) > 0;
                bool aHasTo   = partitionGroupA_.count(to) > 0;
                if (aHasFrom != aHasTo) return false;  // opposite sides
            }
            std::uniform_int_distribution<int> lossRoll(1, 100);
            if (lossRoll(rng_) <= lossPct_) return false;
            std::uniform_int_distribution<int> latRoll(minLatency_, maxLatency_);
            sleepMs = latRoll(rng_);
        }
        if (sleepMs > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
        }
        return true;
    }

    RaftNode* Get(NodeId id) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = nodes_.find(id);
        return it == nodes_.end() ? nullptr : it->second;
    }

    bool IsDown(NodeId id) {
        std::lock_guard<std::mutex> lk(mu_);
        return down_.count(id) > 0;
    }

private:
    std::mutex mu_;
    std::map<NodeId, RaftNode*> nodes_;
    std::set<NodeId> down_;
    bool partitioned_ = false;
    std::set<NodeId> partitionGroupA_;
    int lossPct_ = 0;
    int minLatency_ = 1;
    int maxLatency_ = 5;
    std::mt19937 rng_{std::random_device{}()};
};

}  // namespace raft
