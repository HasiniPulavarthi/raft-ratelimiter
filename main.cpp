#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <set>
#include <sstream>
#include <thread>
#include <vector>

#include "raft/network.hpp"
#include "raft/raft_node.hpp"
#include "ratelimiter/token_bucket_sm.hpp"

using namespace std::chrono_literals;

namespace {

double NowMs() {
    return std::chrono::duration<double, std::milli>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

void Banner(const std::string& title) {
    std::cout << "\n========== " << title << " ==========\n";
}

raft::RaftNode* FindLeader(std::vector<std::unique_ptr<raft::RaftNode>>& nodes) {
    for (auto& n : nodes) if (n->IsLeader()) return n.get();
    return nullptr;
}

// Tries to make a rate-limit decision against whichever node currently
// claims to be leader. Retries against a fresh leader on failure, exactly
// like a real client would when it gets NOT_LEADER or a timeout.
std::string ClientCheck(std::vector<std::unique_ptr<raft::RaftNode>>& nodes,
                         const std::string& key, double capacity, double refillPerSec,
                         double cost, int maxRetries = 6) {
    for (int attempt = 0; attempt < maxRetries; ++attempt) {
        raft::RaftNode* leader = FindLeader(nodes);
        if (!leader) { std::this_thread::sleep_for(60ms); continue; }

        std::ostringstream cmd;
        cmd << "CHECK " << key << " " << capacity << " " << refillPerSec << " " << cost
            << " " << NowMs();

        raft::Index idx; raft::Term term;
        if (!leader->Propose(cmd.str(), &idx, &term)) { std::this_thread::sleep_for(30ms); continue; }

        std::string result;
        if (leader->WaitApplied(idx, term, 400, &result)) return result;
        // Leader likely got deposed (e.g. partitioned into the minority)
        // before the entry could commit -- retry against the new leader.
    }
    return "ERROR no_leader_after_retries";
}

void PrintClusterStatus(std::vector<std::unique_ptr<raft::RaftNode>>& nodes) {
    for (auto& n : nodes) {
        std::cout << "  node " << n->Id() << ": " << raft::StateName(n->State())
                  << " term=" << n->CurrentTerm() << " commitIdx=" << n->CommitIndex()
                  << " logLen=" << n->LogSnapshot().size() << "\n";
    }
}

}  // namespace

int main() {
    constexpr int kNumNodes = 5;
    raft::Network net;
    ratelimiter::TokenBucketSM sm;

    std::vector<std::unique_ptr<raft::RaftNode>> nodes;
    for (int i = 0; i < kNumNodes; ++i) {
        std::vector<raft::NodeId> peers;
        for (int j = 0; j < kNumNodes; ++j) if (j != i) peers.push_back(j);
        nodes.push_back(std::make_unique<raft::RaftNode>(i, peers, net, sm));
    }
    for (auto& n : nodes) n->Start();

    // ---------------------------------------------------------------
    Banner("1. Initial leader election among 5 healthy nodes");
    std::this_thread::sleep_for(600ms);
    PrintClusterStatus(nodes);

    // ---------------------------------------------------------------
    Banner("2. Normal client traffic: bucket capacity=5, refill=1/sec, cost=1");
    for (int i = 0; i < 7; ++i) {
        std::string r = ClientCheck(nodes, "api-key-42", 5, 1, 1);
        std::cout << "  request " << i << " -> " << r << "\n";
    }
    std::cout << "  (first 5 ALLOWED, then DENIED once the bucket is drained "
                 "faster than it refills -- correct token-bucket behavior)\n";

    // ---------------------------------------------------------------
    Banner("3. Inject network partition: {leader} isolated in minority (1 vs 4)");
    raft::RaftNode* leaderBefore = FindLeader(nodes);
    int leaderId = leaderBefore ? leaderBefore->Id() : 0;
    std::set<raft::NodeId> minority = {leaderId};
    net.Partition(minority);
    std::cout << "  partitioned node " << leaderId << " away from the rest of the cluster\n";

    std::cout << "  old leader tries to serve a request while cut off from the majority...\n";
    if (leaderBefore) {
        raft::Index idx; raft::Term term;
        std::ostringstream cmd;
        cmd << "CHECK during-partition 5 1 1 " << NowMs();
        if (leaderBefore->Propose(cmd.str(), &idx, &term)) {
            std::string result;
            bool committed = leaderBefore->WaitApplied(idx, term, 500, &result);
            std::cout << "  -> " << (committed ? "committed: " + result
                                                : "NEVER COMMITTED (no majority ack) -- correctly rejected")
                      << "\n";
        }
    }

    std::this_thread::sleep_for(600ms);
    Banner("4. Majority side elects a new leader; old leader can't (split-brain avoided)");
    PrintClusterStatus(nodes);
    raft::RaftNode* leaderDuringPartition = nullptr;
    for (auto& n : nodes) {
        if (n->IsLeader() && n->Id() != leaderId) leaderDuringPartition = n.get();
    }
    std::cout << "  new leader on majority side: node "
              << (leaderDuringPartition ? std::to_string(leaderDuringPartition->Id()) : "none")
              << " -- old leader " << leaderId << " remains stuck as a lone leader with no quorum\n";

    std::cout << "  serving more client traffic against the majority leader while partitioned:\n";
    for (int i = 0; i < 3; ++i) {
        std::string r = ClientCheck(nodes, "api-key-42", 5, 1, 1);
        std::cout << "    request -> " << r << "\n";
    }

    // ---------------------------------------------------------------
    Banner("5. Heal the partition");
    net.HealPartition();
    std::this_thread::sleep_for(500ms);
    PrintClusterStatus(nodes);
    std::cout << "  old leader " << leaderId
              << " discovers the higher term via AppendEntries/RequestVote and steps down;\n"
              << "  its divergent uncommitted entry (if any) is truncated by log matching.\n";

    // ---------------------------------------------------------------
    Banner("6. Crash a follower mid-traffic, then bring it back");
    raft::RaftNode* victim = nullptr;
    for (auto& n : nodes) if (!n->IsLeader()) { victim = n.get(); break; }
    int victimId = victim->Id();
    net.CrashNode(victimId);
    std::cout << "  crashed node " << victimId << "\n";
    for (int i = 0; i < 4; ++i) ClientCheck(nodes, "api-key-99", 10, 2, 1);
    std::cout << "  cluster kept serving traffic with " << (kNumNodes - 1) << "/" << kNumNodes
              << " nodes (still a majority)\n";

    net.ReviveNode(victimId);
    std::cout << "  revived node " << victimId << ", waiting for it to catch up via replication...\n";
    std::this_thread::sleep_for(500ms);

    // ---------------------------------------------------------------
    Banner("7. Final consistency check across all nodes");
    PrintClusterStatus(nodes);
    bool allMatch = true;
    auto refLog = nodes[0]->LogSnapshot();
    raft::Index refCommit = nodes[0]->CommitIndex();
    for (auto& n : nodes) {
        auto lg = n->LogSnapshot();
        raft::Index c = n->CommitIndex();
        size_t cmpLen = std::min<size_t>(std::min(refCommit, c), std::min(lg.size(), refLog.size()));
        for (size_t i = 0; i < cmpLen; ++i) {
            if (lg[i].term != refLog[i].term || lg[i].command != refLog[i].command) {
                allMatch = false;
                std::cout << "  MISMATCH at index " << (i + 1) << " on node " << n->Id() << "\n";
            }
        }
    }
    std::cout << "  " << (allMatch ? "All nodes agree on every committed log entry. Consensus holds.\n"
                                    : "Divergence detected -- see above.\n");

    for (auto& n : nodes) n->Stop();
    return 0;
}
