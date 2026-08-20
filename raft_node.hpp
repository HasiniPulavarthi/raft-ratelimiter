#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "raft/network.hpp"
#include "raft/state_machine.hpp"
#include "raft/types.hpp"

namespace raft {

// A full Raft peer: election, replication and commit-index advancement,
// implemented from the paper (Ongaro & Ousterhout, "In Search of an
// Understandable Consensus Algorithm") rather than wrapping a library.
class RaftNode {
public:
    RaftNode(NodeId id, std::vector<NodeId> peers, Network& net, StateMachine& sm);
    ~RaftNode();

    void Start();
    void Stop();

    // Client entry point. Only succeeds if this node is currently the
    // leader; the caller must retry against whichever node it discovers
    // is leader otherwise (mirrors how real Raft clients behave).
    bool Propose(const std::string& command, Index* outIndex, Term* outTerm);

    // Blocks the caller until `index` is applied to the state machine (or
    // times out), then returns the result the state machine produced.
    // Returns false on timeout or if the entry at that index was
    // overwritten by a later leader (i.e. it never committed).
    bool WaitApplied(Index index, Term term, int timeoutMs, std::string* outResult);

    // RPC handlers -- invoked by Network on behalf of a peer.
    RequestVoteReply HandleRequestVote(const RequestVoteArgs& args);
    AppendEntriesReply HandleAppendEntries(const AppendEntriesArgs& args);

    NodeId Id() const { return id_; }
    NodeState State() const { std::lock_guard<std::mutex> lk(mu_); return state_; }
    Term CurrentTerm() const { std::lock_guard<std::mutex> lk(mu_); return currentTerm_; }
    bool IsLeader() const { std::lock_guard<std::mutex> lk(mu_); return state_ == NodeState::Leader; }
    Index CommitIndex() const { std::lock_guard<std::mutex> lk(mu_); return commitIndex_; }
    std::vector<LogEntry> LogSnapshot() const { std::lock_guard<std::mutex> lk(mu_); return log_; }

private:
    // --- background loops ---
    void ElectionTimerLoop();
    void LeaderLoop();
    void ApplyLoop();

    void StartElection();
    void SendRequestVoteTo(NodeId peer, Term electionTerm);

    void ReplicateTo(NodeId peer);
    void AdvanceCommitIndex();

    int RandomElectionTimeoutMs();
    void ResetElectionDeadline();

    // Log helpers (0-indexed vector, but Raft indices are 1-based; index 0
    // is a sentinel with term 0).
    Term LogTermAt(Index idx) const;
    Index LastLogIndex() const { return log_.empty() ? 0 : log_.back().index; }
    Term LastLogTerm() const { return log_.empty() ? 0 : log_.back().term; }

    NodeId id_;
    std::vector<NodeId> peers_;
    Network& net_;
    StateMachine& sm_;

    mutable std::mutex mu_;
    std::condition_variable cv_;

    // --- persistent state (would be fsynced to disk in a real system) ---
    Term currentTerm_ = 0;
    NodeId votedFor_ = -1;
    std::vector<LogEntry> log_;  // log_[i].index == i+1

    // --- volatile state ---
    NodeState state_ = NodeState::Follower;
    Index commitIndex_ = 0;
    Index lastApplied_ = 0;
    std::chrono::steady_clock::time_point electionDeadline_;

    // --- leader-only volatile state ---
    std::map<NodeId, Index> nextIndex_;
    std::map<NodeId, Index> matchIndex_;

    // Results of applied commands, keyed by log index, so Propose() callers
    // can retrieve what the state machine decided.
    std::map<Index, std::string> results_;
    std::map<Index, Term> resultTerm_;

    // Votes granted to us, keyed by the election term they were granted in
    // (a fresh set each election; stale replies from an old election are
    // naturally ignored because the map key won't match currentTerm_).
    std::map<Term, std::set<NodeId>> grantedVotes_;

    std::atomic<bool> running_{false};
    std::thread electionThread_;
    std::thread leaderThread_;
    std::thread applyThread_;
};

}  // namespace raft
