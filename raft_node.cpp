#include "raft/raft_node.hpp"

#include <algorithm>
#include <iostream>
#include <random>

namespace raft {

namespace {
std::mt19937& Rng() {
    static thread_local std::mt19937 rng{std::random_device{}()};
    return rng;
}
}  // namespace

RaftNode::RaftNode(NodeId id, std::vector<NodeId> peers, Network& net, StateMachine& sm)
    : id_(id), peers_(std::move(peers)), net_(net), sm_(sm) {
    log_.clear();  // index 0 is implicit sentinel (term 0), not stored
    net_.RegisterNode(id_, this);
}

RaftNode::~RaftNode() { Stop(); }

void RaftNode::Start() {
    running_ = true;
    ResetElectionDeadline();
    electionThread_ = std::thread(&RaftNode::ElectionTimerLoop, this);
    leaderThread_   = std::thread(&RaftNode::LeaderLoop, this);
    applyThread_    = std::thread(&RaftNode::ApplyLoop, this);
}

void RaftNode::Stop() {
    if (!running_.exchange(false)) return;
    cv_.notify_all();
    if (electionThread_.joinable()) electionThread_.join();
    if (leaderThread_.joinable()) leaderThread_.join();
    if (applyThread_.joinable()) applyThread_.join();
}

int RaftNode::RandomElectionTimeoutMs() {
    std::uniform_int_distribution<int> d(150, 300);
    return d(Rng());
}

void RaftNode::ResetElectionDeadline() {
    electionDeadline_ = std::chrono::steady_clock::now() +
                         std::chrono::milliseconds(RandomElectionTimeoutMs());
}

Term RaftNode::LogTermAt(Index idx) const {
    if (idx == 0 || idx > log_.size()) return 0;
    return log_[idx - 1].term;
}

// ---------------------------------------------------------------------
// Election
// ---------------------------------------------------------------------

void RaftNode::ElectionTimerLoop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (!running_) return;
        if (net_.IsDown(id_)) continue;  // crashed nodes don't run timers

        std::unique_lock<std::mutex> lk(mu_);
        if (state_ == NodeState::Leader) continue;  // leaders don't time out
        if (std::chrono::steady_clock::now() < electionDeadline_) continue;
        lk.unlock();
        StartElection();
    }
}

void RaftNode::StartElection() {
    Term electionTerm;
    {
        std::lock_guard<std::mutex> lk(mu_);
        state_ = NodeState::Candidate;
        currentTerm_ += 1;
        votedFor_ = id_;
        electionTerm = currentTerm_;
        ResetElectionDeadline();
        std::cout << "[node " << id_ << "] starting election for term " << electionTerm << "\n";
    }

    for (NodeId peer : peers_) {
        std::thread(&RaftNode::SendRequestVoteTo, this, peer, electionTerm).detach();
    }
}

void RaftNode::SendRequestVoteTo(NodeId peer, Term electionTerm) {
    RequestVoteArgs args;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (currentTerm_ != electionTerm || state_ != NodeState::Candidate) return;
        args.term = electionTerm;
        args.candidateId = id_;
        args.lastLogIndex = LastLogIndex();
        args.lastLogTerm = LastLogTerm();
    }

    if (!net_.CanDeliver(id_, peer)) return;
    RaftNode* target = net_.Get(peer);
    if (!target) return;
    RequestVoteReply reply = target->HandleRequestVote(args);
    if (!net_.CanDeliver(peer, id_)) return;  // reply lost on the way back

    std::lock_guard<std::mutex> lk(mu_);

    if (reply.term > currentTerm_) {
        // Someone is ahead of us -- abandon this election and fall back to
        // follower, exactly as the paper's "rules for servers" require.
        currentTerm_ = reply.term;
        state_ = NodeState::Follower;
        votedFor_ = -1;
        return;
    }

    if (state_ != NodeState::Candidate || currentTerm_ != electionTerm) return;
    if (!reply.voteGranted) return;

    grantedVotes_[electionTerm].insert(peer);
    size_t haveVotes = grantedVotes_[electionTerm].size() + 1;  // +1 self
    size_t majority = (peers_.size() + 1) / 2 + 1;
    if (haveVotes >= majority && state_ == NodeState::Candidate && currentTerm_ == electionTerm) {
        state_ = NodeState::Leader;
        std::cout << "[node " << id_ << "] === became LEADER for term " << electionTerm
                  << " with " << haveVotes << "/" << (peers_.size() + 1) << " votes ===\n";
        Index nextIdx = LastLogIndex() + 1;
        for (NodeId p : peers_) {
            nextIndex_[p] = nextIdx;
            matchIndex_[p] = 0;
        }
    }
}

RequestVoteReply RaftNode::HandleRequestVote(const RequestVoteArgs& args) {
    std::lock_guard<std::mutex> lk(mu_);
    RequestVoteReply reply;

    if (args.term > currentTerm_) {
        currentTerm_ = args.term;
        state_ = NodeState::Follower;
        votedFor_ = -1;
    }
    reply.term = currentTerm_;

    if (args.term < currentTerm_) {
        reply.voteGranted = false;
        return reply;
    }

    bool logOk = (args.lastLogTerm > LastLogTerm()) ||
                 (args.lastLogTerm == LastLogTerm() && args.lastLogIndex >= LastLogIndex());

    if ((votedFor_ == -1 || votedFor_ == args.candidateId) && logOk) {
        votedFor_ = args.candidateId;
        reply.voteGranted = true;
        ResetElectionDeadline();
    } else {
        reply.voteGranted = false;
    }
    return reply;
}

// ---------------------------------------------------------------------
// Replication (leader side)
// ---------------------------------------------------------------------

void RaftNode::LeaderLoop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(40));  // heartbeat interval
        if (!running_) return;
        if (net_.IsDown(id_)) continue;

        bool amLeader;
        {
            std::lock_guard<std::mutex> lk(mu_);
            amLeader = (state_ == NodeState::Leader);
        }
        if (!amLeader) continue;

        std::vector<std::thread> workers;
        for (NodeId peer : peers_) {
            workers.emplace_back(&RaftNode::ReplicateTo, this, peer);
        }
        for (auto& t : workers) t.join();

        AdvanceCommitIndex();
    }
}

void RaftNode::ReplicateTo(NodeId peer) {
    AppendEntriesArgs args;
    Term myTerm;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (state_ != NodeState::Leader) return;
        myTerm = currentTerm_;
        Index ni = nextIndex_.count(peer) ? nextIndex_[peer] : LastLogIndex() + 1;
        args.term = currentTerm_;
        args.leaderId = id_;
        args.prevLogIndex = ni - 1;
        args.prevLogTerm = LogTermAt(args.prevLogIndex);
        for (Index i = ni; i <= LastLogIndex(); ++i) args.entries.push_back(log_[i - 1]);
        args.leaderCommit = commitIndex_;
    }

    if (!net_.CanDeliver(id_, peer)) return;
    RaftNode* target = net_.Get(peer);
    if (!target) return;
    AppendEntriesReply reply = target->HandleAppendEntries(args);
    if (!net_.CanDeliver(peer, id_)) return;

    std::lock_guard<std::mutex> lk(mu_);
    if (state_ != NodeState::Leader || currentTerm_ != myTerm) return;

    if (reply.term > currentTerm_) {
        currentTerm_ = reply.term;
        state_ = NodeState::Follower;
        votedFor_ = -1;
        std::cout << "[node " << id_ << "] stepping down, saw higher term " << reply.term << "\n";
        return;
    }

    if (reply.success) {
        Index newMatch = args.prevLogIndex + args.entries.size();
        matchIndex_[peer] = std::max(matchIndex_[peer], newMatch);
        nextIndex_[peer] = matchIndex_[peer] + 1;
    } else {
        // Fast backtrack using the conflict hint instead of decrementing by one.
        if (reply.conflictTerm != 0) {
            Index i = LastLogIndex();
            while (i > 0 && LogTermAt(i) != reply.conflictTerm) --i;
            nextIndex_[peer] = (i == 0) ? reply.conflictIndex : i + 1;
        } else {
            nextIndex_[peer] = std::max<Index>(1, reply.conflictIndex);
        }
    }
}

void RaftNode::AdvanceCommitIndex() {
    std::lock_guard<std::mutex> lk(mu_);
    if (state_ != NodeState::Leader) return;
    for (Index n = LastLogIndex(); n > commitIndex_; --n) {
        if (LogTermAt(n) != currentTerm_) continue;  // only commit own-term entries directly
        size_t count = 1;  // leader itself
        for (NodeId p : peers_) if (matchIndex_[p] >= n) ++count;
        if (count >= (peers_.size() + 1) / 2 + 1) {
            commitIndex_ = n;
            cv_.notify_all();
            break;
        }
    }
}

AppendEntriesReply RaftNode::HandleAppendEntries(const AppendEntriesArgs& args) {
    std::lock_guard<std::mutex> lk(mu_);
    AppendEntriesReply reply;

    if (args.term > currentTerm_) {
        currentTerm_ = args.term;
        votedFor_ = -1;
    }
    reply.term = currentTerm_;

    if (args.term < currentTerm_) {
        reply.success = false;
        return reply;
    }

    // Valid leader for this (or newer) term: reset to follower and reset
    // the election clock -- this is what prevents a follower that can
    // still hear the leader from starting a pointless election.
    state_ = NodeState::Follower;
    ResetElectionDeadline();

    if (args.prevLogIndex > LastLogIndex()) {
        reply.success = false;
        reply.conflictIndex = LastLogIndex() + 1;
        reply.conflictTerm = 0;
        return reply;
    }
    if (args.prevLogIndex > 0 && LogTermAt(args.prevLogIndex) != args.prevLogTerm) {
        reply.conflictTerm = LogTermAt(args.prevLogIndex);
        Index i = args.prevLogIndex;
        while (i > 1 && LogTermAt(i - 1) == reply.conflictTerm) --i;
        reply.conflictIndex = i;
        reply.success = false;
        return reply;
    }

    // Append new entries, truncating any conflicting suffix first (this is
    // exactly how a stale ex-leader's uncommitted writes get discarded once
    // the partition heals -- log matching wins, not "who wrote first").
    Index insertAt = args.prevLogIndex;
    for (const LogEntry& e : args.entries) {
        insertAt += 1;
        if (insertAt <= LastLogIndex()) {
            if (LogTermAt(insertAt) == e.term) continue;
            log_.resize(insertAt - 1);
        }
        LogEntry stored = e;
        stored.index = insertAt;
        log_.push_back(stored);
    }

    if (args.leaderCommit > commitIndex_) {
        commitIndex_ = std::min(args.leaderCommit, LastLogIndex());
        cv_.notify_all();
    }
    reply.success = true;
    return reply;
}

// ---------------------------------------------------------------------
// Client-facing API + apply loop
// ---------------------------------------------------------------------

bool RaftNode::Propose(const std::string& command, Index* outIndex, Term* outTerm) {
    std::lock_guard<std::mutex> lk(mu_);
    if (state_ != NodeState::Leader) return false;
    LogEntry e;
    e.term = currentTerm_;
    e.index = LastLogIndex() + 1;
    e.command = command;
    log_.push_back(e);
    if (outIndex) *outIndex = e.index;
    if (outTerm) *outTerm = e.term;
    return true;
}

bool RaftNode::WaitApplied(Index index, Term term, int timeoutMs, std::string* outResult) {
    std::unique_lock<std::mutex> lk(mu_);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (lastApplied_ < index) {
        if (state_ != NodeState::Leader && currentTerm_ > term) {
            // We were deposed and enough time has passed that this entry
            // almost certainly isn't ours anymore; keep waiting for the
            // real outcome rather than guessing -- fall through to timeout.
        }
        if (cv_.wait_until(lk, deadline) == std::cv_status::timeout) return false;
    }
    auto it = results_.find(index);
    auto itTerm = resultTerm_.find(index);
    if (it == results_.end() || itTerm == resultTerm_.end() || itTerm->second != term) {
        return false;  // entry at this index belongs to a different term -> our write was lost
    }
    if (outResult) *outResult = it->second;
    return true;
}

void RaftNode::ApplyLoop() {
    while (running_) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait_for(lk, std::chrono::milliseconds(20),
                     [this] { return !running_ || lastApplied_ < commitIndex_; });
        while (lastApplied_ < commitIndex_) {
            lastApplied_ += 1;
            const LogEntry& e = log_[lastApplied_ - 1];
            std::string cmd = e.command;
            Index idx = e.index;
            Term term = e.term;
            lk.unlock();
            std::string result = sm_.Apply(idx, cmd);
            lk.lock();
            results_[idx] = result;
            resultTerm_[idx] = term;
        }
        cv_.notify_all();
        if (!running_) return;
    }
}

}  // namespace raft
