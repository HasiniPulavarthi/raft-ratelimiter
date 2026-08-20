#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace raft {

using NodeId  = int;
using Term    = uint64_t;
using Index   = uint64_t;

enum class NodeState { Follower, Candidate, Leader, Down };

inline const char* StateName(NodeState s) {
    switch (s) {
        case NodeState::Follower:  return "Follower";
        case NodeState::Candidate: return "Candidate";
        case NodeState::Leader:    return "Leader";
        case NodeState::Down:      return "Down";
    }
    return "?";
}

// A single entry in the replicated log. `command` is an opaque string that
// the state machine (the rate limiter, in this project) knows how to apply.
struct LogEntry {
    Term term = 0;
    Index index = 0;
    std::string command;
};

// ---- RPC payloads (Raft paper, Figure 2) ----

struct RequestVoteArgs {
    Term    term = 0;
    NodeId  candidateId = -1;
    Index   lastLogIndex = 0;
    Term    lastLogTerm = 0;
};

struct RequestVoteReply {
    Term term = 0;
    bool voteGranted = false;
};

struct AppendEntriesArgs {
    Term term = 0;
    NodeId leaderId = -1;
    Index  prevLogIndex = 0;
    Term   prevLogTerm = 0;
    std::vector<LogEntry> entries;   // empty => heartbeat
    Index  leaderCommit = 0;
};

struct AppendEntriesReply {
    Term term = 0;
    bool success = false;
    // Fast-backtrack hints (used to shrink nextIndex quickly on conflict,
    // instead of decrementing one index at a time).
    Index conflictIndex = 0;
    Term  conflictTerm = 0;
};

}  // namespace raft
