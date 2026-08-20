# Distributed Rate Limiter with Raft Consensus (C++)

A rate limiter whose decisions are agreed on by a cluster of nodes via a
**from-scratch Raft implementation** — no consensus library, no external
dependency beyond the standard library and threads. The point of the
project is the consensus engine itself: leader election, log replication,
commit-index advancement, and correct behavior under partitions, crashes,
and split-brain, all implemented and then stress-tested against simulated
failures rather than assumed.

## Why a rate limiter as the workload

A naive distributed rate limiter keeps independent counters per node, which
drift and let traffic through inconsistently under partition. Routing every
`CHECK` decision through a **replicated log** means every node applies the
exact same sequence of token-bucket operations in the exact same order, so
all replicas converge on the identical allow/deny outcome — the rate limiter
is just a state machine sitting on top of consensus, which is exactly what
Raft is designed to replicate.

## What's implemented

- **Leader election** — randomized election timeouts (150–300ms), term
  increments, `RequestVote` RPCs, majority quorum, split votes handled by
  timeout + retry (`raft_node.cpp: StartElection`, `HandleRequestVote`).
- **Log replication** — `AppendEntries` with `prevLogIndex`/`prevLogTerm`
  consistency checks, conflicting-suffix truncation, and a fast backtrack
  using conflict-term hints instead of decrementing `nextIndex` one at a
  time (`ReplicateTo`, `HandleAppendEntries`).
- **Commit-index advancement** — a leader only commits entries from its
  *own* current term once replicated to a majority, per the Raft safety
  argument (Figure 8 in the paper) — this is what prevents an older,
  already-replicated-but-uncommitted entry from being incorrectly exposed.
- **Failover / split-brain prevention** — `Network` (in `network.hpp`) can
  partition the cluster into two groups, drop packets, add latency, or crash
  a node outright. A leader stuck in the minority side can keep appending to
  its own log, but can never reach a majority, so `WaitApplied` times out
  and the write is correctly reported as never committed. When the
  partition heals, the stale leader discovers a higher term and steps down;
  its divergent tail is truncated by ordinary log matching, not any special
  reconciliation logic.
- **State machine** — `TokenBucketSM` applies `CHECK key capacity refillRate
  cost timestamp` commands deterministically. The timestamp is chosen once
  by the leader and embedded in the command so every replica computes the
  identical refill/consume math, regardless of when each replica happens to
  apply the entry.

## What's intentionally out of scope

- **Log compaction / snapshotting** — the in-memory log grows unbounded;
  a real system would snapshot and truncate. Left out to keep the core
  algorithm the focus.
- **Persistence** — `currentTerm`, `votedFor`, and the log are supposed to
  be fsynced before replying to RPCs (Raft's crash-recovery safety depends
  on this). Here they live in memory, so a "crash" is really a node going
  silent and later resuming with its in-memory state intact — it models
  partitions/pauses accurately but not power loss.
- **Real sockets** — nodes talk through an in-process `Network` object that
  can drop/delay/partition messages, rather than over TCP. This keeps the
  failure-injection deterministic and testable; swapping in a socket layer
  would only touch `Network::CanDeliver`/`Get`, not `RaftNode`.
- **Membership changes** — the cluster size is fixed at startup.

## Build & run

```bash
g++ -std=c++17 -Wall -Wextra -O2 -Iinclude src/raft_node.cpp src/main.cpp -pthread -o raft_ratelimiter
./raft_ratelimiter
```

(A `CMakeLists.txt` is included for IDEs / `cmake --build`, if `cmake` is
available in your environment.)

## What the demo (`main.cpp`) actually exercises

1. Boots 5 nodes, waits for a leader to be elected purely by RPCs.
2. Sends real rate-limit checks against a token bucket (capacity 5,
   refill 1/sec) and shows it correctly transitions from `ALLOWED` to
   `DENIED` once exhausted.
3. **Partitions the current leader into a 1-vs-4 minority.** Shows its
   in-flight write never commits (no majority ack).
4. Shows the 4-node majority independently elects a new leader and keeps
   serving traffic while partitioned — the cluster stays available and
   correct despite half of it being unreachable.
5. **Heals the partition** and shows the stale leader step down and its
   log reconverge with the rest of the cluster.
6. **Crashes a follower**, shows the cluster still serves traffic on a
   remaining majority, then revives it and shows it catch up via ordinary
   `AppendEntries` replication.
7. Walks every node's committed log and confirms byte-for-byte agreement —
   the actual consensus safety property, checked programmatically rather
   than asserted.

## File layout

```
include/raft/types.hpp              term/index/log entry/RPC structs
include/raft/network.hpp            simulated network: partitions, drops, latency, crashes
include/raft/state_machine.hpp      StateMachine interface (Apply(index, command))
include/raft/raft_node.hpp          RaftNode class declaration
src/raft_node.cpp                   election + replication + commit logic
include/ratelimiter/token_bucket_sm.hpp   the replicated rate limiter
src/main.cpp                        cluster bootstrap + failure-scenario demo
```
