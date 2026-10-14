# HANDOFF

## State

Branch `commit-2`. Phases 1-8 are on this branch. This push is phase 9: a loopback benchmark harness against commit-1 `9ae58dd`. No production change. The PR stays a draft.

`bench/Synkrolyn.Bench` is an executable, not a test project. `dotnet test` does not run it. `.github/workflows/bench.yml` is `workflow_dispatch` only. Numbers, machine info, and findings are in `docs/benchmarks.md`.

Findings, not fixed here: commit p99 about 30 ms versus about 3 to 6 ms; lease-read p50 about 0.37 us versus 0.17 us; election p99 about 280 ms versus about 153 ms. Write throughput is higher (about 9100 ops/s versus about 1400). Commit p50, ReadIndex p50, and election p50 stay inside noise. Proposed fixes are in that doc. None was small enough to land without a new test.

Peer liveness is inbound silence past `PeerDeadTimeout`, a socket reset, or a graceful FIN. `Send` only enqueues. AppendEntries stay coalesced. No send-progress drop and no `TCP_USER_TIMEOUT`.

## Decisions

- A pending `SendAsync` is cancelled with `CancelIoEx` on Windows and shutdown otherwise. The waiter completes the operation itself, so `Close` is not the only wake. The socket stays non-blocking. Connect finishes on the writer thread with `Poll`.
- `SendWaits`, `SendDones`, and `AbortTicks` live on the transport. `BlockedSend_counterIsPerTransport` failed first on the static counters: an idle transport reported the busy transport's wait (expected 0, actual 1).
- `IsLive` treats a zero-byte poll and `WouldBlock` as live. Death in that probe is a disconnect socket error, or a non-blocking peek that returns 0 twice (FIN).
- `RequestVote`, `RequestVoteResponse`, and `InstallSnapshotResponse` use an unbounded channel. The 64-slot channel still drops oldest heartbeats and append replies.
- `ConnectFailuresAndReset_originalPutStillCommits` still does one Put. On timeout it throws role, term, commit, applied, leader, and the failure-path notes.
- A session is a log entry. Its index is the client id on every replica. Each later command carries `(clientId, seq, ackFloor)` and the leader clock sampled once at propose. The timestamp is inside the command bytes, so the Raft log and RPC formats stay the same size on entries that are not session commands.
- Apply keeps the last seq and the cached put or delete. The same seq does not write the map again. A seq at or below the ack floor is discarded. A session client that has not observed success resends that same command after a timeout, a step-down, or an indeterminate snapshot. A client with no session still does not retry an indeterminate proposal.
- Expiry uses the max leader timestamp applied from the log. Replicas do not read a local clock. The same timestamp is in the snapshot (PTCS version 2) and on a delta tail. Version 1 snapshots still restore.
- `PutAsync` and `GetAsync` complete a `TaskCompletionSource` from the existing apply or read path (`RunContinuationsAsynchronously`). No thread is started per request.
- One session-table lookup per session apply. Over 20,000 lookups the median was 0.165 ms, against a plain-apply median of 7.06 ms whose trial spread was about 0.4 ms. The lookup is inside that spread, so `docs/pure-improvements.md` is unchanged.
- The state-machine clock is the max leader timestamp applied. It is already stored in the PTCS version 2 snapshot tail. A new leader anchors to that clock when it is non-zero, then adds elapsed process time. A later lower tick does not move the clock backwards. The first leader in a term with no applied timestamp uses its process clock, so a shifted clock is visible in the log.
- A session command on a missing or expired session returns `SessionExpired`. A sequence above last+1 and above the ack floor returns `OutOfOrder`. Neither writes the map. `KvClient` returns null and does not ack the sequence. `SessionExpired` drops the session and registers again on the caller thread, not from the apply hook. A duplicate still returns `None` plus the cached response.
- Expiry removes the session and its cached response inside apply, after the log clock moves, and before the command is processed. A snapshot taken after that has an empty session table and the same clock. A seq at or below the ack floor is still discarded and is not one of these errors.
- The simulator runs four nodes on one thread. One splitmix seed drives delay, drop, duplicate, reorder, partition, crash/restart, and per-node clock rate. Durable log and term/vote stay. The `RaftNode` is discarded on crash. `SameSeed_reproducesTraceHash` compares trace hashes.
- After every step the simulator checks Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety, and that a live node's commit index does not move backwards. Client histories store invoke and complete with the simulator timestamp. Gets on the normal path use `LinearizableGet` and record `absent` or `unknown`. `PushSeeds` checks every seed.
- The checker is Wing-Gong per key, with a memoized bitset for overlapping calls. A failed put or an indeterminate get is a no-op. A timed-out put stays pending and may commit after its response. A cache budget or a concurrent cluster over 128 calls returns `Unknown`, and the test fails on that. `StaleLocalRead_isViolationOnSomeSeed` reads a live node's local map and must report `Violation`.
- A session client resends its inflight put. The simulator keeps that key until the attempt finishes, so a later commit is not labeled as a new key. Leader completeness ignores log marks covered by a snapshot; those marks are the pre-compact entry.
- Step count is `SIM_STEPS`, default 2000. Seed count is `SIM_COUNT`, default 200. Faults stay on for the first 80 percent of steps. Nightly sets both: 10000 seeds and 2000 steps. Lower the seed count before the step count if a run would pass about 2 minutes. A failure prints `SIM_SEED=<n> dotnet test ... --filter FullyQualifiedName~SimSeed_replaysEnvironmentSeed`.
- A crash keeps flushed log and term/vote bytes. `SimDurableLog` drops the whole unflushed suffix or tears the last unflushed record. `SimDurableState` restores the last flush. The state machine is rebuilt from the surviving snapshot and log. During faults, about one send in 40 also crashes the sender after the message is accepted.
- After the fault window, one leader must stay in office past an 80-step grace. Every live member's commit index must reach that leader's. A client put that is still open must commit or return `SessionExpired` or `OutOfOrder` within 80 steps. Live members' snapshots must match. `n4` is checked only once it is a voter or learner.
- Local 200 seeds at 2000 steps took about 8 seconds before this checker. With the checker, 240 tests took about 13 seconds. `.github/workflows/sim-nightly.yml` is manual and nightly.
- Skipping `CandidateLogUpToDate` made seed 332 fail State Machine Safety (`state machine diverged at 4`). That edit was reverted. Seed 332 passes with the real check.
- Acking AppendEntries before `Force` failed seed 1 with `append ack before flush n2 index 1`. Granting a vote before `RecordVote` failed seed 1 with `vote grant before flush n3 term 1`. Both edits were reverted and are not in this commit.
- Seed 63 elected no leader. Every voter stayed a pre-candidate. A pre-candidate refused a higher node id even when that node had the longer log, so the best log could not win. `OnPreVote` still refuses a higher id when the logs are tied. It grants when the candidate log is strictly newer. Seed 63 passes.
- `tla/Synkrolyn.tla` models PreVote, check-quorum, election, replication, current-term commit, the restart vote gate, and a lease read. Entries are terms. A mismatch drops the follower's last entry. `tla/MC.cfg` is 3 servers, term at most 2, log length at most 2. Local TLC found no error: 3,016,397 distinct states, depth 35, about 1 min 35 s. `tla/MCBug.cfg` commits a prior-term entry from a five-server figure-8 state and violates LeaderCompleteness (20 distinct states). Ack expiry after a quorum is not a separate action. Action names stay `BecomeLeader(s)`, `ClientAppend(s)`, and `Force(s)` because `TlaSpecTests` anchors on those strings.
- `docs/test-parity.md` maps each of the 297 Prytolyn tests to a Synkrolyn test, or N/A. New tests cover clock bounds, vote rules, log edges, codec and frame rejection, delta snapshots, transport guards, a five-node election, joint quorum, and a strictly newer pre-vote. Stryker.NET 5.0.0 is pinned. A wide Basic run scored 61.92% then 63.07% after those tests (7 min 44 s locally) and took 14 min 11 s on the pull-request runner. CI now mutates Kv, the in-memory log and vote, MembershipCodec, InMemoryTransport, and `RaftNode.cs` lines 1-1899. That scope scored 67.32% in 2 min 52 s locally. Break threshold is 63. Equivalent mutants are listed in that doc.

## CI

`5ccadae` push 38134006514 and pull_request 38134008733: Ubuntu pass, Windows fail `BlockedSend_completesWithin250msOfAbort`. `ConnectFailuresAndReset` passed on that Windows run.

`1c700da` push 38134324942 and pull_request 38134327957: Ubuntu and Windows pass.

`91b7aeb` push 37839924014: Ubuntu `SilentPeer` not released; Windows `ConnectFailuresAndReset` timed out "put committed". Pull request 37839929476: Ubuntu also failed `SlowReader` and `SlowButLiveReader`; Windows passed.

Phase 4 CI for `3dcb8f0` and the review push `e90f4a1` were green on Ubuntu and Windows for both push and pull_request. Phase 5 CI for `44b5884` was green on Ubuntu and Windows for both push and pull_request. The phase 5 review CI is in the previous chat reply. Phase 6 CI is in the previous chat reply. This phase's CI is recorded in the chat reply.

## Open items

- Windows put-timeout root cause was not printed. The exception text is the evidence if it fails again. Do not retry the Put.
- Repair cap for phase 9 is 3. No phase after 9 is started. The PR stays a draft.
- Benchmark findings to fix later, with tests: commit p99 tail, lease-read lock on the hit path, pre-vote retry after a grant-less round. See `docs/benchmarks.md`.

## Files

- `src/Synkrolyn/Net/SocketTransport.cs:894-900` per-transport `SendWaits`, `SendDones`, `AbortTicks`
- `src/Synkrolyn/Net/SocketTransport.cs:943-1055` `SendAll` and `WaitPending` increment those fields
- `src/Synkrolyn/Net/SocketTransport.cs:1385` `NativeCancel` (`CancelIoEx`, then shutdown)
- `tests/Synkrolyn.Tests/Net/SocketDeliveryTests.cs:376-413` `BlockedSend_counterIsPerTransport`
- `src/Synkrolyn/Kv/KvCommandCodec.cs:66-181` register, session put, tick
- `src/Synkrolyn/Kv/KvCommandError.cs:1-16` `SessionExpired` and `OutOfOrder`
- `src/Synkrolyn/Kv/InMemoryKvStore.cs:276-333` session read, count, and applied timestamp
- `src/Synkrolyn/Kv/InMemoryKvStore.cs:530-658` apply result and expiry removal
- `src/Synkrolyn/Kv/InMemoryKvStore.cs:663-779` snapshot session tail
- `src/Synkrolyn/Kv/KvClient.cs:54-190` `LastError`, register, and `AwaitCommitted`
- `src/Synkrolyn/Kv/KvClient.cs:290-430` command result, re-register off the Raft thread
- `src/Synkrolyn/Kv/FileKvStore.cs:95-103` forwards the applied timestamp
- `src/Synkrolyn/Raft/IStateMachine.cs:11-15` `AppliedTimestamp`
- `src/Synkrolyn/Raft/RaftNode.cs:675-725` leader clock offset
- `src/Synkrolyn/Raft/RaftNode.cs:1285-1290` anchor on becoming leader
- `tests/Synkrolyn.Tests/Raft/ClusterHarness.cs:25-40` optional per-node clock
- `tests/Synkrolyn.Tests/Kv/KvSessionTests.cs:113-192` skew, dropped writes, and removal
- `tests/Synkrolyn.Tests/Kv/KvSessionTests.cs:216-300` skewed-leader helper and `ShiftedClock`
- `src/Synkrolyn/Raft/RaftNode.cs:1551-1562` pre-vote grant when the candidate log is strictly newer
- `src/Synkrolyn/Raft/RaftNode.cs:2995-3004` `CandidateLogStrictlyNewer`
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulator.cs:19-48` `SimResult` depth and liveness flags
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulator.cs:61-70` `SIM_STEPS` 2000 and `SIM_COUNT` 200
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulator.cs:1536-1658` durable log, flush, drop, and tear
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulator.cs:1660-1758` durable term and vote
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulatorTests.cs:80-151` liveness, 200 seeds, and the stale-read injection
- `tests/Synkrolyn.Tests/Sim/LinearizabilityChecker.cs:34-130` per-key check and history pairing
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulator.cs:726-765` linearizable get, or a local read when `staleReads` is set
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulator.cs:474-484` inflight session put kept until it finishes
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulator.cs:1207-1228` snapshot-covered marks are not a leader-completeness miss
- `.github/workflows/sim-nightly.yml` 10k seeds at 2000 steps, manual or nightly
- `tla/Synkrolyn.tla:177-420` election, replication, commit, restart gate, and lease read
- `tla/MC.cfg` real commit rule, 3 servers, term <= 2, log length <= 2
- `tla/MCBug.cfg` prior-term commit, five-server figure 8
- `spec/README.md` action to C# method map
- `.github/workflows/ci.yml:32-69` Ubuntu TLC job, pinned tla2tools 1.8.0
- `docs/test-parity.md` one Prytolyn test per line, plus equivalent mutants
- `tests/Synkrolyn.Tests/Raft/PrytolynParityTests.cs:1-563` parity and mutant-killing tests
- `dotnet-tools.json` Stryker.NET 5.0.0
- `stryker-config.json` Basic mutations, RaftNode lines 1-1899, break 63
- `.github/workflows/ci.yml:72-83` Ubuntu Stryker job on pull_request
- `bench/Synkrolyn.Bench/BenchMain.cs` loopback throughput, latency, election, allocation
- `bench/Synkrolyn.Bench/ReadProbe.cs` ticket probe, or the baseline ack set
- `docs/benchmarks.md` side-by-side table and findings
- `.github/workflows/bench.yml` manual benchmark workflow
