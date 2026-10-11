# HANDOFF

## State

Branch `commit-2`, rebased on `dcae567` (phase 3 send cancel). Phases 1-4 are accepted. This push is phase 5: a seeded simulator in the test project. It does not change production code. Local results are in the chat reply. The PR stays a draft.

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
- After every step the simulator checks Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety, and that a live node's commit index does not move backwards. Client histories store invoke and complete with the simulator timestamp. There is no linearizability checker in this phase.
- Faults stop at two thirds of the steps. Later steps advance a healed network. A leader and a committed session put are required by the end. `PushSeeds_holdInvariants` runs `SIM_COUNT` seeds, or 200 when unset. A failure prints `SIM_SEED=<n> dotnet test ... --filter FullyQualifiedName~SimSeed_replaysEnvironmentSeed`.
- Local 200 seeds took 354 ms. `.github/workflows/sim-nightly.yml` is manual and nightly and sets `SIM_COUNT=10000`.
- Skipping `CandidateLogUpToDate` made seed 332 fail State Machine Safety (`state machine diverged at 4`). That edit was reverted. Seed 332 passes with the real check. The 200 push seeds did not schedule that election. The nightly run includes seed 332. No production bug was found.

## CI

`5ccadae` push 38134006514 and pull_request 38134008733: Ubuntu pass, Windows fail `BlockedSend_completesWithin250msOfAbort`. `ConnectFailuresAndReset` passed on that Windows run.

`1c700da` push 38134324942 and pull_request 38134327957: Ubuntu and Windows pass.

`91b7aeb` push 37839924014: Ubuntu `SilentPeer` not released; Windows `ConnectFailuresAndReset` timed out "put committed". Pull request 37839929476: Ubuntu also failed `SlowReader` and `SlowButLiveReader`; Windows passed.

Phase 4 CI for `3dcb8f0` and the review push `e90f4a1` were green on Ubuntu and Windows for both push and pull_request. Phase 5 CI is recorded in the chat reply.

## Open items

- Windows put-timeout root cause was not printed. The exception text is the evidence if it fails again. Do not retry the Put.
- Repair cap for phase 5 is 3. This is the first push. No phase after 5 is started. The PR stays a draft.

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
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulator.cs:11-36` client history and `SimResult`
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulator.cs:42-123` seeded run and healed liveness steps
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulator.cs:492-760` invariant checks after every step
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulator.cs:830-938` simulated network
- `tests/Synkrolyn.Tests/Sim/DeterministicSimulatorTests.cs:7-70` trace hash, history, liveness, 200 seeds
- `.github/workflows/sim-nightly.yml` 10k seeds, manual or nightly
