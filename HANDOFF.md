# HANDOFF

## State

Branch `commit-2`, rebased on `dcae567` (phase 3 send cancel). Phases 1-3 are accepted. This push is phase 4 client sessions, plus a test that the blocked-send counters cannot leak across transports. Local results are in the chat reply. The PR stays a draft.

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

## CI

`5ccadae` push 38134006514 and pull_request 38134008733: Ubuntu pass, Windows fail `BlockedSend_completesWithin250msOfAbort`. `ConnectFailuresAndReset` passed on that Windows run.

`1c700da` push 38134324942 and pull_request 38134327957: Ubuntu and Windows pass.

`91b7aeb` push 37839924014: Ubuntu `SilentPeer` not released; Windows `ConnectFailuresAndReset` timed out "put committed". Pull request 37839929476: Ubuntu also failed `SlowReader` and `SlowButLiveReader`; Windows passed.

Phase 4 CI for this push is recorded in the chat reply.

## Open items

- Windows put-timeout root cause was not printed. The exception text is the evidence if it fails again. Do not retry the Put.
- Repair cap for phase 4 is 3. This is the first push. No phase after 4 is started. The PR stays a draft.

## Files

- `src/Synkrolyn/Net/SocketTransport.cs:894-900` per-transport `SendWaits`, `SendDones`, `AbortTicks`
- `src/Synkrolyn/Net/SocketTransport.cs:943-1055` `SendAll` and `WaitPending` increment those fields
- `src/Synkrolyn/Net/SocketTransport.cs:1385` `NativeCancel` (`CancelIoEx`, then shutdown)
- `tests/Synkrolyn.Tests/Net/SocketDeliveryTests.cs:376-413` `BlockedSend_counterIsPerTransport`
- `src/Synkrolyn/Kv/KvCommandCodec.cs:66-181` register, session put, tick
- `src/Synkrolyn/Kv/InMemoryKvStore.cs:276-323` session read
- `src/Synkrolyn/Kv/InMemoryKvStore.cs:501-590` `ApplySession`
- `src/Synkrolyn/Kv/InMemoryKvStore.cs:603-719` snapshot session tail
- `src/Synkrolyn/Kv/KvClient.cs:54-160` register, follow, `PutAsync`
- `src/Synkrolyn/Kv/KvClient.cs:318-410` retry the same seq, including indeterminate
- `src/Synkrolyn/Raft/RaftNode.cs:673-690` `LeaderMillis`
- `src/Synkrolyn/Raft/RaftNode.cs:701-706` apply waiter callback
- `tests/Synkrolyn.Tests/Kv/KvSessionTests.cs:13-132` the four session tests
