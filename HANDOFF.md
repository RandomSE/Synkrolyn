# HANDOFF

## State

Branch `commit-2`. Peer liveness is inbound silence past `PeerDeadTimeout`, a socket reset, or a graceful FIN. `Send` only enqueues. AppendEntries stay coalesced. No send-progress drop and no `TCP_USER_TIMEOUT`.

## Decisions

- A pending `SendAsync` is cancelled with `CancelIoEx` on Windows and shutdown otherwise. The waiter completes the operation itself, so `Close` is not the only wake. The socket stays non-blocking. Connect finishes on the writer thread with `Poll`, not a thread-pool callback.
- `IsLive` treats a zero-byte poll and `WouldBlock` as live. Death in that probe is a disconnect socket error, or a non-blocking peek that returns 0 twice (FIN).
- `RequestVote`, `RequestVoteResponse`, and `InstallSnapshotResponse` use an unbounded channel. The 64-slot channel still drops oldest heartbeats and append replies.
- `ConnectFailuresAndReset_originalPutStillCommits` still does one Put. On timeout it throws role, term, commit, applied, leader, and the failure-path notes.

## CI

`5ccadae` push 38134006514 and pull_request 38134008733: Ubuntu pass, Windows fail `BlockedSend_completesWithin250msOfAbort` ("send never blocked on the silent reader", SocketDeliveryTests.cs:395). `ConnectFailuresAndReset` passed on that Windows run.

`1c700da` push 38134324942 and pull_request 38134327957: Ubuntu and Windows pass. That only grew the frame until `SendAsync` parked. It still relied on shutdown to finish the wait.

`91b7aeb` push 37839924014: Ubuntu `SilentPeer` not released within the peer-dead timeout (SocketDeliveryTests.cs:300); Windows `ConnectFailuresAndReset` timed out "put committed" (SocketClusterTests.cs:45). Pull request 37839929476: Ubuntu also failed `SlowReader` line 175 and `SlowButLiveReader` line 322; Windows passed.

## Open items

- Windows put-timeout root cause was not printed. The exception text is the evidence if it fails again. Do not retry the Put.
- Do not rerun jobs. Phase 4 is not started. The PR stays a draft.
