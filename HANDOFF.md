# HANDOFF

## State

Branch `commit-2` at the socket-reliability fix on top of `91b7aeb`. Local socket tests passed 20/20. Full suite passed 204/204. CI for this commit is not in this file; the chat reply records Ubuntu and Windows push and pull_request.

Peer liveness is still inbound silence past `PeerDeadTimeout`, a send error, or FIN/RST. `Send` only enqueues. AppendEntries stay coalesced. No send-progress drop and no `TCP_USER_TIMEOUT`.

## Decisions

- A peer-dead abort shuts the socket down both ways, then disposes it, so a `SendAsync` parked on a reader that never reads is supposed to finish. `BlockedSend_completesWithin250msOfAbort` passed here in 34 ms even before that change, because `Close()` already woke the send on this kernel. CI run 37839924014 still failed `SilentPeer_doesNotHoldAFrameForever` on Ubuntu with the old `Close()` path, which is why shutdown comes first.
- `IsLive` treats a zero-byte poll and `WouldBlock` as live. Death in that probe is a disconnect socket error, or a non-blocking peek that returns 0 twice (FIN). A send that completes with `WouldBlock` waits for a writable window instead of dropping the socket.
- `RequestVote`, `RequestVoteResponse`, and `InstallSnapshotResponse` use an unbounded channel. The 64-slot channel still drops oldest heartbeats and append replies. AppendEntries stay on the single coalesced slot.
- `ConnectFailuresAndReset_originalPutStillCommits` still does one Put. On timeout it throws role, term, commit, applied, leader, and the failure-path notes. That is the one instrumentation round. It did not fail locally after the fix (20/20), so the notes were not observed.

## Windows hypothesis

Confidence: 64% | Evidence: Windows push run 37839924014 timed out at "put committed" (13 s); the same commit's pull_request Windows job passed; one local failure in a loaded 15-run loop was not captured.

Hypothesis: after `DropInbound`, a reset outbound send does not fail before `CheckQuorum`. `PeerDeadTimeout` is the election timeout, so the leader can step down while the put is still uncommitted, and a shorter log can win and truncate it. Not confirmed. If CI times out, the new exception text is the evidence. The Put is not retried.

## CI cycle 1 (`5ccadae`)

- push 38134006514: Ubuntu pass, Windows fail `BlockedSend_completesWithin250msOfAbort` ("send never blocked on the silent reader").
- pull_request 38134008733: Ubuntu pass, Windows same failure.
- `ConnectFailuresAndReset_originalPutStillCommits` passed on that Windows run, so the timeout snapshot was not printed. Root cause still unconfirmed. Confidence: 64%.

## Open items

- Windows did not park `SendAsync` for a 256 KB frame (send buffer absorbed it). The test now grows the frame until a wait starts, still under the 1 MiB cap.
- Repair cap is 3. Cycle 1 is spent. Cycle 2 is the buffer-fill test fix.

## Files

- `src/Synkrolyn/Net/SocketTransport.cs:262-274` critical enqueue
- `src/Synkrolyn/Net/SocketTransport.cs:454-465` critical dequeue
- `src/Synkrolyn/Net/SocketTransport.cs:736-780` `CloseSocket` shutdown then dispose
- `src/Synkrolyn/Net/SocketTransport.cs:930-960` failure notes
- `src/Synkrolyn/Net/SocketTransport.cs:965-1021` `SendAll`
- `src/Synkrolyn/Net/SocketTransport.cs:1023-1163` `IsLive` and `ConfirmedFin`
- `src/Synkrolyn/Net/SocketTransport.cs:1280-1284` unbounded critical channel
- `tests/Synkrolyn.Tests/Net/SocketDeliveryTests.cs:338-374` `VoteAndSnapshotAck_areNotDroppedFromAFullQueue` (failed first)
- `tests/Synkrolyn.Tests/Net/SocketDeliveryTests.cs:376-424` `BlockedSend_completesWithin250msOfAbort`
- `tests/Synkrolyn.Tests/Net/SocketClusterTests.cs:43-62` one-Put timeout snapshot
