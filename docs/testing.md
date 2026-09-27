# Testing

## Clock

Unit tests advance `FakeClock`, which is compiled into the test assembly. `ClusterHarness.Advance` moves the clock, drains every mailbox, then runs CheckQuorum. `SourceConventionsTests.UnitTestsMustNotCallThreadSleep` fails if `Thread.Sleep` appears under `tests/`. `Phase3RobustnessTests.Finding16_fakeClock_isNotInTheProductionAssembly` fails if that type is linked into `Synkrolyn.dll`.

`SystemRaftClock` reads `Stopwatch` ticks. `FakeClockTests.SystemClock_movesForwardAndIgnoresListeners` checks that those ticks move forward and that `OnAdvance` is not fired.

`RaftRuntime.ForTests` keeps identity election jitter and snapshot threshold 0. `ForProduction` uses jitter in `[T, 2T]` and snapshot threshold 4096. Socket tests use `ForTests`.

## What runs where

- Log, hard state, torn tail, and checksum failures: `FileDurabilityTests` and `InMemoryRaftLogTests`.
- Election, PreVote, replication, commit, transfer completion: `RaftNodeElectionTests`, `ReplicationAndPureImprovementTests`.
- KV, ReadIndex, redirect, exactly-once: `KvClusterTests`, `ReadIndexTests`.
- WAL group commit and crash/reopen: `DurableRestartTests`.
- Snapshots and InstallSnapshot: `SnapshotClusterTests`, `KvSnapshotTests`.
- Membership and joint consensus: `MembershipChangeTests`, `JointConsensusTests`.
- In-memory chaos (partition, drop, delay, kill-mid-write reopen, divergent tails): `ChaosCampaignTests`, `InMemoryTransportTests`.
- Sockets: `SocketClusterTests` (election, put/get, reconnect, truncated frame).
- FakeClock election window: `ElectionUnavailabilityTests`.

## Sockets

`SocketClusterTests` polls with `Thread.Yield` until a deadline. That is wall-clock integration, not a FakeClock unit test. The FakeClock suite is the safety gate. `ThreeNodeTcp_electsExactlyOneLeader_andPutReplicates` and `ConnectFailuresAndReset_originalPutStillCommits` both use 120/350/600 ms. A refused connect, a peer that starts listening after the first send, or a reset connection must still deliver the frame that was already queued: `SocketDeliveryTests`.

## TLA+

`dotnet test` checks that `tla/Synkrolyn.tla` still describes the leader no-op and `forcedIndex` commit guard. TLC is optional. CI runs it when `tlc` is on `PATH` and skips it otherwise. TLC does not prove the C# implementation.
