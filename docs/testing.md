# Testing

## Clock

Unit tests advance `FakeClock`. `ClusterHarness.Advance` moves the clock, drains every mailbox, then runs CheckQuorum. `SourceConventionsTests.UnitTestsMustNotCallThreadSleep` fails if `Thread.Sleep` appears under `tests/`.

`SystemRaftClock` reads `Stopwatch` ticks. It does not read `DateTime`. `FakeClockTests` scans `src/Synkrolyn` for `DateTime.Now` and `DateTime.UtcNow`.

`RaftRuntime.ForTests` keeps identity election jitter and snapshot threshold 0. `ForProduction` uses jitter in `[T, 2T]` and snapshot threshold 4096. Socket tests use `ForTests`.

## What runs where

- Log, hard state, torn tail, and checksum failures: `FileDurabilityTests` and `InMemoryRaftLogTests`.
- Election, PreVote, replication, commit, transfer completion: `RaftNodeElectionTests`, `ReplicationAndPureImprovementTests`.
- KV, ReadIndex, redirect, exactly-once: `KvClusterTests`, `ReadIndexTests`.
- WAL group commit and crash/reopen: `DurableRestartTests`.
- Snapshots and InstallSnapshot: `SnapshotClusterTests`, `KvSnapshotTests`.
- Membership and joint consensus: `MembershipChangeTests`, `JointConsensusTests`.
- In-memory chaos (partition, drop, delay, kill-mid-write reopen, divergent tails): `ChaosCampaignTests`, `InMemoryTransportTests`.
- Sockets: `SocketClusterTests` (election, put/get, reconnect, truncated frame, election-window trials).
- FakeClock election window: `ElectionUnavailabilityTests`. The socket trials assert that N trials finished. They do not fail on a median.

## Sockets

`SocketClusterTests` polls with `Thread.Yield` until a deadline. That is wall-clock integration, not a FakeClock unit test. If a socket test fails once, rerun it. Do not treat one pass or one fail as the only evidence. The FakeClock suite is the safety gate.

## TLA+

`dotnet test` checks that `tla/Synkrolyn.tla` still describes the leader no-op and `forcedIndex` commit guard. TLC is optional. CI runs it when `tlc` is on `PATH` and skips it otherwise. TLC does not prove the C# implementation.
