# Synkrolyn

Synkrolyn is a from-scratch C# Raft key-value store. It ports the feature set of [Prytolyn](https://github.com/RandomSE/Prytolyn) (`master`, commit `0c27275`) into idiomatic .NET, and it tightens a few Raft rules that do not trade away latency, throughput, or availability.

The implementation is tested. It is not formally proven. The TLA+ model in `tla/` checks a small 3-server abstraction. It does not prove `RaftNode`.

## How it compares

Classic Raft (Ongaro and Ousterhout, 2014) is the core: follower, candidate, and leader; RequestVote with the section 5.4.1 log check, including an empty suffix after compaction; AppendEntries with prevLog matching, conflict truncation, and an append-only leader; the section 5.4.2 current-term commit rule; and a leader no-op on becoming leader.

Prytolyn, and therefore Synkrolyn, also includes the usual production additions that stay on the paper's safety side: PreVote, CheckQuorum, fast-backup hints (XLen, XTerm, XIndex), bounded AppendEntries batches, chunked InstallSnapshot, group-commit WAL, ReadIndex, a send-time lease, leadership transfer, learners, optional joint consensus, and a range-sharded client.

Synkrolyn differs from Prytolyn where a stricter rule does not cost a Raft property or a latency path. The leader still sends AppendEntries in parallel with its own fsync (paper section 10.2.1). It counts itself in the commit quorum only after that fsync. Leases, CheckQuorum, and election timers read a monotonic clock. Leadership transfer reports success only after the target is observed as leader. Details are in `docs/pure-improvements.md`.

## Layout

- `src/Synkrolyn`: library. Namespaces `Synkrolyn.Raft`, `Synkrolyn.Kv`, `Synkrolyn.Net`.
- `src/Synkrolyn.Host`: localhost 3-node console that puts `hello=world` and reads it back.
- `tests/Synkrolyn.Tests`: xUnit.
- `tla/Synkrolyn.tla`: bounded safety model.

## Build and test

.NET 10 SDK (this tree uses the 10.0.100 roll-forward in `global.json`).

```text
dotnet build Synkrolyn.slnx
dotnet test Synkrolyn.slnx
```

Warnings are errors. Unit tests drive `FakeClock` and do not call `Thread.Sleep`.

## Host

```text
dotnet run --project src/Synkrolyn.Host
```

The host binds three loopback nodes, waits for one leader, puts `hello` to `world`, and prints `hello=world`. Exit code 0 means the local read saw that value. `--help` prints usage and does not start a cluster.

## Docs

- `docs/raft-safety.md`: property to test mapping
- `docs/pure-improvements.md`: stricter rules and the tests that cover them
- `docs/testing.md`: clock, chaos, and socket tests
- `docs/membership.md`: learners, single-server changes, joint consensus
- `docs/tla.md`: what the model covers and how to run TLC
