# TLA+

`tla/Synkrolyn.tla` is a bounded model of election and log replication. TLC, if you run it, checks that model. It does not prove the C# implementation, and it does not replace the xUnit suite.

## Modeled

- `currentTerm`, `votedFor`, `log` (a sequence of terms), `commitIndex`, `role`, `votesGranted`, `nextIndex`, `matchIndex`.
- `forcedIndex`: the prefix known durable. `Force` sets it to `Len(log)`. `ClientAppend` and `BecomeLeader` do not.
- `CanCommit` requires the entry's term to be the leader's current term, a majority storing that prefix, and `forcedIndex[s] >= n` when the leader is the server being counted.
- A successful AppendEntries sets the follower's `forcedIndex` to the new log length, matching ack-after-fsync.
- `BecomeLeader` appends `currentTerm[s]` when the log still has room (leader no-op).
- RequestVote uses the section 5.4.1 last-term / last-index check.
- PrevLog mismatch uses `FastBackupNext` (XLen, XTerm, XIndex) and does not increase `nextIndex`.

Invariants: `TypeOK`, `ElectionSafety`, `LogMatching`, `CommittedPrefixSafety`. Property: `LeaderAppendOnlyProperty`.

## Not modeled

Membership, learners, joint consensus, snapshots, InstallSnapshot, sockets, the file format, ReadIndex, leases, KV bytes, and leadership-transfer completion. Dropping an RPC is represented only by not taking that action. Chaos campaigns and the election-window measurement stay in the C# tests.

## How to run

From the repository root, if `tlc` is on `PATH`:

```text
tlc -config tla/MC.cfg tla/Synkrolyn.tla
```

Bounds in `tla/MC.cfg`: servers `{n1, n2, n3}`, `MaxTerm = 3`, `MaxLogLen = 3`, `Majority = 2`, deadlock checking off.

CI (`.github/workflows/ci.yml`) runs that command on Linux when `tlc` is installed and prints a skip line otherwise. A missing toolbox does not fail `dotnet test`.

`TlaSpecTests` only checks that the spec text still contains the leader no-op append and the `forcedIndex` commit guard.
