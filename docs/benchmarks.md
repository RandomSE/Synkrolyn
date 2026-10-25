# Benchmarks

Loopback comparison of this branch with the commit-1 baseline `9ae58dd`. The harness is `bench/Synkrolyn.Bench`. It is an executable, not a test project, so `dotnet test` does not run it. `.github/workflows/bench.yml` is `workflow_dispatch` only.

## Setup

One process, three nodes, TCP on `127.0.0.1`, in-memory log and KV. `RaftRuntime.ForTests` keeps election jitter at the timeout (150 ms) and leaves the snapshot threshold at 0. Heartbeat is 40 ms. The current tree's default clock-drift bound is 0.01. The baseline has no such field. The harness does not call `SetClockDriftBound` or `PeerDeadTimeout` (the current socket default is already 150 ms).

Each number below is from one Release process per commit, run back to back. A repeat of the same commands is cited in the notes. Commands:

```text
dotnet run --project bench/Synkrolyn.Bench/Synkrolyn.Bench.csproj --configuration Release -- --label commit-2 --out bench-now.txt
```

Baseline: `git worktree add --detach /tmp/synkrolyn-9ae58dd 9ae58dd`, copy `bench/Synkrolyn.Bench` into that tree, and run the same command with `--label 9ae58dd`.

Defaults: 500 sequential commits, 5 pipelined trials of 2000 puts at depth 32, 500 lease reads, 200 ReadIndex rounds, 15 leader-loss trials (one discarded warmup). Allocation is `GC.GetTotalAllocatedBytes(true)` over a batch of 80 ops, median of 5 trials, divided by the batch. That count includes raft and socket threads.

## Machine

| Item | Value |
| --- | --- |
| Host | Linux 6.12.94+, Ubuntu 24.04.5, KVM |
| CPU | 4 vCPU, Intel Xeon family 6 model 173 (L3 480 MiB reported by the guest) |
| Memory | 16 GiB, about 11 GiB in use, no swap |
| SDK | .NET SDK 10.0.402 |
| Runtime | .NET 10.0.13, x64, workstation GC |

## Results

Positive change means this branch is higher. For latency, election time, and bytes, higher is worse. For throughput, higher is better. A finding is a worse result of more than 5 percent whose gap also exceeds the larger within-run interquartile range, on both runs. The table is the second paired run after the phase 9b fixes (2026-10-14).

| Metric | 9ae58dd | commit-2 | Change | Note |
| --- | --- | --- | --- | --- |
| Write throughput p50 (ops/s) | 1487.2 | 5776.7 | +288% | Faster. Repeat 1237.6 vs 7089.5. The current IQR is wide (3748 and 9847). The low half still sits above the baseline. |
| Commit latency p50 (us) | 237.9 | 170.0 | -29% | Repeat 203.2 vs 200.3. Inside the ~100 to 200 us IQR. |
| Commit latency p99 (us) | 13491 | 5213 | -61% | Was the phase 9 finding (about 28 to 34 ms vs 3.5 to 6 ms). Repeat today 26478 vs 10966. Both runs are faster than the same-day baseline. A single sample can still reach about one heartbeat (max 30 ms and 37 ms). |
| Lease read p50 (us) | 0.165 | 0.121 | -27% | Was about 0.37 us. Repeat 0.165 vs 0.119. IQR is 0.001 us. |
| Lease read p99 (us) | 0.195 | 0.296 | +52% | Repeat was the other way: 0.215 vs 0.202. This run's max is 17 us, so one outlier moves p99. Not stable across the two runs. |
| ReadIndex p50 (us) | 41.6 | 78.0 | +88% | Not stable. The other run was 51.4 vs 52.1. |
| ReadIndex p99 (us) | 112.0 | 35325 | large | Not called. The other run was 7950 vs 124. Both trees move by milliseconds between runs. |
| Election p50 (ms) | 112.1 | 149.9 | +34% | Repeat 114.3 vs 150.6. Min is about 111 ms on both. Left in place: a follower gates votes for the leader's full election timeout, not two heartbeats. |
| Election p99 (ms) | 153.6 | 152.1 | -1.0% | Was about 245 to 279 ms. Repeat 155.0 vs 155.5. Max is 152 ms and 156 ms. |
| Alloc per pipelined write (bytes) | 213363 | 32307 | -85% | Fewer bytes. Repeat 336522 vs 25266. |
| Alloc per lease read (bytes) | 3.6 | 9.2 | noisy | Repeat was 0 vs 0. A few bytes on one trial. |
| Alloc per ReadIndex (bytes) | 12247 | 7580.7 | -38% | Was 41834 before tickets were reused. Review repeat 7297.4. Same-day baseline 13303.2 and 10213.5. |

ReadIndex completion is a quorum ack (this node plus one peer) after `BeginReadIndex`, with the index applied. This branch times `ReadIndexSatisfied(long)`. The baseline's parameterless `ReadIndexSatisfied` returns immediately while the lease is valid, so the harness reads `_readIndexAcks` under `_eventLock` instead. Both probes wait for one peer ack.

Election time is from `RaftRuntime.Dispose` on the leader until another node is leader at a higher term. The leader is stopped only after one committed put and an 80 ms settle. `9ae58dd` gates a pre-vote for two heartbeats (80 ms), so the faster follower can win near 112 ms. This tree gates for the leader's election timeout (150 ms), the same window as the read lease. A follower that heard the leader just before the stop must not vote while that lease can still be valid. Shortening the gate would reopen that hole, so election p50 stays at the timeout.

ReadIndex bytes on 2026-10-21 used the same alloc batch (5 trials of 80) with the other loops set to 1. The byte count is `GC.GetTotalAllocatedBytes` around that batch only. `9ae58dd` was 13303.2 then 10213.5. This tree was 7580.7 then 7297.4. `ReadIndexSatisfied(long)` no longer allocates a closure per poll, and the ticket's dictionary and ack set are reused.

## Phase 9 findings after the fixes

Phase 9 called three findings. Each now has a failing test and a fix. Same-day pairs put all three inside the rule above, or better than `9ae58dd`.

**Commit p99.** The writer used to notice the next append only after `WaitPending` returned. `ChainNext` takes `PendingAppend` from the send-completion callback and starts the next `SendAsync` before that wait returns. The 20 ms slice remains the silence-abort poll. `Propose` still only enqueues. `NextAppend_isFlushedByTheSendCompletionCallback` failed first (the second append was not readable while the writer was held), then passed.

**Lease-read latency.** `ReadIndexSatisfied()` reads a published expiry with no `_eventLock`. The raft thread writes that expiry at the end of `Drain`, on step-down, and on the other paths that drop the lease. `LinearizableGet` checks it before `Role`. `HotLeaseRead_returnsWhileTheRaftLockIsHeld` failed first (the read blocked for 200 ms). `LeaseRead_dropsWhenTheClockPassesThePublishedExpiry` and `LeaseRead_dropsOnStepDownBeforeTheExpiry` cover the drop.

**Election p99.** A pre-vote that can no longer reach quorum, ignoring a missing grant from the leader this round already lost, clears `_preCandidate` and arms the next try for the remaining advertised lease (or 1 ms when that gate is already open). `VoteBlockedByLeaderContact` is unchanged. `GrantlessPreVote_retriesWhenTheAdvertisedLeaseEnds` failed first (no leader 50 ms after the gate), then passed, and it still requires the in-lease denial.

## What did not regress

Write throughput stays higher. Commit p50 stays inside noise. Election p99 matches the baseline cap of about one timeout. Bytes per pipelined write stay lower.

Phase 14 added one volatile floor check on `ReadIndexSatisfied()`. Two quiet runs of 500 lease reads, with the other loops shortened, were 0.130 us and 0.151 us. Both sit under the published `9ae58dd` lease p50 of 0.165 us. The table above is unchanged.

Phase 15 fsyncs the log directory inside `FileRaftLog.Force`. The harness still prints `log=in-memory`, so that fsync is not on the measured path. Same-day write runs (500 commits, 5 trials of 2000 pipelined puts, alloc batch unchanged, other loops set to 1): head throughput p50 7060 then 7877 ops/s, pre-phase-15 `f9ad895` 6381 then 7085, commit-1 `9ae58dd` 2589. Commit p50 was 170 then 126 us versus 137 then 118 us and 164 us. Both head-versus-pre gaps sit inside the larger write IQR (61 us and 49 us). Alloc per write was 35340 then 20585 bytes versus 16779 then 44729 and 135408. Throughput stayed higher. The directory fsync was left on every force.
