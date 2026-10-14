# Benchmarks

Loopback comparison of this branch with the commit-1 baseline `9ae58dd`. The harness is `bench/Synkrolyn.Bench`. It is an executable, not a test project, so `dotnet test` does not run it. `.github/workflows/bench.yml` is `workflow_dispatch` only.

## Setup

One process, three nodes, TCP on `127.0.0.1`, in-memory log and KV. `RaftRuntime.ForTests` keeps election jitter at the timeout (150 ms) and leaves the snapshot threshold at 0. Heartbeat is 40 ms. The current tree's default clock-drift bound is 0.01. The baseline has no such field. The harness does not call `SetClockDriftBound` or `PeerDeadTimeout` (the current socket default is already 150 ms).

Each number below is from one Release process per commit, run back to back. A repeat of the same commands reproduced the findings called out below. Commands:

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

Positive change means this branch is higher. For latency, election time, and bytes, higher is worse. For throughput, higher is better. A finding is a worse result of more than 5 percent whose gap also exceeds the larger within-run interquartile range, on both runs. The table is the second paired run (2026-10-14).

| Metric | 9ae58dd | commit-2 | Change | Note |
| --- | --- | --- | --- | --- |
| Write throughput p50 (ops/s) | 1397.8 | 9112.3 | +552% | Faster. This branch's IQR is 6709 against the baseline's 430, so the speedup factor is noisy, and the low half of the trials still sits above the baseline. |
| Commit latency p50 (us) | 172.6 | 181.0 | +4.9% | Inside 5 percent and inside the ~100 us IQR. |
| Commit latency p99 (us) | 3512 | 34268 | +876% | Finding. The repeat was 6251 us vs 28265 us. p50 did not move. The slow samples sit near one heartbeat (max 39 ms). |
| Lease read p50 (us) | 0.166 | 0.368 | +122% | Finding. Repeat 0.165 vs 0.312. Gap is about 0.2 us, IQR is under 0.04 us. |
| Lease read p99 (us) | 0.234 | 0.507 | +117% | Finding. Same shape as p50. |
| ReadIndex p50 (us) | 58.7 | 46.6 | -21% | Not stable. The other run was 56.9 vs 64.0. Treat p50 as unchanged. |
| ReadIndex p99 (us) | 452.0 | 1066.5 | +136% | Not called. The baseline's own p99 moved from 155 us to 452 us between runs. |
| Election p50 (ms) | 150.2 | 150.7 | +0.3% | Same one-timeout detection. |
| Election p99 (ms) | 152.6 | 279.2 | +83% | Finding. Repeat 151.8 vs 245.5. Baseline max stays about 152 ms. This branch's max is 260 ms and 300 ms. |
| Alloc per pipelined write (bytes) | 195394 | 24676 | -87% | Fewer bytes. The baseline trial spread is wide (the other run was 110574). |
| Alloc per lease read (bytes) | 0 | 0 | 0 | The hit path did not move the process counter over the batch. |
| Alloc per ReadIndex (bytes) | 11901 | 30617 | +157% | More bytes on the ticket round. Not in the latency, throughput, or availability gate. The repeat was 12052 vs 25357. |

ReadIndex completion is a quorum ack (this node plus one peer) after `BeginReadIndex`, with the index applied. This branch times `ReadIndexSatisfied(long)`. The baseline's parameterless `ReadIndexSatisfied` returns immediately while the lease is valid, so the harness reads `_readIndexAcks` under `_eventLock` instead. Both probes wait for one peer ack.

Election time is from `RaftRuntime.Dispose` on the leader until another node is leader at a higher term. The leader is stopped only after one committed put and an 80 ms settle.

## Findings

No fix is in this change. Each one needs a test that fails on the current tree and passes once the tail is gone, and none of them is a one-line correction.

**Commit p99.** Sequential commits are still about 180 us at p50, and pipelined throughput is higher, but about 1 percent of commits take 30 to 40 ms. `Propose` already calls `ReplicateToAll`, and `SocketTransport.Send` only stores `PendingAppend` and pokes the writer. The writer waits on an in-flight socket send in 20 ms slices (`WaitPending`) and parks 5 ms when idle. A commit that arrives while that wait is in progress is not flushed until the slice ends, which lines up with a tail just under the 40 ms heartbeat. A pure fix is to flush `PendingAppend` from the send callback as soon as the previous socket operation completes, and to keep the 20 ms slice only for the silence abort. The raft thread still must not block on a full window.

**Lease-read latency.** `LinearizableGet` on a hot lease is about twice the baseline (0.17 us to 0.37 us). The body is a lock inside `ReadIndexSatisfied` plus the session check in `Get`, on top of the immutable map. A pure fix is a lease flag published by the raft thread so the hit path reads the map without taking `_eventLock`. The flag still has to drop when the lease ends or the node steps down, which is the part that needs a test. Absolute cost is a fraction of a microsecond. It is still past the 5 percent line and past the IQR.

**Election p99.** Median failover stays one 150 ms timeout. The slow trials take about two timeouts. `BeginPreVote` arms a fresh full timeout immediately. A pre-vote that gets no grant (the other follower is still inside `VoteBlockedByLeaderContact`, or the logs are tied and the higher id refuses) does not retry until that new timeout. The baseline has no lease gate, and its slowest trial stays at about 152 ms. A pure fix is to retry a grant-less pre-vote when the remaining advertised lease ends, instead of waiting a second full timeout. The gate itself stays, so a follower still cannot vote inside the leader's lease.

## What did not regress

Write throughput is higher. Commit p50, ReadIndex p50, and election p50 stay inside the noise band defined above. Bytes per pipelined write are lower. Lease-read bytes stayed at zero.
