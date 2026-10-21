# Thesis conformance

Diego Ongaro, *Consensus: Bridging Theory and Practice* (Stanford, 2014). Section titles below are that dissertation's. The extended Raft paper numbers some of the same rules differently: paper 5.4.1 is thesis 3.6.1, and paper 5.4.2 is thesis 3.6.2.

Status is `implemented`, `extended` (the rule is present, and this tree adds a stricter or extra behavior), `not applicable`, or `gap`. A unit test is a scenario, not a proof of `RaftNode`. The simulator (`DeterministicSimulatorTests.PushSeeds_holdInvariants`, 200 seeds) checks Election Safety, Leader Append-Only, Log Matching, Leader Completeness, and State Machine Safety, and `LinearizabilityChecker` checks client histories. TLC checks `tla/Synkrolyn.tla`. Stryker mutates Kv, sessions, and all of `RaftNode` in two CI jobs (breaks 62 and 68); see `docs/test-parity.md`. Loopback measurements are in `docs/benchmarks.md`.

## Chapter 3: Basic Raft

| Section | Status | Code | Evidence |
| --- | --- | --- | --- |
| 3.1 Understandability | not applicable | Design goal, not a runtime rule. | Chapter 7 is the user study. Not run here. |
| 3.2 Overview, 3.3 Basics | implemented | `RaftNode` roles, terms, `OnTick` | `RaftNodeElectionTests.ThreeNode_shortestTimeoutWins_exactlyOneLeader`; `ClusterHarness.AssertAtMostOneLeaderPerTerm`; TLA `ElectionSafety` |
| 3.4 Leader election | implemented | `BeginElection`, `OnRequestVote`, `ResetElectionDeadline`. `RaftRuntime.ProductionJitter` draws `[T, 2T]`. Tests pin identity jitter. | `SplitVote_thenLaterTimeoutElectsOneLeader`; `HeartbeatsPreventSecondElection`; `PushSeeds_holdInvariants` |
| 3.5 Log replication | implemented | `Propose`, `ReplicateToAll`, `OnAppendEntries`, `MaybeAdvanceCommit` | `ReplicationAndPureImprovementTests.ThreeNode_propose_commitsOnMajority_andThirdCatchesUp` |
| 3.5 Conflict index (optional) | extended | `FastBackupNext` (XTerm, XIndex, XLen). The thesis also allows one-step `nextIndex` decrement. | `FastBackupRepairTests` |
| 3.6.1 Election restriction | implemented | `CandidateLogUpToDate` | `Figure8_oldTermMajority_doesNotCommitUntilCurrentTermEntry`; TLA RequestVote last-term / last-index check |
| 3.6.2 Commit current-term entries only | implemented | `CanCommitIndex`. `BecomeLeader` appends a no-op. | Same Figure 8 test; TLA `CanCommit`; `Leader_doesNotCountItselfUntilForce` (self counts only after `DurableIndex`) |
| 3.6.3 Safety argument | implemented | Properties, not a separate method. | TLA `ElectionSafety`, `LogMatching`, `LeaderCompleteness`, `StateMachineSafety`, `LeaderAppendOnlyProperty`; `PushSeeds_holdInvariants`; `KvClusterTests.StateMachineSafety_sameIndexSameKvEffect` |
| 3.7 Follower and candidate crashes | implemented | AppendEntries and RequestVote are retried. Acks wait for `Force`. | `DurableRestartTests.CrashBeforeForce_waiterNotApplied_reopenDropsEntry` |
| 3.8 Persisted state and restart | implemented | `FilePersistentState.SetCurrentTerm`, `RecordVote`; `FileRaftLog.Force` | `RestartAllThree_logsKeepCommitted_newTermProposeReappliesKv`; `DeterministicSimulatorTests.Crash_losesDeferredVote_andKeepsFlushedTerm`. The simulator fails a grant that is sent before the vote is flushed. |
| 3.9 Timing and availability | implemented | Heartbeats from `OnTick`. Election timeout is the caller's. | `HeartbeatsPreventSecondElection`; `ElectionUnavailabilityTests.IsolateLeader_successorCommitsNewPut_windowWithinPaperBound`; election rows in `docs/benchmarks.md` |
| 3.10 Leadership transfer | extended | `TransferLeadership`, `OnTimeoutNow`. The leader stops new proposals only after `TimeoutNow`, stays leader until the target is observed or the deadline aborts, and does not step down at send time. | `Phase2ReadElectionTests.Finding17_transfer_keepsLeaderUntilTargetWinsThenResumesOnTimeout`; `Finding17_timeoutNowHigherTerm_campaignsAtTermPlusOne` |

## Chapter 4: Membership

Both modes in `docs/membership.md` are present. Single-server changes take effect when the log entry is applied. Joint consensus takes effect when the entry is appended.

| Section | Status | Code | Evidence |
| --- | --- | --- | --- |
| 4.1 Safety (one server at a time) | implemented | `AddLearner`, `PromoteVoter`, `RemoveServer` | `MembershipChangeTests.AddLearner_doesNotChangeMajority_untilPromotedAfterCatchUp`; `RemoveVoter_oldNodeNoLongerRequiredForCommit`; `RemoveVoterFromTwo_returnsEmpty_learnerRemoveReturnsIndex` |
| 4.2.1 Catching up new servers | implemented | A learner does not vote. `PromoteVoter` waits until `matchIndex` reaches `commitIndex`. | `PromoteVoter_refusedWhileEmptyLearnerIsBehind`; `EmptyLearner_cannotWinElection` |
| 4.2.2 Removing the current leader | implemented | `MaybeStepDownOutsideCommittedConfig` | `ThesisSafetyTests.Finding9_nonVoterLeaderDoesNotCountItself_andStepsDownAfterCnewCommits` |
| 4.2.3 Disruptive servers | implemented | `VoteBlockedByLeaderContact` (no vote inside the advertised lease). `MaybeCheckQuorum` steps a leader down without a majority of recent append acks. `BecomeLeader` seeds that ack map and not the read-lease map. | `Phase123ReviewTests.MixedTimeout_fastVoter_doesNotVoteInsideTheLeadersLease`; `SafetyRepairTests.LeaseFalseBeforeAppendAck_checkQuorumGraceKeepsLeader`; TLA `CheckQuorum` |
| 4.3 Joint consensus | implemented | `EnterJoint`, `LeaveJoint`. Commit and votes need a majority of Cold and of Cnew. | `JointConsensusTests.EnterJoint_leaderUsesJointQuorumBeforeCommit`; `TruncateUncommittedJoint_revertsToColdQuorum`; `HoldJointFalse_autoAppendsCnewAfterJointCommits` |
| 4.4 System integration | implemented | Config is in the snapshot header and in `config.bin`. | `MembershipChangeTests.SnapshotAndHardState_persistConfig_restoreOnReplace`; `JointConsensusTests.InstallSnapshot_midJointOntoBlankNode_restoresColdAndCnew` |

## Chapter 5: Log compaction

| Section | Status | Code | Evidence |
| --- | --- | --- | --- |
| 5.1 Memory-based snapshots | implemented | `SnapshotThroughLocked`, `OnInstallSnapshot`. The KV map is in memory. | `SnapshotClusterTests.ThreeNode_leaderSnapshot_compactedIndexesUnreadable_laterPutStillServes` |
| 5.1.1 Snapshot concurrently | implemented | The snapshot executor encodes and fsyncs. The Raft thread renames and swaps. | `Phase123ReviewTests.Item13_snapshotCapture_doesNotBlockDrain` |
| 5.1.2 When to snapshot | implemented | `SnapshotThreshold`. `RaftRuntime` sets 4096 in production and leaves tests at 0. | `PrytolynParityTests.ProductionStart_setsSnapshotThreshold_forTestsStaysZero` |
| 5.1.3 InstallSnapshot chunks | implemented | Chunked `InstallSnapshot`, resumed from the last acked offset. The thesis figure already splits the snapshot into chunks. | `Phase3RobustnessTests.InstallSnapshot_droppedConnection_resumesFromLastAckedOffset`; `InstallSnapshot_resentOffsetZero_doesNotRestart` |
| 5.2 Disk-based state machines | not applicable | `FileKvStore` still applies in memory and writes the map on snapshot, close, and restore. It is not a state machine whose own on-disk layout is the snapshot. | `KvSnapshotTests.DiskKv_survivesReopen` |
| 5.3 Log cleaning and LSM | not applicable | Alternative to 5.1. Not used. Compaction is a snapshot plus an optional KV delta (see Beyond). | |
| 5.4 Leader-based snapshot-in-the-log | not applicable | Alternative to 5.1. Not used. | |

## Chapter 6: Client interaction

Section 6.3 is linearizable semantics, and sessions are the mechanism in that section. Section 6.4 is read-only queries. Clock leases are 6.4.1.

| Section | Status | Code | Evidence |
| --- | --- | --- | --- |
| 6.1 Finding the cluster | not applicable | Callers pass node ids. No broadcast or DNS discovery. The host binds three known loopback addresses. | `src/Synkrolyn.Host/Program.cs` |
| 6.2 Routing to the leader | implemented | `KvClient.LeaderHint`. `RedirectingKvClient` retries a put, delete, or linearizable get once on that id. | `KvClusterTests.FollowerPut_redirectsViaLeaderHint` |
| 6.3 Linearizable commands and sessions | implemented | `KvClient.Register` (the applied index is the client id). `KvCommandCodec.EncodeSessionPut`. `InMemoryKvStore.Apply` keeps the latest serial and response. `ExpireSessions` uses the leader timestamp from the log. A missing session returns `SessionExpired` and is not applied. | `KvClusterTests.DuplicatePutSameClientSerial_appliedOnce`; `KvSessionTests.ExpiredUnknownAndOutOfOrder_areNotReportedAsApplied`; `KvSnapshotTests.ClientSerialsSurviveSnapshotRestore_duplicatePutDoesNotChangeMap` |
| 6.4 Read-only queries | implemented | `BeginReadIndex`, `ReadIndexSatisfied(long)`. A read waits for a quorum heartbeat round from the current term, then reads the applied map. Followers do not answer `LinearizableGet`. | `ReadIndexTests.ThreeNode_putThenLinearizableGetOnLeader`; `FollowerLinearizableGet_emptyWhileBoundedStaleGetHits`; `IsolatedLeader_withoutNewElection_cannotCompleteReadIndex` |
| 6.4.1 Clock leases | implemented | `LeaderLeaseMillis` is the election timeout times `(1 - ρ)`. `SetClockDriftBound`, default `ρ = 0.01`. `ReadIndexSatisfied()` serves a read while the published expiry holds, and the expiry is cleared on step-down. Followers still gate votes on the full advertised bound (4.2.3). | `Phase3SafetyRepairTests.DefaultClockDriftBound_shrinksTheLeaderLease`; `SkewedFollowerClock_leaderLeaseShrinksByDriftBound`; `LeaseReadTests.HotLeaseRead_returnsWhileTheRaftLockIsHeld`; `LeaseRead_dropsWhenTheClockPassesThePublishedExpiry`; `LeaseRead_dropsOnStepDownBeforeTheExpiry`. `PushSeeds_holdInvariants` runs `LinearizabilityChecker` on lease and ReadIndex gets. |

## Chapter 8 and Chapter 9

| Section | Status | Code | Evidence |
| --- | --- | --- | --- |
| 8.1 Formal specification | gap | `tla/Synkrolyn.tla` is a bounded 3-server model, not a proof of `RaftNode`. | TLC on `tla/MC.cfg`. `TlaSpecTests` checks that the spec text still contains the leader no-op and the `forcedIndex` guard. `tla/MCBug.cfg` is a negative check. See Gaps. |
| 9.1-9.5 Election experiments | not applicable | The AvailSim latency study is not re-run. | This tree measures leader-loss time on loopback. See `docs/benchmarks.md`. |
| 9.6 PreVote | implemented | `BeginPreVote`, `OnPreVote`. The term increments only after a majority of pre-vote grants. A pre-vote inside the leader lease is refused (4.2.3). | `RaftNodeElectionTests.PreVote_partitionedFollowerDoesNotBumpHealthyLeaderTerm`; `PrytolynParityTests.PreVote_higherIdWithStrictlyNewerLog_isGranted`; TLA `PreVote` |
| 9.6 and 4.2.3 CheckQuorum | implemented | Listed under 4.2.3. Section 9.6 presents PreVote; CheckQuorum is the disruptive-server rule in 4.2.3. | Same 4.2.3 row. |

## Chapter 10 (implementation rules this tree follows)

| Section | Status | Code | Evidence |
| --- | --- | --- | --- |
| 10.2.1 Disk write in parallel with AppendEntries | extended | `ReplicateToAll` sends before the leader `Force`. The leader counts itself only once `DurableIndex` covers the index. | `Leader_sendsAppendEntriesBeforeItsOwnForce`; `Leader_doesNotCountItselfUntilForce` |
| 10.2.2 Batching and pipelining | implemented | `MaxAppendEntriesBatch`, `MaxAppendEntriesBytes`. The KV client keeps more than one proposal in flight. | `Phase3RobustnessTests.Finding6_appendBatch_staysUnderFrameCap`; write-throughput rows in `docs/benchmarks.md` |

Chapters 7 (user study), 11 (related work), and 12 (conclusion) are not applicable. Nothing in those chapters is a runtime rule.

## Beyond the thesis

These are in `docs/pure-improvements.md`, with the tests named there. They do not replace a row above.

- Durable self-count, monotonic follower `commitIndex`, and ack-after-`Force` with one fsync per drain.
- Transfer reports success only after the target is leader. `TimeoutNow` at a higher term campaigns at `term + 1`.
- Leases, CheckQuorum, and election deadlines use a monotonic clock. A leadership-transfer window does not extend either read lease.
- A client waiter completes only when the applied entry has the proposal's term. A snapshot that covers the index is indeterminate.
- `InstallSnapshot` echoes the installed index and term. `matchIndex` advances only to that echo.
- ReadIndex keeps a ticket per round. An ack from before the read does not confirm it. One dead peer is dropped from the in-flight set.
- Sessions add an ack floor and `OutOfOrder`. Expiry itself is thesis 6.3.
- KV delta snapshots (`KvSnapshotTests.DeltaSnapshot_mergesOnBase`) sit on top of the full snapshot.
- `BoundedStaleGet` is an opt-in follower read. It is not the linearizable lease.
- A grant-less pre-vote retries when the remaining vote gate ends (`RaftNodeElectionTests.GrantlessPreVote_retriesWhenTheAdvertisedLeaseEnds`). The gate in 4.2.3 is unchanged.
- The hot lease read does not take the Raft lock. The raft thread publishes the expiry.
- `SocketTransport` coalesces AppendEntries and flushes the next frame from the send callback. The raft thread only enqueues.
- `RedirectingKvClient` is one hop, not a cluster-wide directory. A range-sharded client is optional and outside Raft.

## Rejected tradeoffs

The full list is `docs/pure-improvements.md` under "Considered and rejected". The ones that would have changed a thesis rule or a measured path:

- Do not send AppendEntries until the leader has fsynced. That adds a disk round. Section 10.2.1 allows the parallel send. The durable self-count is the safety fix.
- Do not count a follower ack until a barrier past the existing force-before-ack. The extra barrier adds latency.
- Report transfer success when `TimeoutNow` is sent, or step down at that send. A lost `TimeoutNow` then costs a full election. Section 3.10 is met by staying leader until the target wins or the deadline aborts.
- Shorten the read lease to two heartbeats so PreVote can use a shorter gate. The lease and the vote gate stay the election timeout (6.4.1 and 4.2.3).
- Refuse proposals while transfer is still catching the target up. Section 3.10 refuses new proposals only after `TimeoutNow`.
- Drive leases from wall time. A step can expire or extend a lease with no real interval.
- Give a read recorded during an in-flight round that round's stamp. The stamp was sent before the read existed (6.4).
- Count append acks toward the read lease during a leadership transfer. The target can win while the old lease is still valid.
- Drop a live socket because a send made no progress, or arm `TCP_USER_TIMEOUT`. A peer is dead on inbound silence of one election timeout, which is the same window as CheckQuorum.

## Gaps

- Chapter 8. The C# implementation is not formally proven. `tla/Synkrolyn.tla` models election, replication, current-term commit, `forcedIndex`, PreVote, CheckQuorum, and a quorum-ack read (`Read`, invariant `ReadLinearizability`) for 3 servers, term at most 3, and log length at most 3. It does not model membership, learners, joint consensus, snapshots, InstallSnapshot, sessions, ReadIndex tickets, the clock-drift bound ρ, or sockets. No thesis protocol rule above was found missing from the C# tree. The rows marked not applicable are design goals, unused alternatives, or experiments this repository does not re-run.
