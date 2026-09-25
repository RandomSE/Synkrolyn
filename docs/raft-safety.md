# Raft safety properties

Election safety, log matching, and state-machine safety are checked on an in-process cluster. A passing unit test is evidence for that scenario. It is not a proof of `RaftNode`.

| Property | Meaning | Covered by |
| --- | --- | --- |
| Election safety | At most one leader in a term | `ClusterHarness.AssertAtMostOneLeaderPerTerm` after every harness step; `RaftNodeElectionTests.ThreeNode_shortestTimeoutWins_exactlyOneLeader`; `SingleNode_electsItself`; `SplitVote_thenLaterTimeoutElectsOneLeader` |
| Leader append-only | A leader does not truncate its own log | Leader `Propose` only appends. Followers truncate. `Figure8_oldTermMajority_doesNotCommitUntilCurrentTermEntry` |
| Log matching | Same index and term implies the same prefix | `ThreeNode_propose_commitsOnMajority_andThirdCatchesUp`; `FastBackupRepairTests`; `ChaosCampaignTests.ConstructedDivergence_dropAndDelayOnHeal_committedKvWins`; `SnapshotClusterTests.FarBehindEmptyFollower_catchesUpViaInstallSnapshot` |
| Leader completeness | A committed entry is present in later leaders | `Figure8_oldTermMajority_doesNotCommitUntilCurrentTermEntry`; `DurableRestartTests.RestartAllThree_logsKeepCommitted_newTermProposeReappliesKv` |
| State machine safety | The same index is not applied as two different commands | `KvClusterTests.StateMachineSafety_sameIndexSameKvEffect`; `StaleLeaderUncommittedPut_doesNotApply_winnerAppliesAfterHeal` |
| Current-term commit | Commit advances only for an entry from the leader's current term | `Figure8_oldTermMajority_doesNotCommitUntilCurrentTermEntry`; leader no-op is index 1 in the KV tests (`AwaitCommitted` is 2 for the first put) |
| Durable self-count | The leader counts itself only after `Force` | `Leader_doesNotCountItselfUntilForce` |
| matchIndex from the RPC | A dropped suffix is not committed because an older heartbeat ack arrives | `MatchIndexInflightTests.DroppedLargeSuffix_delayedHeartbeatSuccess_mustNotAdvanceMatchIndexOrCommit` |
| Fast backup | A long divergent or short follower is repaired without a one-step walk | `FastBackupRepairTests` (at most four failed AppendEntries for 32 entries) |
| Monotonic follower commit | Reordered AppendEntries does not decrease `commitIndex` | `ReorderedAppendEntries_commitIndexDoesNotDecrease` |
| Force before ack | No success or apply before fsync | `CrashBeforeForce_waiterNotApplied_reopenDropsEntry` |
| Snapshot echo | Stale InstallSnapshot done does not jump `matchIndex` | `InstallSnapshot_staleDoneDoesNotJumpMatchToNewerSnapshot` |
| PreVote | A partitioned follower does not bump a healthy leader's term | `PreVote_partitionedFollowerDoesNotBumpHealthyLeaderTerm` |
| CheckQuorum | An isolated leader steps down; the grace seed is not a read lease | `IsolatedLeader_withoutNewElection_cannotCompleteReadIndex`; `LeaseFalseBeforeAppendAck_checkQuorumGraceKeepsLeader` |
| Linearizable read | Leader ReadIndex or send-time lease. Followers return null. | `ReadIndexTests` |
| Bounded-stale read | Opt-in lease. Not linearizable. | `FollowerLinearizableGet_emptyWhileBoundedStaleGetHits`; `FollowerBoundedStaleGet_emptyAfterLeaseExpires` |
| Exactly-once | Same client id and serial applies once, including across snapshots | `DuplicatePutSameClientSerial_appliedOnce`; `KvSnapshotTests` |
| Membership | Learners, promote after catch-up, remove majority guard, joint Cold+Cnew | `MembershipChangeTests`; `JointConsensusTests` |

`Get` is the applied local map. It is not a linearizable read.

The same names appear as TLA+ invariants in `tla/Synkrolyn.tla`: `ElectionSafety`, `LeaderAppendOnly`, `LogMatching`, `CommittedPrefixSafety`. See `docs/tla.md`. That model does not replace these tests.
