# Membership

Synkrolyn starts from the constructor peer list. Those peers are voters. Self is not stored in the peer set.

## Single-server changes

`AddLearner`, `PromoteVoter`, and `RemoveServer` each append one membership command. The new config is installed when that entry is applied (committed), not when it is first appended.

- A learner does not vote and does not count in the commit majority.
- `PromoteVoter` returns null while `matchIndex` is behind `commitIndex`.
- `RemoveServer` refuses a voter removal that would leave fewer voters than a majority of the current set. A 3-voter cluster may become 2. A 2-voter cluster may not become 1. Removing a learner is allowed.
- An empty learner cannot win an election.

Config is stored in the snapshot header (`MembershipSnapshot`, magic `PTCF`) and in `config.bin` next to hard state.

## Joint consensus

`EnterJoint` appends Cold (current voters) and Cnew (the new set, which must include this node). That config is live when the entry is appended, on the leader and on a follower that stores it. Commit and votes need a majority of Cold and a majority of Cnew.

If the uncommitted joint entry is truncated, the node reverts to the membership in the remaining log.

After the joint entry commits, the leader appends Cnew unless `SetHoldJoint(true)`. `LeaveJoint` appends Cnew explicitly. Auto-Cnew does not loop.

Snapshots and `config.bin` keep Cold and Cnew. InstallSnapshot onto a blank log restores both sets.

Single-server add, promote, and remove stay apply-on-commit. Joint stays live on append. Making single-server changes live on append would change the majority before commit, which is a different protocol, so it is not done.

## Tests

- `MembershipChangeTests.AddLearner_doesNotChangeMajority_untilPromotedAfterCatchUp`
- `PromoteVoter_refusedWhileEmptyLearnerIsBehind`
- `RemoveVoter_oldNodeNoLongerRequiredForCommit`
- `RemoveVoterFromTwo_returnsEmpty_learnerRemoveReturnsIndex`
- `EmptyLearner_cannotWinElection`
- `SnapshotAndHardState_persistConfig_restoreOnReplace`
- `JointConsensusTests.EnterJoint_leaderUsesJointQuorumBeforeCommit`
- `FollowerAppendsJoint_inJointBeforeFollowerCommit`
- `TruncateUncommittedJoint_revertsToColdQuorum`
- `HoldJointFalse_autoAppendsCnewAfterJointCommits`
- `HoldJointTrue_staysJointUntilLeaveJoint`
- `HoldJoint_snapshotCloseReopen_restoresColdAndCnew`
- `InstallSnapshot_midJointOntoBlankNode_restoresColdAndCnew`

## Not in this tree

Service discovery, an admin UI, and automatic cluster bootstrap are out of scope. `SocketTransport.SetPeer` is how a new id gets an address.
