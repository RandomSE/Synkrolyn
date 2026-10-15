# Synkrolyn Raft model

`tla/Synkrolyn.tla` models the implementation's election and replication path. A log entry is its term. `tla/MC.cfg` is 3 servers, terms at most 2, logs of length at most 2, and the real current-term commit rule. `tla/MCBug.cfg` sets `CommitPriorTerms` and starts from a five-server figure-8 state, so TLC must report a counterexample.

- `Timeout` maps to the election-deadline tick calling `BeginPreVote` (`RaftNode.cs:1209`).
- `PreVote` maps to `OnPreVote` (`RaftNode.cs:1543`).
- `BeginElection` maps to `BeginElection` and `CampaignAt` (`RaftNode.cs:1241`).
- `RequestVote` maps to `OnRequestVote` and `OnRequestVoteResponse` (`RaftNode.cs:1509`, `RaftNode.cs:1585`).
- `BecomeLeader` maps to `BecomeLeader` (`RaftNode.cs:1283`).
- `ClientAppend` maps to `AppendLocked` (`RaftNode.cs:3066`).
- `Force` maps to the flush that lets the leader count its own copy in commit.
- `AppendEntries` maps to `OnAppendEntries` (`RaftNode.cs:1624`); a mismatch drops the follower's last entry.
- `AdvanceCommit` maps to `MaybeAdvanceCommit` (`RaftNode.cs:2310`).
- `CheckQuorum` maps to `MaybeCheckQuorum` (`RaftNode.cs:3028`).
- `Restart` maps to the restart lease armed in `Start` (`RaftNode.cs:481`).
- `ExpireRestartGate` maps to the restart window ending so the election deadline can fire (`RaftNode.cs:490`).
- `Read` maps to `ReadIndexSatisfied` and `LinearizableLeaseValidLocked` (`RaftNode.cs:840`, `RaftNode.cs:2831`).

Not in the model: membership, snapshots, the XLen/XTerm fast backup, sockets, and leadership transfer.
