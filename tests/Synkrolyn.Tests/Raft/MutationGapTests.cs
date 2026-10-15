using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

/// <summary>
/// Observable gaps left by the Basic mutation run: vote and term rules, the read lease
/// boundary, and joint check-quorum. Assertions are on role, grant, commit, and lease.
/// </summary>
public sealed class MutationGapTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void EqualTermPreVote_isGranted_andALeaderDoesNotGrant()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100_000), Heartbeat);
        var grants = new List<bool>();
        cluster.Transport.Register("n2", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response)
            {
                grants.Add(response.VoteGranted);
            }
        });
        cluster.Transport.Register("n3", _ => { });
        cluster.Transport.Send("n2", "n1", new RequestVote(0, "n0", 0, 0, true));
        cluster.DrainAll();
        Assert.Equal([true], grants);
        Assert.Equal(Role.Follower, cluster.Node("n1").Role);

        ClusterHarness elected = Elect();
        var denied = new List<bool>();
        elected.Transport.Reregister("n2", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && response.PreVote)
            {
                denied.Add(response.VoteGranted);
            }
        });
        IRaftLog log = elected.Log("n1");
        elected.Transport.Send(
            "n2",
            "n1",
            new RequestVote(elected.Node("n1").CurrentTerm, "n0", log.LastIndex, log.LastTerm, true));
        elected.DrainAll();
        Assert.Equal([false], denied);
        Assert.Equal(Role.Leader, elected.Node("n1").Role);
    }

    [Fact]
    public void FollowerPreVote_rejectsStaleLog_evenWhenTheVoteGateIsOpen()
    {
        ClusterHarness cluster = Elect();
        RaftNode follower = cluster.Node("n2");
        Assert.True(follower.CommitIndex > 0);
        var grants = new List<bool>();
        cluster.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && response.PreVote)
            {
                grants.Add(response.VoteGranted);
            }
        });
        cluster.Transport.Send(
            "n3",
            "n2",
            new RequestVote(follower.CurrentTerm, "n0", 0, 0, true, 0, true));
        cluster.DrainAll();
        Assert.Equal([false], grants);
        Assert.Equal("n1", follower.LeaderId);
        Assert.Equal(Role.Follower, follower.Role);
    }

    [Fact]
    public void PreCandidate_refusesItsOwnId_andGrantsASameTermLongerLog()
    {
        var sent = new List<RequestVote>();
        ClusterHarness cluster = StartPreCandidate(sent);
        RaftNode n2 = cluster.Node("n2");
        var own = new List<bool>();
        cluster.Transport.Reregister("n1", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && response.PreVote)
            {
                own.Add(response.VoteGranted);
            }
        });
        IRaftLog log = cluster.Log("n2");
        cluster.Transport.Send("n1", "n2", new RequestVote(n2.CurrentTerm, "n2", log.LastIndex, log.LastTerm, true));
        cluster.DrainAll();
        Assert.Equal([false], own);

        var newer = new List<bool>();
        cluster.Transport.Register("n9", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && response.PreVote)
            {
                newer.Add(response.VoteGranted);
            }
        });
        cluster.Transport.Send(
            "n9",
            "n2",
            new RequestVote(n2.CurrentTerm, "n9", log.LastIndex + 5, log.LastTerm, true));
        cluster.DrainAll();
        Assert.Equal([true], newer);
        Assert.Equal(Role.Follower, n2.Role);
    }

    [Fact]
    public void EqualTermRequestVote_isNotStale_andDoesNotClearTheLeader()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100_000), Heartbeat);
        var grants = new List<bool>();
        cluster.Transport.Register("n2", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && !response.PreVote)
            {
                grants.Add(response.VoteGranted);
            }
        });
        cluster.Transport.Register("n3", _ => { });
        cluster.Transport.Send("n2", "n1", new RequestVote(0, "n2", 0, 0));
        cluster.DrainAll();
        Assert.Equal([true], grants);
        Assert.Equal("n2", cluster.State("n1").VotedFor);
        Assert.Equal(0, cluster.Node("n1").CurrentTerm);

        ClusterHarness elected = Elect();
        RaftNode follower = elected.Node("n2");
        long term = follower.CurrentTerm;
        Assert.Equal("n1", follower.LeaderId);
        elected.Transport.Send(
            "n3",
            "n2",
            new RequestVote(term, "n9", elected.Log("n2").LastIndex, elected.Log("n2").LastTerm, false, 0, true));
        elected.DrainAll();
        Assert.Equal("n1", follower.LeaderId);
        Assert.Equal(term, follower.CurrentTerm);
        Assert.Equal(Role.Follower, follower.Role);
        Assert.Equal(Role.Leader, elected.Node("n1").Role);
    }

    [Fact]
    public void Follower_ignoresGrantedVotes_andOnePreVoteDenialDoesNotDropALaterGrant()
    {
        ClusterHarness elected = Elect();
        RaftNode follower = elected.Node("n2");
        long term = follower.CurrentTerm;
        elected.Transport.Send("n1", "n2", new RequestVoteResponse(term, true));
        elected.Transport.Send("n3", "n2", new RequestVoteResponse(term, true));
        elected.DrainAll();
        Assert.Equal(Role.Follower, follower.Role);
        Assert.Equal(Role.Leader, elected.Node("n1").Role);
        Assert.Equal("n1", follower.LeaderId);

        var sent = new List<RequestVote>();
        ClusterHarness cluster = StartPreCandidate(sent);
        long round = sent[0].Round;
        cluster.Transport.Send("n1", "n2", new RequestVoteResponse(0, false, true, round));
        cluster.Transport.Send("n3", "n2", new RequestVoteResponse(0, true, true, round));
        cluster.DrainAll();
        Assert.Equal(Role.Candidate, cluster.Node("n2").Role);
        Assert.Equal(1, cluster.Node("n2").CurrentTerm);
    }

    [Fact]
    public void HeardLeader_blocksAVoteUntilTheElectionTimeout_thenGrants()
    {
        const long timeout = 200;
        ClusterHarness cluster = QuietNode(timeout);
        HearLeaderWithoutAdvertisedLease(cluster);
        var early = new List<bool>();
        cluster.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && !response.PreVote)
            {
                early.Add(response.VoteGranted);
            }
        });
        cluster.Transport.Send("n3", "n1", new RequestVote(cluster.Node("n1").CurrentTerm, "n3", 0, 0));
        cluster.DrainAll();
        Assert.Equal([false], early);
        Assert.Null(cluster.State("n1").VotedFor);

        ClusterHarness later = QuietNode(timeout);
        HearLeaderWithoutAdvertisedLease(later);
        var opened = new List<bool>();
        later.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && !response.PreVote)
            {
                opened.Add(response.VoteGranted);
            }
        });
        later.Clock.Advance(timeout);
        later.Transport.Send("n3", "n1", new RequestVote(later.Node("n1").CurrentTerm, "n3", 0, 0));
        later.DrainAll();
        Assert.Equal([true], opened);
        Assert.Equal("n3", later.State("n1").VotedFor);
    }

    [Fact]
    public void RestartLease_blocksUntilTheDeadline_thenAllowsTheVote()
    {
        const long timeout = 200;
        ClusterHarness early = Restarted(timeout);
        early.Clock.Advance(timeout - 1);
        var denied = CaptureRealVotes(early);
        early.Transport.Send("n3", "n1", new RequestVote(early.Node("n1").CurrentTerm, "n3", 0, 0));
        early.DrainAll();
        Assert.Equal([false], denied);
        Assert.Null(early.State("n1").VotedFor);

        ClusterHarness opened = Restarted(timeout);
        var granted = CaptureRealVotes(opened);
        opened.Clock.Advance(timeout);
        opened.Transport.Send("n3", "n1", new RequestVote(opened.Node("n1").CurrentTerm, "n3", 0, 0));
        opened.DrainAll();
        Assert.Equal([true], granted);
        Assert.Equal("n3", opened.State("n1").VotedFor);
    }

    [Fact]
    public void TinyDriftBound_stillPublishesAOneMillisecondLease()
    {
        ClusterHarness cluster = Elect(150);
        RaftNode leader = cluster.Node("n1");
        leader.SetClockDriftBound(0.999);
        Assert.True(leader.ReadIndexSatisfied());
        cluster.Clock.Advance(1);
        Assert.False(leader.ReadIndexSatisfied());
        Assert.Equal(Role.Leader, leader.Role);
    }

    [Fact]
    public void FollowerBeginReadIndex_returnsNull_afterACurrentTermCommit()
    {
        ClusterHarness cluster = Elect();
        Assert.NotNull(cluster.Propose("n1", "cmd"u8.ToArray()));
        RaftNode follower = cluster.Node("n2");
        Assert.True(follower.CommitIndex > 0);
        Assert.Null(follower.BeginReadIndex());
        Assert.NotNull(cluster.Node("n1").BeginReadIndex());
        Assert.Equal(Role.Follower, follower.Role);
    }

    [Fact]
    public void ReadLease_holdsThroughTheLastMillisecond_andFailsOnTheExpiry()
    {
        const long election = 150;
        ClusterHarness cluster = Elect(election);
        cluster.Node("n1").SetClockDriftBound(0);
        Assert.NotNull(cluster.Propose("n1", "cmd"u8.ToArray()));
        cluster.Advance(Heartbeat.Milliseconds);
        RaftNode leader = cluster.Node("n1");
        Assert.True(leader.ReadIndexSatisfied());
        cluster.Clock.Advance(election - 1);
        Assert.True(leader.ReadIndexSatisfied());
        cluster.Clock.Advance(1);
        Assert.False(leader.ReadIndexSatisfied());
        Assert.Equal(Role.Leader, leader.Role);
    }

    [Fact]
    public void JointCheckQuorum_stepsDownWhenOneSideIsSilent()
    {
        ClusterHarness cluster = JointLeader();
        cluster.Isolate("n2");
        cluster.Isolate("n4");
        cluster.Advance(150);
        Assert.NotEqual(Role.Leader, cluster.Node("n1").Role);
        Assert.True(cluster.Leaders().Count <= 1);
    }

    [Fact]
    public void JointCheckQuorum_staysLeaderWhenSelfCompletesOneSide()
    {
        ClusterHarness cluster = JointLeader();
        cluster.Isolate("n4");
        cluster.Advance(150);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        Assert.True(cluster.Node("n1").InJointConsensus);
    }

    [Fact]
    public void JointCheckQuorum_treatsAnAckAtTheTimeoutAsStale()
    {
        ClusterHarness cluster = JointLeader();
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        cluster.Clock.Advance(150);
        cluster.Node("n1").CheckQuorumAfterDrain();
        Assert.NotEqual(Role.Leader, cluster.Node("n1").Role);
    }

    [Fact]
    public void LowerTermAppendSuccess_doesNotAdvanceMatch()
    {
        ClusterHarness cluster = Elect();
        Assert.NotNull(cluster.Propose("n1", "a"u8.ToArray()));
        RaftNode leader = cluster.Node("n1");
        long match = leader.MatchIndex("n2");
        long term = leader.CurrentTerm;
        Assert.True(match >= 1);
        leader.Receive(new Envelope("n2", "n1", new AppendEntriesResponse(term - 1, true, match + 10)));
        leader.Drain();
        Assert.Equal(match, leader.MatchIndex("n2"));
        Assert.Equal(Role.Leader, leader.Role);
        Assert.Equal(term, leader.CurrentTerm);
    }

    [Fact]
    public void FastBackup_equalHintLowersMatch_andAZeroHintDoesNot()
    {
        ClusterHarness hinted = Elect();
        Assert.NotNull(hinted.Propose("n1", "a"u8.ToArray()));
        RaftNode leader = hinted.Node("n1");
        long match = leader.MatchIndex("n2");
        long next = leader.NextIndex("n2");
        Assert.True(match >= 1);
        Assert.Equal(match + 1, next);
        leader.Receive(new Envelope(
            "n2",
            "n1",
            new AppendEntriesResponse(leader.CurrentTerm, false, 0, match, 0, 0, 1, next - 1)));
        leader.Drain();
        Assert.Equal(match - 1, leader.MatchIndex("n2"));
        Assert.Equal(match, leader.NextIndex("n2"));
        Assert.Equal(Role.Leader, leader.Role);

        ClusterHarness zero = Elect();
        Assert.NotNull(zero.Propose("n1", "a"u8.ToArray()));
        RaftNode again = zero.Node("n1");
        long kept = again.MatchIndex("n2");
        long before = again.NextIndex("n2");
        again.Receive(new Envelope(
            "n2",
            "n1",
            new AppendEntriesResponse(again.CurrentTerm, false, 0, 0, 0, 0, 1, before - 1)));
        again.Drain();
        Assert.Equal(kept, again.MatchIndex("n2"));
        Assert.Equal(before, again.NextIndex("n2"));
        Assert.Equal(Role.Leader, again.Role);
    }

    [Fact]
    public void ConflictingAppend_keepsThePrefixAndReplacesTheSuffix()
    {
        var cluster = new ClusterHarness();
        cluster.Transport.Register("n2", _ => { });
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(100_000), Heartbeat);
        cluster.Transport.Send("n2", "n1", new AppendEntries(
            1,
            "n2",
            0,
            0,
            [new LogEntry(1, 1, "a"u8.ToArray()), new LogEntry(2, 1, "b"u8.ToArray()), new LogEntry(3, 1, "c"u8.ToArray())],
            3));
        cluster.DrainAll();
        cluster.Transport.Send("n2", "n1", new AppendEntries(
            2,
            "n2",
            1,
            1,
            [new LogEntry(2, 2, "x"u8.ToArray()), new LogEntry(3, 2, "y"u8.ToArray())],
            3));
        cluster.DrainAll();
        IRaftLog log = cluster.Log("n1");
        Assert.Equal("a"u8.ToArray(), log.Read(1).Command);
        Assert.Equal(2, log.Read(2).Term);
        Assert.Equal("x"u8.ToArray(), log.Read(2).Command);
        Assert.Equal("y"u8.ToArray(), log.Read(3).Command);
        Assert.Equal(3, log.LastIndex);
        Assert.Equal(3, cluster.Node("n1").CommitIndex);
        Assert.Equal(Role.Follower, cluster.Node("n1").Role);
        Assert.Equal(2, cluster.Node("n1").CurrentTerm);
    }

    [Fact]
    public void StalledApply_doesNotStartAnother_andTheLeaderLeaseStaysOnTheLeader()
    {
        ClusterHarness cluster = Elect();
        RaftNode follower = cluster.Node("n2");
        int calls = 0;
        follower.SetApplyExecutor(_ => calls++);
        Assert.NotNull(cluster.Propose("n1", "a"u8.ToArray()));
        Assert.NotNull(cluster.Propose("n1", "b"u8.ToArray()));
        Assert.Equal(1, calls);
        Assert.True(follower.CommitIndex > follower.LastApplied);
        Assert.False(follower.FollowerReadLeaseValid);
        Assert.False(follower.QuorumLeaseValid);
        RaftNode leader = cluster.Node("n1");
        Assert.False(leader.FollowerReadLeaseValid);
        Assert.True(leader.QuorumLeaseValid);
        Assert.Equal(Role.Leader, leader.Role);
    }

    [Fact]
    public void ZeroLeaseHeartbeat_doesNotShrinkALongerAdvertisedLease()
    {
        var cluster = new ClusterHarness();
        cluster.Transport.Register("n2", _ => { });
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(100_000), Heartbeat);
        cluster.Transport.Send("n2", "n1", new AppendEntries(1, "n2", 0, 0, [], 0, 1, 500));
        cluster.DrainAll();
        cluster.Transport.Send("n2", "n1", new AppendEntries(1, "n2", 0, 0, [], 0, 2, 0));
        cluster.DrainAll();
        cluster.Clock.Advance(200);
        var grants = new List<bool>();
        cluster.Transport.Reregister("n2", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && !response.PreVote)
            {
                grants.Add(response.VoteGranted);
            }
        });
        cluster.Transport.Send("n2", "n1", new RequestVote(1, "n2", 0, 0));
        cluster.DrainAll();
        Assert.Equal([false], grants);
        Assert.Null(cluster.State("n1").VotedFor);
        Assert.Equal(500, cluster.State("n1").MaxAdvertisedLeaseMillis);
    }

    [Fact]
    public void Compact_raisesTheDurableIndexToTheSnapshot()
    {
        var log = new InMemoryRaftLog();
        log.Append(new LogEntry(1, 1, [1]));
        log.Append(new LogEntry(2, 1, [2]));
        Assert.Equal(0, log.DurableIndex);
        log.CompactThrough(2, 1, [9]);
        Assert.Equal(2, log.DurableIndex);
        Assert.Equal(2, log.LastIncludedIndex);
        Assert.Equal(1, log.LastIncludedTerm);
        Assert.Throws<InvalidOperationException>(() => log.Read(2));
    }

    [Fact]
    public void ReadBatch_keepsAnExactFit_andASingleOversizedEntry()
    {
        var log = new InMemoryRaftLog();
        log.Append(new LogEntry(1, 1, new byte[10]));
        log.Append(new LogEntry(2, 1, new byte[10]));
        log.Append(new LogEntry(3, 1, new byte[100]));
        ReplicationBatch fit = log.ReadForReplication(1, 10, 20);
        Assert.Equal(2, fit.Entries.Count);
        ReplicationBatch oversized = log.ReadForReplication(3, 10, 1);
        LogEntry only = Assert.Single(oversized.Entries);
        Assert.Equal(3, only.Index);
    }

    private static ClusterHarness Elect(long electionMillis = 80)
    {
        var cluster = new ClusterHarness();
        var election = TimeSpan.FromMilliseconds(electionMillis);
        var slow = TimeSpan.FromMilliseconds(100_000);
        cluster.AddNode("n1", ["n2", "n3"], election, Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], slow, Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], slow, Heartbeat);
        cluster.Advance(electionMillis);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        return cluster;
    }

    private static ClusterHarness JointLeader()
    {
        var cluster = new ClusterHarness();
        var election = TimeSpan.FromMilliseconds(150);
        var slow = TimeSpan.FromMilliseconds(100_000);
        cluster.AddNode("n1", ["n2", "n3"], election, Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], slow, Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], slow, Heartbeat);
        cluster.Advance(150);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        cluster.AddNode("n4", ["n1", "n2", "n3"], slow, Heartbeat);
        cluster.Node("n1").SetHoldJoint(true);
        Assert.NotNull(cluster.Node("n1").EnterJoint(["n1", "n2", "n4"]));
        cluster.DrainAll();
        cluster.Advance(Heartbeat.Milliseconds);
        Assert.True(cluster.Node("n1").InJointConsensus);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        return cluster;
    }

    private static ClusterHarness StartPreCandidate(List<RequestVote> sent)
    {
        var cluster = new ClusterHarness();
        cluster.Transport.Register("n1", envelope =>
        {
            if (envelope.Payload is RequestVote vote)
            {
                sent.Add(vote);
            }
        });
        cluster.Transport.Register("n3", envelope =>
        {
            if (envelope.Payload is RequestVote vote)
            {
                sent.Add(vote);
            }
        });
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.Advance(80);
        Assert.Equal(Role.Follower, cluster.Node("n2").Role);
        Assert.Contains(sent, vote => vote.PreVote);
        return cluster;
    }

    private static List<bool> CaptureRealVotes(ClusterHarness cluster)
    {
        var grants = new List<bool>();
        cluster.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && !response.PreVote)
            {
                grants.Add(response.VoteGranted);
            }
        });
        return grants;
    }

    private static ClusterHarness Restarted(long electionMillis)
    {
        var state = new InMemoryPersistentState();
        state.SetCurrentTerm(3);
        var cluster = new ClusterHarness();
        cluster.Transport.Register("n2", _ => { });
        cluster.Transport.Register("n3", _ => { });
        cluster.AddNode(
            "n1",
            ["n2", "n3"],
            TimeSpan.FromMilliseconds(electionMillis),
            Heartbeat,
            state: state);
        Assert.Equal(3, cluster.Node("n1").CurrentTerm);
        Assert.Equal(Role.Follower, cluster.Node("n1").Role);
        return cluster;
    }

    private static ClusterHarness QuietNode(long electionMillis)
    {
        var cluster = new ClusterHarness();
        cluster.Transport.Register("n2", _ => { });
        cluster.Transport.Register("n3", _ => { });
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(electionMillis), Heartbeat);
        return cluster;
    }

    private static void HearLeaderWithoutAdvertisedLease(ClusterHarness cluster)
    {
        cluster.Transport.Send(
            "n2",
            "n1",
            new AppendEntries(1, "n2", 0, 0, [], 0, 1, 0));
        cluster.DrainAll();
        Assert.Equal("n2", cluster.Node("n1").LeaderId);
        Assert.Equal(1, cluster.Node("n1").CurrentTerm);
    }
}
