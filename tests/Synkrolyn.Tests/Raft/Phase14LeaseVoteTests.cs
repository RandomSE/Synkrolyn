using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

/// <summary>
/// Lease and vote-gate boundaries. Each assertion changes if the comparison,
/// the transfer exception, or the one-millisecond retry is removed.
/// </summary>
public sealed class Phase14LeaseVoteTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void TransferVote_isGrantedInsideTheLeaderLease_andANormalVoteIsNot()
    {
        ClusterHarness cluster = Heard(advertised: 500);
        RaftNode follower = cluster.Node("n1");
        long term = follower.CurrentTerm;
        var grants = new List<bool>();
        cluster.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && !response.PreVote)
            {
                grants.Add(response.VoteGranted);
            }
        });

        cluster.Transport.Send("n3", "n1", new RequestVote(term, "n3", 0, 0, false, 0, true));
        cluster.DrainAll();
        Assert.Equal([true], grants);
        Assert.Equal("n3", cluster.State("n1").VotedFor);
        Assert.Equal(Role.Follower, follower.Role);

        ClusterHarness blocked = Heard(advertised: 500);
        var denied = new List<bool>();
        blocked.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && !response.PreVote)
            {
                denied.Add(response.VoteGranted);
            }
        });
        blocked.Transport.Send("n3", "n1", new RequestVote(blocked.Node("n1").CurrentTerm, "n3", 0, 0));
        blocked.DrainAll();
        Assert.Equal([false], denied);
        Assert.Null(blocked.State("n1").VotedFor);
        Assert.Equal(Role.Follower, blocked.Node("n1").Role);
    }

    [Fact]
    public void OpenVoteGate_preVoteDenialRetriesOnTheNextMillisecond()
    {
        var cluster = new ClusterHarness();
        long round = 0;
        cluster.Transport.Register("n1", envelope =>
        {
            if (envelope.Payload is RequestVote vote && vote.PreVote)
            {
                round = vote.Round;
            }
        });
        cluster.Transport.Register("n3", envelope =>
        {
            if (envelope.Payload is RequestVote vote && vote.PreVote)
            {
                round = vote.Round;
            }
        });
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.Advance(80);
        Assert.True(round > 0);
        Assert.Equal(Role.Follower, cluster.Node("n2").Role);

        long term = cluster.Node("n2").CurrentTerm;
        cluster.Transport.Send("n1", "n2", new RequestVoteResponse(term, false, true, round));
        cluster.Transport.Send("n3", "n2", new RequestVoteResponse(term, false, true, round));
        cluster.DrainAll();
        RaftNode node = cluster.Node("n2");
        Assert.Equal(cluster.Clock.Millis + 1, node.NextDeadlineMillis());
        Assert.Equal(Role.Follower, node.Role);
        Assert.Equal(term, node.CurrentTerm);

        cluster.Clock.Advance(1);
        node.Drain();
        Assert.True(node.NextDeadlineMillis() > cluster.Clock.Millis);
    }

    [Fact]
    public void PreVoteRetry_usesTheAdvertisedLease_andAZeroBoundUsesTheTimeout()
    {
        long advertisedRound = 0;
        var advertised = new ClusterHarness();
        advertised.Transport.Register("n2", envelope =>
        {
            if (envelope.Payload is RequestVote vote && vote.PreVote)
            {
                advertisedRound = vote.Round;
            }
        });
        advertised.Transport.Register("n3", envelope =>
        {
            if (envelope.Payload is RequestVote vote && vote.PreVote)
            {
                advertisedRound = vote.Round;
            }
        });
        advertised.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        advertised.Transport.Send("n2", "n1", new AppendEntries(1, "n2", 0, 0, [], 0, 1, 400));
        advertised.DrainAll();
        advertised.Advance(80);
        Assert.True(advertisedRound > 0);
        long advertisedTerm = advertised.Node("n1").CurrentTerm;
        advertised.Transport.Send("n2", "n1", new RequestVoteResponse(advertisedTerm, false, true, advertisedRound));
        advertised.Transport.Send("n3", "n1", new RequestVoteResponse(advertisedTerm, false, true, advertisedRound));
        advertised.DrainAll();
        Assert.Equal(400, advertised.Node("n1").NextDeadlineMillis());
        Assert.Equal(Role.Follower, advertised.Node("n1").Role);

        var zero = new ClusterHarness();
        var zeroGrants = new List<bool>();
        zero.Transport.Register("n2", _ => { });
        zero.Transport.Register("n3", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && !response.PreVote)
            {
                zeroGrants.Add(response.VoteGranted);
            }
        });
        zero.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(200), Heartbeat);
        zero.Transport.Send("n2", "n1", new AppendEntries(1, "n2", 0, 0, [], 0, 1, 0));
        zero.DrainAll();
        Assert.Equal("n2", zero.Node("n1").LeaderId);
        zero.Transport.Send("n3", "n1", new RequestVote(zero.Node("n1").CurrentTerm, "n3", 0, 0));
        zero.DrainAll();
        Assert.Equal([false], zeroGrants);
        Assert.Null(zero.State("n1").VotedFor);
        Assert.Equal(Role.Follower, zero.Node("n1").Role);
    }

    [Fact]
    public void QuorumLease_isValidOnTheLastMillisecond_andFailsOnTheBound()
    {
        const long election = 200;
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(election), TimeSpan.FromMilliseconds(1_000));
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(election);
        RaftNode leader = cluster.Node("n1");
        leader.SetClockDriftBound(0);
        Assert.NotNull(cluster.Propose("n1", "v"u8.ToArray()));
        cluster.DrainAll();
        Assert.True(leader.QuorumLeaseValid);
        Assert.True(leader.ReadIndexSatisfied());
        cluster.Transport.PartitionBidirectional("n1", "n2");
        cluster.Clock.Advance(election - 1);
        leader.Drain();
        Assert.True(leader.QuorumLeaseValid);
        Assert.True(leader.ReadIndexSatisfied());
        cluster.Clock.Advance(1);
        leader.Drain();
        Assert.False(leader.QuorumLeaseValid);
        Assert.False(leader.ReadIndexSatisfied());
        Assert.Equal(Role.Leader, leader.Role);
    }

    [Fact]
    public void FollowerLease_expiresAtTimeoutPlusSlack()
    {
        const long election = 100;
        const long slack = 25;
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(election), Heartbeat);
        cluster.Advance(10_000);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        RaftNode follower = cluster.Node("n2");
        follower.SetFollowerLeasePauseSlackMillis(slack);
        cluster.Transport.PartitionBidirectional("n1", "n2");
        cluster.Transport.Send(
            "n1",
            "n2",
            new AppendEntries(cluster.Node("n1").CurrentTerm, "n1", 0, 0, [], 0, 1, election));
        follower.Drain();
        Assert.True(follower.FollowerReadLeaseValid);
        Assert.False(follower.QuorumLeaseValid);

        cluster.Clock.Advance(election + slack - 1);
        follower.Drain();
        Assert.True(follower.FollowerReadLeaseValid);
        Assert.Equal(Role.Follower, follower.Role);
        cluster.Clock.Advance(1);
        follower.Drain();
        Assert.False(follower.FollowerReadLeaseValid);
        Assert.Equal("n1", follower.LeaderId);
    }

    [Fact]
    public void JointLease_failsWhenOnlyOneConfigurationIsFresh()
    {
        ClusterHarness cluster = Joint();
        RaftNode leader = cluster.Node("n1");
        leader.SetClockDriftBound(0);
        Assert.NotNull(cluster.Propose("n1", "joint"u8.ToArray()));
        cluster.DrainAll();
        Assert.True(leader.QuorumLeaseValid);
        Assert.True(leader.ReadIndexSatisfied());

        cluster.Transport.PartitionBidirectional("n1", "n2");
        cluster.Transport.PartitionBidirectional("n1", "n3");
        cluster.Clock.Advance(149);
        leader.Drain();
        cluster.Node("n4").Drain();
        Assert.True(leader.QuorumLeaseValid);
        cluster.Clock.Advance(1);
        leader.Drain();
        cluster.Node("n4").Drain();
        Assert.False(leader.QuorumLeaseValid);
        Assert.False(leader.ReadIndexSatisfied());
        Assert.Equal(Role.Leader, leader.Role);
        Assert.True(leader.InJointConsensus);
    }

    [Fact]
    public void TransferWindow_ignoresCampaignAcks_andCountsAnAckAfterTheWindowCloses()
    {
        var cluster = new ClusterHarness();
        var stamps = new List<long>();
        bool hold = false;
        void Watch(string id, Envelope envelope)
        {
            if (envelope.Payload is AppendEntries append && append.Stamp > 0)
            {
                stamps.Add(append.Stamp);
            }

            if (!hold && envelope.Payload is not TimeoutNow)
            {
                cluster.Node(id).Receive(envelope);
            }
        }

        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(200), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Transport.Reregister("n2", envelope => Watch("n2", envelope));
        cluster.Transport.Reregister("n3", envelope => Watch("n3", envelope));
        cluster.Advance(200);
        RaftNode leader = cluster.Node("n1");
        leader.SetClockDriftBound(0);
        Assert.NotNull(cluster.Propose("n1", "before"u8.ToArray()));
        cluster.DrainAll();
        Assert.True(leader.ReadIndexSatisfied());
        Assert.True(leader.QuorumLeaseValid);

        hold = true;
        Assert.True(leader.TransferLeadership("n2"));
        Assert.Equal(LeadershipTransferStatus.AwaitingWinner, leader.TransferStatus);
        Assert.False(leader.ReadIndexSatisfied());
        Assert.False(leader.QuorumLeaseValid);
        cluster.Advance(Heartbeat.Milliseconds);
        Assert.Equal(LeadershipTransferStatus.AwaitingWinner, leader.TransferStatus);
        long inside = stamps[^1];
        leader.Receive(new Envelope(
            "n3",
            "n1",
            new AppendEntriesResponse(leader.CurrentTerm, true, leader.MatchIndex("n3"), inside)));
        leader.Drain();
        Assert.False(leader.ReadIndexSatisfied());
        Assert.False(leader.QuorumLeaseValid);

        for (int i = 0; i < 9; i++)
        {
            leader.Receive(new Envelope(
                "n3",
                "n1",
                new AppendEntriesResponse(leader.CurrentTerm, true, leader.MatchIndex("n3"))));
            leader.Drain();
            cluster.Advance(20);
        }

        Assert.Equal(LeadershipTransferStatus.Aborted, leader.TransferStatus);
        Assert.Equal(Role.Leader, leader.Role);
        leader.Receive(new Envelope(
            "n3",
            "n1",
            new AppendEntriesResponse(leader.CurrentTerm, true, leader.MatchIndex("n3"), inside)));
        leader.Drain();
        Assert.False(leader.ReadIndexSatisfied());
        Assert.False(leader.QuorumLeaseValid);

        long after = stamps[^1];
        Assert.True(after > inside);
        leader.Receive(new Envelope(
            "n3",
            "n1",
            new AppendEntriesResponse(leader.CurrentTerm, true, leader.MatchIndex("n3"), after)));
        leader.Drain();
        Assert.True(leader.ReadIndexSatisfied());
        Assert.True(leader.QuorumLeaseValid);
        Assert.Equal(Role.Leader, leader.Role);
    }

    [Fact]
    public void BackwardStep_refusesTheLeaseUntilAFreshQuorumAck()
    {
        var leaderClock = new StepClock();
        var followerClock = new StepClock();
        var transport = new InMemoryTransport(leaderClock);
        var leader = new RaftNode(
            "n1",
            ["n2"],
            leaderClock,
            transport,
            new InMemoryPersistentState(),
            new InMemoryRaftLog(),
            TimeSpan.FromMilliseconds(200),
            TimeSpan.FromMilliseconds(1_000),
            IStateMachine.NoOp());
        var follower = new RaftNode(
            "n2",
            ["n1"],
            followerClock,
            transport,
            new InMemoryPersistentState(),
            new InMemoryRaftLog(),
            TimeSpan.FromMilliseconds(10_000),
            Heartbeat,
            IStateMachine.NoOp());
        leader.SetElectionJitter(static timeout => timeout);
        follower.SetElectionJitter(static timeout => timeout);
        leader.SetClockDriftBound(0);
        transport.Register("n1", leader.Receive);
        transport.Register("n2", follower.Receive);
        leader.Start();
        follower.Start();
        for (int i = 0; i < 40 && leader.Role != Role.Leader; i++)
        {
            leaderClock.Advance(10);
            leader.Drain();
            follower.Drain();
            leader.Drain();
        }

        Assert.Equal(Role.Leader, leader.Role);
        Assert.NotNull(leader.Propose("v"u8.ToArray()));
        leader.Drain();
        follower.Drain();
        leader.Drain();
        Assert.True(leader.ReadIndexSatisfied());
        Assert.True(leader.QuorumLeaseValid);

        leaderClock.Jump(-1_000);
        Assert.False(leader.ReadIndexSatisfied());
        Assert.False(leader.QuorumLeaseValid);
        Assert.Equal(Role.Leader, leader.Role);
        var client = new KvClient(leader, new InMemoryKvStore());
        Assert.Null(client.LinearizableGet("k"));
        Assert.Null(client.BoundedStaleGet("k"));

        leaderClock.Advance(1_000);
        leader.Drain();
        Assert.False(leader.ReadIndexSatisfied());
        followerClock.Advance(1_000);
        follower.Drain();
        leader.Drain();
        follower.Drain();
        leader.Drain();
        Assert.True(leader.QuorumLeaseValid);
        Assert.True(leader.ReadIndexSatisfied());
    }

    private static ClusterHarness Heard(long advertised)
    {
        var cluster = new ClusterHarness();
        cluster.Transport.Register("n2", _ => { });
        cluster.Transport.Register("n3", _ => { });
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100_000), Heartbeat);
        cluster.Transport.Send("n2", "n1", new AppendEntries(1, "n2", 0, 0, [], 0, 1, advertised));
        cluster.DrainAll();
        Assert.Equal("n2", cluster.Node("n1").LeaderId);
        Assert.Equal(Role.Follower, cluster.Node("n1").Role);
        return cluster;
    }

    private static ClusterHarness Joint()
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
        Assert.True(cluster.Node("n1").InJointConsensus);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        return cluster;
    }

    private sealed class StepClock : IRaftClock
    {
        private readonly List<Action> _listeners = [];
        private long _millis;

        public long Millis => _millis;

        public void Advance(long delta)
        {
            if (delta <= 0)
            {
                throw new ArgumentException("delta must be positive (got " + delta + ")");
            }

            _millis += delta;
            Fire();
        }

        public void Jump(long delta)
        {
            long next = _millis + delta;
            if (next < 0)
            {
                next = 0;
            }

            if (next == _millis)
            {
                return;
            }

            _millis = next;
            Fire();
        }

        public void OnAdvance(Action listener)
        {
            ArgumentNullException.ThrowIfNull(listener);
            _listeners.Add(listener);
        }

        private void Fire()
        {
            foreach (Action listener in _listeners.ToArray())
            {
                listener();
            }
        }
    }
}
