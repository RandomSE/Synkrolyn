using System.Reflection;
using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;
using Synkrolyn.Tests.Kv;
using Synkrolyn.Tests.Net;

namespace Synkrolyn.Tests.Raft;

/// <summary>
/// Behavior the phase 1-3 review required. Each test fails on the pre-fix node.
/// </summary>
public class Phase123ReviewTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void Item1_applyLag_doesNotDemoteAPromotedServer()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("a", ["b", "c"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("b", ["a", "c"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("c", ["a", "b"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("d", ["a", "b", "c"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("e", ["a", "b", "c"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        RaftNode leader = cluster.Node("a");
        Action? blocked = null;
        leader.SetApplyExecutor(action => blocked = action);
        Assert.NotNull(leader.Propose("user1"u8.ToArray()));
        cluster.DrainAll();
        Assert.NotNull(blocked);
        Assert.NotNull(leader.AddLearner("d"));
        cluster.DrainAll();
        Assert.NotNull(leader.Propose("user2"u8.ToArray()));
        cluster.DrainAll();
        Assert.NotNull(leader.PromoteVoter("d"));
        cluster.DrainAll();
        Assert.Contains("d", leader.VoterPeerIds);
        blocked!();
        leader.Drain();
        Assert.Contains("d", leader.VoterPeerIds);
    }

    [Fact]
    public void Item1_restart_keepsUncommittedPromotion()
    {
        var cluster = new ClusterHarness();
        var state = new InMemoryPersistentState();
        var log = new InMemoryRaftLog();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat, state: state, log: log);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.NotNull(cluster.Node("n1").AddLearner("n4"));
        cluster.DrainAll();
        cluster.Isolate("n2");
        cluster.Isolate("n3");
        Assert.NotNull(cluster.Node("n1").PromoteVoter("n4"));
        cluster.Node("n1").Drain();
        Assert.Contains("n4", cluster.Node("n1").VoterPeerIds);

        var restarted = new RaftNode(
            "n1",
            ["n2", "n3"],
            cluster.Clock,
            cluster.Transport,
            state,
            log,
            TimeSpan.FromMilliseconds(80),
            Heartbeat);
        cluster.Transport.Reregister("n1", restarted.Receive);
        long committed = cluster.Node("n1").CommitIndex;
        Assert.True(log.LastIndex > committed);
        restarted.Start();
        restarted.Receive(new Envelope(
            "n2",
            "n1",
            new AppendEntries(restarted.CurrentTerm, "n2", log.LastIndex, log.LastTerm, [], committed)));
        restarted.Drain();
        Assert.Contains("n4", restarted.VoterPeerIds);
    }

    [Fact]
    public void Item1_twoValuesAtOneIndex_runsTheMembershipBranch()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("a", ["b", "c"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("b", ["a", "c"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("c", ["a", "b"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        cluster.AddNode("d", ["a", "b", "c"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        Assert.NotNull(cluster.Node("a").AddLearner("d"));
        cluster.DrainAll();
        Assert.NotNull(cluster.Node("a").PromoteVoter("d"));
        cluster.DrainAll();
        Assert.Contains("d", cluster.Node("a").VoterPeerIds);
        long? removed = cluster.Node("a").RemoveServer("c");
        Assert.NotNull(removed);
        cluster.DrainAll();
        long horizon = Math.Max(cluster.Log("a").LastIndex, cluster.Log("c").LastIndex);
        Assert.True(horizon >= removed.Value);
        for (long index = 1; index <= horizon; index++)
        {
            byte[]? left = CommandIfCommitted(cluster, "a", index);
            byte[]? right = CommandIfCommitted(cluster, "c", index);
            if (left is not null && right is not null)
            {
                Assert.Equal(left, right);
            }
        }
    }

    [Fact]
    public void Item2_requestVoteInsideElectionTimeout_isDisregarded()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(150), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(150);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        cluster.Advance(100);
        long term = cluster.Node("n2").CurrentTerm;
        cluster.Node("n2").Receive(new Envelope(
            "n3",
            "n2",
            new RequestVote(term + 1, "n3", 1, 1, true, 1)));
        cluster.Node("n2").Drain();
        Assert.False(cluster.Node("n2").LastPreVoteGranted);
        Assert.Equal(term, cluster.Node("n2").CurrentTerm);
        cluster.Node("n2").Receive(new Envelope(
            "n3",
            "n2",
            new RequestVote(term + 5, "n3", 1, 1)));
        cluster.Node("n2").Drain();
        Assert.Equal(term, cluster.Node("n2").CurrentTerm);
    }

    [Fact]
    public void Item2_timeoutNow_invalidatesTheLease()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(150), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(150);
        cluster.Propose("n1", "k"u8.ToArray());
        cluster.DrainAll();
        Assert.True(cluster.Node("n1").QuorumLeaseValid);
        Assert.True(cluster.Node("n1").TransferLeadership("n2"));
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        Assert.False(cluster.Node("n1").QuorumLeaseValid);
    }

    [Fact]
    public void Item11_leaseLastsTheElectionTimeout()
    {
        var heartbeat = TimeSpan.FromMilliseconds(40);
        using var cluster = new KvClusterTests.KvFixture(heartbeat);
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(150));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000));
        cluster.Harness.Advance(150);
        cluster.Clients["n1"].Put("k", "v");
        cluster.Harness.DrainAll();
        cluster.Harness.Transport.PartitionBidirectional("n1", "n2");
        cluster.Harness.Transport.PartitionBidirectional("n1", "n3");
        cluster.Harness.Advance(100);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
        Assert.True(cluster.Harness.Node("n1").QuorumLeaseValid);
        cluster.Dispose();
    }

    [Fact]
    public void Item3_joiningRead_isNotServedTheOlderIndex()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.NotNull(cluster.Propose("n1", "old"u8.ToArray()));
        cluster.DrainAll();
        RaftNode leader = cluster.Node("n1");
        cluster.Transport.DelayNext("n2", "n1", TimeSpan.FromMilliseconds(10_000));
        long? firstIndex = leader.BeginReadIndex();
        Assert.NotNull(firstIndex);
        long? appended = leader.Propose("new"u8.ToArray());
        Assert.NotNull(appended);
        leader.CommitIndexForTest(appended.Value);
        long? secondIndex = leader.BeginReadIndex();
        Assert.Equal(appended, secondIndex);
        Assert.True(secondIndex > firstIndex);
    }

    [Fact]
    public void Item3_confirmedTicket_survivesTheNextRead()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.NotNull(cluster.Propose("n1", "v"u8.ToArray()));
        cluster.DrainAll();
        RaftNode leader = cluster.Node("n1");
        long? firstIndex = leader.BeginReadIndex();
        long firstTicket = leader.CurrentReadTicket;
        Assert.NotNull(firstIndex);
        cluster.DrainAll();
        Assert.NotNull(leader.BeginReadIndex());
        Assert.True(leader.ReadIndexSatisfied(firstTicket));
    }

    [Fact]
    public void Item3_kvClient_readIndexSucceedsWhenLeaseIsExpired()
    {
        var heartbeat = TimeSpan.FromMilliseconds(40);
        using var cluster = new KvClusterTests.KvFixture(heartbeat);
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(150));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000));
        cluster.Harness.Advance(150);
        cluster.Clients["n1"].Put("k", "v");
        cluster.Harness.DrainAll();
        cluster.Harness.Transport.SetLinkDelay("n2", "n1", TimeSpan.FromMilliseconds(160));
        cluster.Harness.Transport.SetLinkDelay("n3", "n1", TimeSpan.FromMilliseconds(160));
        cluster.Harness.Advance(40);
        cluster.Harness.Advance(160);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
        Assert.False(cluster.Harness.Node("n1").QuorumLeaseValid);
        Assert.Null(cluster.Clients["n1"].LinearizableGet("k"));
        cluster.Harness.Advance(160);
        cluster.Harness.Advance(160);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
        Assert.False(cluster.Harness.Node("n1").QuorumLeaseValid);
        Assert.Equal("v", cluster.Clients["n1"].LinearizableGet("k"));
        cluster.Dispose();
    }

    [Fact]
    public void Item4_multiChunkSnapshot_completesAcrossHeartbeats()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        cluster.Node("n1").SnapshotChunkSize = 8;
        for (int i = 0; i < 6; i++)
        {
            Assert.NotNull(cluster.Propose("n1", [(byte)i]));
            cluster.DrainAll();
        }

        cluster.Node("n1").Snapshot();
        cluster.DrainAll();
        cluster.ReplaceNode(
            "n2",
            ["n1"],
            TimeSpan.FromMilliseconds(10_000),
            Heartbeat,
            IStateMachine.NoOp(),
            new InMemoryPersistentState(),
            new InMemoryRaftLog());
        cluster.Transport.SetLinkDelay("n2", "n1", TimeSpan.FromMilliseconds(30));
        for (int i = 0; i < 400; i++)
        {
            cluster.Advance(20);
            if (cluster.Log("n2").LastIncludedIndex >= cluster.Log("n1").LastIncludedIndex)
            {
                break;
            }
        }

        Assert.True(cluster.Log("n2").LastIncludedIndex >= cluster.Log("n1").LastIncludedIndex);
    }

    [Fact]
    public void Item5_mailboxOverflow_dropsInboundRpc_andKeepsFinishApply()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(10_000);
        RaftNode node = cluster.Node("n1");
        var noise = new AppendEntries(1, "n2", 0, 0, [], 0, 1);
        for (int i = 0; i < 20_000; i++)
        {
            node.Receive(new Envelope("n2", "n1", noise));
        }

        Assert.False(node.Fatal);
        Assert.Equal(Role.Leader, node.Role);
        Assert.NotNull(node.Propose("kept"u8.ToArray()));
        node.Drain();
        Assert.False(node.Fatal);
    }

    [Fact]
    public void Item6_oversizedCommand_isRejected()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        byte[] huge = new byte[FrameCodec.DefaultMaxPayloadBytes];
        Assert.Null(cluster.Node("n1").Propose(huge));
        Assert.False(cluster.Node("n1").Fatal);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
    }

    [Fact]
    public void Item6_kvStoreLargerThanOneRecord_reopens()
    {
        string dir = Directory.CreateTempSubdirectory("synkrolyn-kv-").FullName;
        try
        {
            var store = new FileKvStore(dir);
            store.Apply(1, KvCommandCodec.EncodePut("k", new string('x', ChecksummedRecords.MaxPayloadBytes)));
            store.Dispose();
            using var reopened = new FileKvStore(dir);
            Assert.Equal(new string('x', ChecksummedRecords.MaxPayloadBytes), reopened.Get("k"));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Item7_staleFailure_stillBacksUpUnderLoad()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        for (int i = 0; i < 4; i++)
        {
            Assert.NotNull(cluster.Propose("n1", [(byte)i]));
        }

        cluster.Transport.PartitionBidirectional("n1", "n2");
        cluster.Node("n1").StepDownForTest();
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        Assert.NotNull(cluster.Propose("n1", [9]));
        long nextBefore = cluster.Node("n1").NextIndex("n2");
        Assert.True(
            nextBefore > cluster.Node("n1").MatchIndex("n2") + 1,
            "next=" + nextBefore + " match=" + cluster.Node("n1").MatchIndex("n2"));
        cluster.Node("n1").Receive(new Envelope(
            "n2",
            "n1",
            new AppendEntriesResponse(cluster.Node("n1").CurrentTerm, false, 0, 0, 0, 0, 1, nextBefore - 1)));
        cluster.Node("n1").Drain();
        Assert.True(cluster.Node("n1").NextIndex("n2") < nextBefore);
    }

    [Fact]
    public void Item8_inFlightStamps_areNotEvictedInsideTheLease()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(10_000);
        cluster.Transport.PartitionBidirectional("n1", "n2");
        for (int i = 0; i < 40; i++)
        {
            Assert.NotNull(cluster.Propose("n1", [(byte)i]));
            cluster.Node("n1").Drain();
        }

        Assert.True(cluster.Node("n1").PendingAeStampCount("n2") >= 40);
    }

    [Fact]
    public void Item9_unknownPeer_isDropped()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Exception? error = Record.Exception(() => cluster.Node("n1").AddLearner("missing"));
        Assert.Null(error);
        Assert.False(cluster.Node("n1").Fatal);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
    }

    [Fact]
    public void Item10_installSnapshot_replaysSuffixConfig()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.NotNull(cluster.Propose("n1", "a"u8.ToArray()));
        cluster.DrainAll();
        long cappedCommit = cluster.Node("n3").CommitIndex;
        RaftNode n3 = cluster.Node("n3");
        cluster.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is AppendEntries append && append.LeaderCommit > cappedCommit)
            {
                n3.Receive(new Envelope(
                    envelope.From,
                    envelope.To,
                    new AppendEntries(
                        append.Term,
                        append.LeaderId,
                        append.PrevLogIndex,
                        append.PrevLogTerm,
                        append.Entries,
                        cappedCommit,
                        append.Stamp)));
                return;
            }

            n3.Receive(envelope);
        });
        Assert.NotNull(cluster.Propose("n1", "b"u8.ToArray()));
        cluster.DrainAll();
        Assert.NotNull(cluster.Node("n1").AddLearner("n4"));
        cluster.DrainAll();
        Assert.Equal(cappedCommit, n3.CommitIndex);
        Assert.Contains("n4", n3.LearnerPeerIds);
        Assert.True(cluster.Log("n3").LastIndex > cappedCommit + 1);
        cluster.Node("n1").SnapshotThrough(cappedCommit + 1);
        cluster.DrainAll();
        cluster.Transport.PartitionBidirectional("n1", "n3");
        cluster.Node("n1").StepDownForTest();
        cluster.Advance(80);
        RaftNode leader = cluster.Node("n1");
        Assert.Equal(Role.Leader, leader.Role);
        int guard = 0;
        while (leader.NextIndex("n3") >= cluster.Log("n1").FirstIndex && guard < 40)
        {
            leader.Receive(new Envelope(
                "n3",
                "n1",
                new AppendEntriesResponse(leader.CurrentTerm, false, 0)));
            leader.Drain();
            guard++;
        }

        Assert.True(leader.NextIndex("n3") < cluster.Log("n1").FirstIndex);
        cluster.Transport.HealBidirectional("n1", "n3");
        cluster.Advance(20);
        cluster.DrainAll();
        Assert.Contains("n4", cluster.Node("n3").LearnerPeerIds);
    }

    [Fact]
    public void Item16_secondComponentOnSameDirectory_isRejected()
    {
        string dir = Directory.CreateTempSubdirectory("synkrolyn-dir-").FullName;
        try
        {
            using var log = new FileRaftLog(dir);
            using var store = new FileKvStore(dir);
            using var state = new FilePersistentState(dir);
            Assert.Throws<IOException>(() =>
            {
                using var foreign = new FileStream(
                    Path.Combine(dir, "directory.lock"),
                    FileMode.OpenOrCreate,
                    FileAccess.ReadWrite,
                    FileShare.None);
            });
            Assert.Throws<IOException>(() => new FileRaftLog(dir));
            Assert.Equal(0, store.PersistCount);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Item16_secondJoint_stillAutoLeaves()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.NotNull(cluster.Node("n1").AddLearner("n4"));
        cluster.DrainAll();
        Assert.NotNull(cluster.Node("n1").PromoteVoter("n4"));
        cluster.DrainAll();
        Assert.False(cluster.Node("n1").InJointConsensus);
        Assert.NotNull(cluster.Node("n1").EnterJoint(["n1", "n2", "n4"]));
        cluster.DrainAll();
        cluster.Advance(20);
        Assert.False(cluster.Node("n1").InJointConsensus);
        Assert.NotNull(cluster.Node("n1").EnterJoint(["n1", "n4"]));
        cluster.DrainAll();
        cluster.Advance(20);
        Assert.False(cluster.Node("n1").InJointConsensus);
        Assert.Contains("n4", cluster.Node("n1").VoterPeerIds);
        Assert.DoesNotContain("n3", cluster.Node("n1").VoterPeerIds);
    }

    [Fact]
    public void Item16_socketFixture_staysNearTheHostTimeout()
    {
        Assert.Equal(120, SocketCluster.ElectionTimeouts[0].TotalMilliseconds);
        Assert.Equal(350, SocketCluster.ElectionTimeouts[1].TotalMilliseconds);
        Assert.Equal(600, SocketCluster.ElectionTimeouts[2].TotalMilliseconds);
    }

    [Fact]
    public void Item12_windowsReplace_usesWriteThrough()
    {
        MethodInfo? method = typeof(FilePersistentState).GetMethod(
            "MoveFileEx",
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
    }

    [Fact]
    public void Item13_compactionWrite_doesNotBlockDrain()
    {
        var cluster = new ClusterHarness();
        var log = new BlockingCompactLog();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat, log: log);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.NotNull(cluster.Propose("n1", "a"u8.ToArray()));
        cluster.DrainAll();
        RaftNode leader = cluster.Node("n1");
        leader.SetSnapshotExecutor(action => ThreadPool.QueueUserWorkItem(_ => action()));
        log.Arm();
        try
        {
            leader.Snapshot();
            var drained = new Thread(() => leader.Drain());
            drained.Start();
            bool finished = drained.Join(400);
            Assert.True(finished);
        }
        finally
        {
            log.Release.Set();
        }
    }

    [Fact]
    public void Item16_applyWaiterAndFatal_areVolatile()
    {
        AssertVolatile(typeof(RaftNode), "_fatal");
        AssertVolatile(typeof(RaftNode.ApplyWaiter), "_applied");
        AssertVolatile(typeof(RaftNode.ApplyWaiter), "_failed");
    }

    [Fact]
    public void Item2_staggeredContact_failoverDoesNotOverlapTheLease()
    {
        var heartbeat = TimeSpan.FromMilliseconds(40);
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(150), heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(150), heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(150), heartbeat);
        cluster.Advance(150);
        Assert.NotNull(cluster.Propose("n1", "k"u8.ToArray()));
        cluster.DrainAll();
        cluster.Transport.PartitionBidirectional("n1", "n3");
        cluster.Advance(40);
        cluster.Transport.PartitionBidirectional("n1", "n2");
        long elapsed = 0;
        while (elapsed < 2000
            && cluster.Node("n2").Role != Role.Leader
            && cluster.Node("n3").Role != Role.Leader)
        {
            cluster.Advance(10);
            elapsed += 10;
        }

        bool otherLeader = cluster.Node("n2").Role == Role.Leader || cluster.Node("n3").Role == Role.Leader;
        Assert.True(otherLeader, "failover elapsed " + elapsed);
        Assert.False(cluster.Node("n1").QuorumLeaseValid);
        Assert.True(elapsed <= 150 + 40, "failover elapsed " + elapsed);
    }

    private static void AssertVolatile(Type type, string fieldName)
    {
        FieldInfo? field = type.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        Assert.Contains(field!.GetRequiredCustomModifiers(), static modifier => modifier.Name == "IsVolatile");
    }

    private static byte[]? CommandIfCommitted(ClusterHarness cluster, string id, long index)
    {
        RaftNode node = cluster.Node(id);
        if (node.CommitIndex < index || cluster.Log(id).LastIndex < index || index < cluster.Log(id).FirstIndex)
        {
            return null;
        }

        return cluster.Log(id).Read(index).Command;
    }

    /// <summary>Blocks inside <see cref="CompactThrough"/> so a drain that compacts cannot return.</summary>
    private sealed class BlockingCompactLog : IRaftLog
    {
        private readonly InMemoryRaftLog _inner = new();

        public ManualResetEventSlim Release { get; } = new(true);

        public void Arm()
        {
            Release.Reset();
        }

        public void Append(LogEntry entry) => _inner.Append(entry);

        public LogEntry Read(long index) => _inner.Read(index);

        public long LastIndex => _inner.LastIndex;

        public long LastTerm => _inner.LastTerm;

        public long DurableIndex => _inner.DurableIndex;

        public void TruncateFrom(long index) => _inner.TruncateFrom(index);

        public long LastIncludedIndex => _inner.LastIncludedIndex;

        public long LastIncludedTerm => _inner.LastIncludedTerm;

        public byte[] SnapshotBytes() => _inner.SnapshotBytes();

        public void CompactThrough(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot)
        {
            Release.Wait();
            _inner.CompactThrough(lastIncludedIndex, lastIncludedTerm, snapshot);
        }

        public StagedCompaction StageCompaction(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot)
        {
            return new StagedCompaction(
                () => Release.Wait(),
                () => _inner.CompactThrough(lastIncludedIndex, lastIncludedTerm, snapshot));
        }

        public void Force() => _inner.Force();
    }
}
