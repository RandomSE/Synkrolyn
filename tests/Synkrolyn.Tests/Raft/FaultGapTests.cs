using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

/// <summary>Phase 13 coverage for the three fault gaps the audit left open.</summary>
public class FaultGapTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void CrashMidInstallSnapshot_restartsAndRejectsStaleFinalChunk()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        Assert.NotNull(cluster.Propose("n1", "alpha-value-persisted"u8.ToArray()));
        Assert.NotNull(cluster.Propose("n1", "beta-value-that-is-longer"u8.ToArray()));
        cluster.DrainAll();
        RaftNode leader = cluster.Node("n1");
        leader.Snapshot();
        cluster.DrainAll();
        byte[] snap = cluster.Log("n1").SnapshotBytes();
        Assert.True(snap.Length > 16, "snapshot length " + snap.Length);
        long index = cluster.Log("n1").LastIncludedIndex;
        long snapTerm = cluster.Log("n1").LastIncludedTerm;
        long leaderTerm = leader.CurrentTerm;
        byte[] first = snap[..8];
        byte[] rest = snap[8..];

        string dir = Directory.CreateTempSubdirectory("synkrolyn-install-crash-").FullName;
        try
        {
            InstallSnapshotResponse? reply = null;
            cluster.Transport.Reregister("n1", envelope =>
            {
                if (envelope.Payload is InstallSnapshotResponse response)
                {
                    reply = response;
                    return;
                }

                leader.Receive(envelope);
            });

            var log = new FileRaftLog(dir);
            var follower = OpenFollower(cluster, log, new InMemoryPersistentState());
            follower.Receive(Chunk(leaderTerm, index, snapTerm, 0, first, false));
            follower.Drain();
            Assert.NotNull(reply);
            Assert.True(reply.Success);
            Assert.False(reply.Done);
            Assert.Equal(first.Length, reply.NextOffset);
            Assert.Equal(0, log.LastIncludedIndex);

            log.Dispose();
            File.WriteAllBytes(Path.Combine(dir, FileRaftLog.SnapshotFileName), first);
            log = new FileRaftLog(dir);
            try
            {
                Assert.Equal(0, log.LastIncludedIndex);
                follower = OpenFollower(cluster, log, new InMemoryPersistentState());
                reply = null;
                follower.Receive(Chunk(leaderTerm, index, snapTerm, first.Length, rest, true));
                follower.Drain();
                Assert.Equal(0, log.LastIncludedIndex);
                Assert.NotNull(reply);
                Assert.False(reply.Done);
                Assert.False(reply.Success);

                reply = null;
                follower.Receive(Chunk(leaderTerm, index, snapTerm, 0, first, false));
                follower.Drain();
                Assert.NotNull(reply);
                Assert.True(reply.Success);
                Assert.False(reply.Done);
                Assert.Equal(first.Length, reply.NextOffset);
                follower.Receive(Chunk(leaderTerm, index, snapTerm, first.Length, rest, true));
                follower.Drain();
                Assert.False(follower.Fatal);
                Assert.Equal(index, log.LastIncludedIndex);
                Assert.Equal(snapTerm, log.LastIncludedTerm);
            }
            finally
            {
                log.Dispose();
            }
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void OneWayPartition_pastElectionTimeout_oneLeaderPerTerm_andNoStaleLeaseRead()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.Advance(80);
        RaftNode stale = cluster.Node("n1");
        Assert.Equal(Role.Leader, stale.Role);
        Assert.NotNull(cluster.Propose("n1", "v"u8.ToArray()));
        cluster.DrainAll();
        Assert.True(stale.ReadIndexSatisfied());
        Assert.True(stale.QuorumLeaseValid);

        RaftNode n2 = cluster.Node("n2");
        RaftNode n3 = cluster.Node("n3");
        cluster.Transport.Reregister("n2", envelope =>
        {
            if (envelope.From != "n1")
            {
                n2.Receive(envelope);
            }
        });
        cluster.Transport.Reregister("n3", envelope =>
        {
            if (envelope.From != "n1")
            {
                n3.Receive(envelope);
            }
        });

        var leaders = new Dictionary<long, string>();
        for (int i = 0; i < 30; i++)
        {
            cluster.Advance(10);
            cluster.AssertAtMostOneLeaderPerTerm();
            foreach (string id in new[] { "n1", "n2", "n3" })
            {
                RaftNode node = cluster.Node(id);
                if (node.Role != Role.Leader)
                {
                    continue;
                }

                if (leaders.TryGetValue(node.CurrentTerm, out string? prior))
                {
                    Assert.Equal(prior, id);
                }
                else
                {
                    leaders[node.CurrentTerm] = id;
                }
            }
        }

        Assert.False(stale.QuorumLeaseValid);
        Assert.False(stale.ReadIndexSatisfied());
        Assert.True(leaders.Count >= 1);
    }

    private static RaftNode OpenFollower(ClusterHarness cluster, FileRaftLog log, IPersistentState state)
    {
        var follower = new RaftNode(
            "n3",
            ["n1"],
            cluster.Clock,
            cluster.Transport,
            state,
            log,
            TimeSpan.FromMilliseconds(10_000),
            Heartbeat,
            IStateMachine.NoOp());
        follower.SetElectionJitter(static timeout => timeout);
        follower.SnapshotThreshold = 0;
        if (cluster.Transport.HasPeer("n3"))
        {
            cluster.Transport.Reregister("n3", follower.Receive);
        }
        else
        {
            cluster.Transport.Register("n3", follower.Receive);
        }

        follower.Start();
        return follower;
    }

    private static Envelope Chunk(
        long leaderTerm,
        long index,
        long snapTerm,
        long offset,
        byte[] data,
        bool done) =>
        new("n1", "n3", new InstallSnapshot(leaderTerm, "n1", index, snapTerm, offset, data, done));
}
