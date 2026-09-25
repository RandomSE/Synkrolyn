using Synkrolyn.Kv;
using Synkrolyn.Raft;
using Synkrolyn.Tests.Kv;

namespace Synkrolyn.Tests.Raft;

public sealed class SnapshotClusterTests : IDisposable
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);
    private readonly KvClusterTests.KvFixture _cluster = new(Heartbeat);

    [Fact]
    public void ThreeNode_leaderSnapshot_compactedIndexesUnreadable_laterPutStillServes()
    {
        Elect();
        Put("a", "1");
        Put("b", "2");
        Put("c", "3");
        _cluster.Harness.Advance(20);
        Assert.Equal(4, _cluster.Harness.Node("n1").LastApplied);
        _cluster.Harness.Node("n1").Snapshot();
        _cluster.Harness.DrainAll();
        Assert.Equal(4, _cluster.Harness.Log("n1").LastIncludedIndex);
        Assert.Contains("compacted", Assert.Throws<InvalidOperationException>(() => _cluster.Harness.Log("n1").Read(1)).Message);
        Put("d", "4");
        _cluster.Harness.Advance(20);
        Assert.Equal("4", _cluster.Clients["n1"].Get("d"));
        Assert.Equal("4", _cluster.Clients["n2"].Get("d"));
    }

    [Fact]
    public void FarBehindEmptyFollower_catchesUpViaInstallSnapshot()
    {
        Elect();
        for (int i = 1; i <= 5; i++)
        {
            Put("k" + i, "v" + i);
        }

        _cluster.Harness.Advance(20);
        _cluster.Harness.Node("n1").Snapshot();
        _cluster.Harness.DrainAll();
        Assert.Equal(6, _cluster.Harness.Log("n1").LastIncludedIndex);
        ReplaceEmpty("n3");
        Assert.Equal(0, _cluster.Harness.Log("n3").LastIncludedIndex);
        _cluster.Harness.Advance(40);
        Assert.True(_cluster.Harness.Node("n3").SnapshotsInstalled >= 1);
        Assert.Equal(6, _cluster.Harness.Log("n3").LastIncludedIndex);
        Assert.Equal("v1", _cluster.Clients["n3"].Get("k1"));
        Assert.Equal("v5", _cluster.Clients["n3"].Get("k5"));
    }

    [Fact]
    public void MultiChunkInstallSnapshot_restoresSameMap()
    {
        Elect();
        _cluster.Harness.Node("n1").SnapshotChunkSize = 8;
        Put("city", "thessaloniki");
        Put("country", "greece");
        _cluster.Harness.Advance(20);
        _cluster.Harness.Node("n1").Snapshot();
        _cluster.Harness.DrainAll();
        ReplaceEmpty("n3");
        _cluster.Harness.Advance(40);
        Assert.True(_cluster.Harness.Node("n3").SnapshotChunksReceived > 1);
        Assert.True(_cluster.Harness.Node("n3").SnapshotsInstalled >= 1);
        Assert.Equal("thessaloniki", _cluster.Clients["n3"].Get("city"));
        Assert.Equal("greece", _cluster.Clients["n3"].Get("country"));
    }

    [Fact]
    public void SnapshotRefusesUncommittedSuffixAndIndexPastLastApplied()
    {
        Elect();
        Put("k", "committed");
        _cluster.Harness.Advance(20);
        Assert.Equal(2, _cluster.Harness.Node("n1").LastApplied);
        _cluster.Harness.Isolate("n1");
        _cluster.Clients["n1"].Put("k", "uncommitted");
        _cluster.Harness.DrainAll();
        Assert.Equal(3, _cluster.Harness.Log("n1").LastIndex);
        Assert.Throws<ArgumentException>(() => _cluster.Harness.Node("n1").SnapshotThrough(3));
        _cluster.Harness.Node("n1").Snapshot();
        Assert.Equal(2, _cluster.Harness.Log("n1").LastIncludedIndex);
        Assert.Equal(3, _cluster.Harness.Log("n1").LastIndex);
    }

    public void Dispose() => _cluster.Dispose();

    private void Elect()
    {
        _cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        _cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        _cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        _cluster.Harness.Advance(80);
        Assert.Equal(Role.Leader, _cluster.Harness.Node("n1").Role);
    }

    private void Put(string key, string value)
    {
        _cluster.Clients["n1"].Put(key, value);
        _cluster.Harness.DrainAll();
        Assert.NotNull(_cluster.Clients["n1"].AwaitCommitted());
    }

    private void ReplaceEmpty(string id)
    {
        var store = new InMemoryKvStore();
        _cluster.Stores[id] = store;
        var node = _cluster.Harness.ReplaceNode(
            id, ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat, store, new InMemoryPersistentState(), new InMemoryRaftLog());
        _cluster.Clients[id] = new KvClient(node, store);
    }
}

public sealed class SafetyRepairTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void InstallSnapshot_staleDoneDoesNotJumpMatchToNewerSnapshot()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        cluster.Isolate("n3");
        cluster.Propose("n1", "a"u8.ToArray());
        cluster.Propose("n1", "b"u8.ToArray());
        cluster.Propose("n1", "c"u8.ToArray());
        cluster.Node("n1").Snapshot();
        long installed = cluster.Log("n1").LastIncludedIndex;
        Assert.True(installed > 1);
        cluster.Transport.DelayNext("n3", "n1", TimeSpan.FromMilliseconds(50));
        cluster.Heal("n3");
        cluster.Advance(20);
        Assert.True(cluster.Node("n3").SnapshotsInstalled >= 1);
        Assert.Equal(installed, cluster.Log("n3").LastIncludedIndex);
        cluster.Propose("n1", "d"u8.ToArray());
        cluster.Propose("n1", "e"u8.ToArray());
        cluster.Node("n1").Snapshot();
        long newer = cluster.Log("n1").LastIncludedIndex;
        Assert.True(newer > installed);
        RaftNode stalled = cluster.Node("n3");
        cluster.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is InstallSnapshot)
            {
                return;
            }

            stalled.Receive(envelope);
        });
        cluster.Advance(50);
        Assert.Equal(installed, cluster.Log("n3").LastIncludedIndex);
        Assert.True(cluster.Node("n1").MatchIndex("n3") <= installed);
        Assert.True(cluster.Node("n1").MatchIndex("n3") < newer);
    }

    [Fact]
    public void LeaseFalseBeforeAppendAck_checkQuorumGraceKeepsLeader()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        RaftNode leader = cluster.Node("n1");
        cluster.Transport.Reregister("n1", envelope =>
        {
            if (envelope.Payload is AppendEntriesResponse)
            {
                return;
            }

            leader.Receive(envelope);
        });
        cluster.Advance(80);
        Assert.Equal(Role.Leader, leader.Role);
        Assert.Equal(0, leader.CommitIndex);
        leader.CommitIndexForTest(cluster.Log("n1").LastIndex);
        Assert.False(leader.QuorumLeaseValid);
        cluster.Advance(30);
        Assert.Equal(Role.Leader, leader.Role);
        Assert.False(leader.QuorumLeaseValid);
    }

    [Fact]
    public void TransferBehindPeer_noTimeoutNowUntilCaughtUp()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        int timeouts = 0;
        RaftNode n2 = cluster.Node("n2");
        cluster.Transport.Reregister("n2", envelope =>
        {
            if (envelope.Payload is TimeoutNow)
            {
                timeouts++;
            }

            if (envelope.Payload is AppendEntries)
            {
                return;
            }

            n2.Receive(envelope);
        });
        cluster.Propose("n1", "behind"u8.ToArray());
        Assert.True(cluster.Log("n1").LastIndex > cluster.Log("n2").LastIndex);
        Assert.True(cluster.Node("n1").TransferLeadership("n2"));
        cluster.DrainAll();
        Assert.Equal(0, timeouts);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        Assert.Equal(0, timeouts);
        cluster.Transport.Reregister("n2", n2.Receive);
        cluster.Advance(20);
        Assert.Equal(0, timeouts);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        Assert.NotEqual(Role.Leader, cluster.Node("n2").Role);
    }

    [Fact]
    public void LinearizableGet_doesNotExtendLeaseFromAckArrival()
    {
        var cluster = new KvClusterTests.KvFixture(Heartbeat);
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        cluster.Harness.Advance(80);
        var leader = new KvClient(cluster.Harness.Node("n1"), cluster.Stores["n1"]);
        leader.Put("k", "v");
        cluster.Harness.DrainAll();
        long delay = 75;
        cluster.Harness.Transport.DelayNext("n2", "n1", TimeSpan.FromMilliseconds(delay));
        cluster.Harness.Transport.DelayNext("n3", "n1", TimeSpan.FromMilliseconds(delay));
        cluster.Harness.Advance(20);
        cluster.Harness.Transport.DelayNext("n2", "n1", TimeSpan.FromMilliseconds(10_000));
        cluster.Harness.Transport.DelayNext("n3", "n1", TimeSpan.FromMilliseconds(10_000));
        cluster.Harness.Advance(delay);
        cluster.Harness.Advance(10);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
        Assert.True(cluster.Harness.Node("n1").QuorumLeaseValid);
        cluster.Harness.Transport.DelayNext("n2", "n1", TimeSpan.FromMilliseconds(10_000));
        cluster.Harness.Transport.DelayNext("n3", "n1", TimeSpan.FromMilliseconds(10_000));
        Assert.Null(leader.LinearizableGet("k"));
        cluster.Dispose();
    }
}

public sealed class ElectionUnavailabilityTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void IsolateLeader_successorCommitsNewPut_windowWithinPaperBound()
    {
        var cluster = new KvClusterTests.KvFixture(Heartbeat);
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(200));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        cluster.Harness.Advance(80);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
        cluster.Clients["n1"].Put("before", "yes");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);

        long t0 = cluster.Harness.Clock.Millis;
        cluster.Harness.Isolate("n1");
        Window? window = null;
        while (cluster.Harness.Clock.Millis - t0 <= 400)
        {
            window = TrySuccessor(cluster, t0);
            if (window is not null)
            {
                break;
            }

            cluster.Harness.Advance(1);
        }

        Assert.NotNull(window);
        Assert.True(window.Millis >= 200);
        Assert.True(window.Millis < 400);
        Assert.True(window.Millis <= 220);
        Assert.NotEqual("n1", window.SuccessorId);
        Assert.Equal("yes", cluster.Clients[window.SuccessorId].Get("before"));
        Assert.Equal("ok", cluster.Clients[window.SuccessorId].Get("after"));
        Assert.Null(cluster.Harness.TakeLinearizableGet(cluster.Clients["n1"], "n1", "after"));
        cluster.Dispose();
    }

    [Fact]
    public void SequentialPutsAllCommit_withoutAdvancingFakeClock()
    {
        var cluster = new KvClusterTests.KvFixture(Heartbeat);
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(200));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        cluster.Harness.Advance(80);
        long start = cluster.Harness.Clock.Millis;
        for (int i = 0; i < 20; i++)
        {
            cluster.Clients["n1"].Put("k" + i, "v" + i);
            cluster.Harness.DrainAll();
            Assert.NotNull(cluster.Clients["n1"].AwaitCommitted());
        }

        Assert.Equal(start, cluster.Harness.Clock.Millis);
        Assert.Equal(21, cluster.Harness.Node("n1").CommitIndex);
        cluster.Dispose();
    }

    private static Window? TrySuccessor(KvClusterTests.KvFixture cluster, long t0)
    {
        foreach ((string id, KvClient client) in cluster.Clients)
        {
            if (id == "n1" || cluster.Harness.Node(id).Role != Role.Leader)
            {
                continue;
            }

            client.Put("after", "ok");
            cluster.Harness.DrainAll();
            long? index = client.AwaitCommitted();
            if (index is null)
            {
                continue;
            }

            int applied = 0;
            foreach (string other in cluster.Clients.Keys)
            {
                if (other == "n1")
                {
                    continue;
                }

                if (cluster.Harness.Node(other).CommitIndex >= index && cluster.Harness.Node(other).LastApplied >= index)
                {
                    applied++;
                }
            }

            if (applied >= 2)
            {
                return new Window(cluster.Harness.Clock.Millis - t0, id);
            }
        }

        return null;
    }

    private sealed record Window(long Millis, string SuccessorId);
}

public sealed class ChaosCampaignTests : IDisposable
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);
    private readonly string _root = Directory.CreateTempSubdirectory("synkrolyn-chaos-").FullName;

    [Fact]
    public void ConstructedDivergence_dropAndDelayOnHeal_committedKvWins()
    {
        var cluster = new KvClusterTests.KvFixture(Heartbeat);
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(200));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        cluster.Harness.Advance(80);
        cluster.Clients["n1"].Put("k", "committed");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        cluster.Harness.Isolate("n1");
        cluster.Clients["n1"].Put("k", "stale-tail");
        cluster.Harness.DrainAll();
        Assert.Equal(2, cluster.Harness.Node("n1").CommitIndex);
        Assert.Equal(3, cluster.Harness.Log("n1").LastIndex);
        cluster.Harness.Advance(400);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n2").Role);
        cluster.Clients["n2"].Put("k", "majority");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("committed", cluster.Clients["n1"].Get("k"));
        Assert.Null(cluster.Harness.TakeLinearizableGet(cluster.Clients["n1"], "n1", "k"));
        cluster.Harness.Heal("n1");
        cluster.Harness.Transport.DropNext("n2", "n1");
        cluster.Harness.Transport.DelayNext("n2", "n1", TimeSpan.FromMilliseconds(30));
        cluster.Harness.Advance(20);
        cluster.Harness.Advance(40);
        cluster.Harness.Advance(40);
        Assert.Equal("majority", cluster.Clients["n1"].Get("k"));
        Assert.Equal("majority", cluster.Clients["n2"].Get("k"));
        Assert.Equal(cluster.Harness.Log("n2").LastIndex, cluster.Harness.Log("n1").LastIndex);
        cluster.Dispose();
    }

    [Fact]
    public void KillMidWrite_priorPutSurvivesReopen()
    {
        var cluster = new ClusterHarness();
        var stores = new Dictionary<string, InMemoryKvStore>();
        var clients = new Dictionary<string, KvClient>();
        void Add(string id, TimeSpan election)
        {
            var store = new InMemoryKvStore();
            stores[id] = store;
            string dir = Path.Combine(_root, id);
            var node = cluster.AddNode(id, Peers(id), election, Heartbeat, store, new FilePersistentState(dir), new FileRaftLog(dir));
            clients[id] = new KvClient(node, store);
        }

        void Restart(string id, TimeSpan election)
        {
            var store = new InMemoryKvStore();
            stores[id] = store;
            string dir = Path.Combine(_root, id);
            var node = cluster.ReplaceNode(id, Peers(id), election, Heartbeat, store, new FilePersistentState(dir), new FileRaftLog(dir));
            clients[id] = new KvClient(node, store);
        }

        Add("n1", TimeSpan.FromMilliseconds(80));
        Add("n2", TimeSpan.FromMilliseconds(200));
        Add("n3", TimeSpan.FromMilliseconds(400));
        cluster.Advance(80);
        clients["n1"].Put("keep", "yes");
        cluster.DrainAll();
        cluster.Advance(20);
        cluster.Isolate("n1");
        clients["n1"].Put("lost", "no");
        cluster.DrainAll();
        Assert.Equal(2, cluster.Node("n1").CommitIndex);
        Restart("n1", TimeSpan.FromMilliseconds(2000));
        Assert.Equal(3, cluster.Log("n1").LastIndex);
        Assert.Equal(0, cluster.Node("n1").CommitIndex);
        cluster.Advance(400);
        Assert.Equal(Role.Leader, cluster.Node("n2").Role);
        clients["n2"].Put("later", "yes");
        cluster.DrainAll();
        cluster.Advance(20);
        cluster.Heal("n1");
        cluster.Heal("n2");
        cluster.Heal("n3");
        cluster.Advance(80);
        Assert.Equal("yes", clients["n1"].Get("keep"));
        Assert.Equal("yes", clients["n1"].Get("later"));
        Assert.Null(clients["n1"].Get("lost"));
        cluster.CloseDurableHandles();
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private static readonly string[] AllIds = ["n1", "n2", "n3"];

    private static string[] Peers(string id) => AllIds.Where(other => other != id).ToArray();
}
