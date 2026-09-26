using Synkrolyn.Kv;
using Synkrolyn.Raft;
using Synkrolyn.Tests.Raft;

namespace Synkrolyn.Tests.Kv;

public sealed class KvClusterTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void ThreeNode_putOnLeader_getReturnsValueAndMajorityApplied()
    {
        using var cluster = NewCluster();
        ElectLeaderN1(cluster);
        long? index = cluster.Clients["n1"].Put("k", "v");
        cluster.Harness.DrainAll();
        Assert.Equal(2L, cluster.Clients["n1"].AwaitCommitted());
        Assert.Equal(2L, index ?? cluster.Clients["n1"].AwaitCommitted());
        Assert.Equal("v", cluster.Clients["n1"].Get("k"));
        Assert.Equal(2, cluster.Harness.Node("n1").CommitIndex);
        Assert.Equal(2, cluster.Harness.Node("n1").LastApplied);
        Assert.Equal(2, cluster.Harness.Node("n2").LastApplied);
        Assert.Equal(2, cluster.Harness.Node("n2").CommitIndex);
    }

    [Fact]
    public void FollowerThatReceivedAe_getMatchesLeader()
    {
        using var cluster = NewCluster();
        ElectLeaderN1(cluster);
        cluster.Clients["n1"].Put("city", "athens");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("athens", cluster.Clients["n2"].Get("city"));
        Assert.Equal("athens", cluster.Clients["n3"].Get("city"));
    }

    [Fact]
    public void PutOverwriteThenDelete_andMissingDeleteStillCommits()
    {
        using var cluster = NewCluster();
        ElectLeaderN1(cluster);
        cluster.Clients["n1"].Put("k", "one");
        cluster.Harness.DrainAll();
        cluster.Clients["n1"].Put("k", "two");
        cluster.Harness.DrainAll();
        Assert.Equal("two", cluster.Clients["n1"].Get("k"));
        Assert.Equal("two", cluster.Clients["n2"].Get("k"));

        cluster.Clients["n1"].Delete("k");
        cluster.Harness.DrainAll();
        Assert.Null(cluster.Clients["n1"].Get("k"));
        Assert.Null(cluster.Clients["n2"].Get("k"));

        Assert.Null(cluster.Clients["n1"].Delete("missing"));
        cluster.Harness.DrainAll();
        Assert.NotNull(cluster.Clients["n1"].AwaitCommitted());
        Assert.Null(cluster.Clients["n1"].Get("missing"));
    }

    [Fact]
    public void SingleNode_putAppliesImmediately()
    {
        using var cluster = NewCluster();
        cluster.Add("solo", [], TimeSpan.FromMilliseconds(50));
        cluster.Harness.Advance(50);
        long? index = cluster.Clients["solo"].Put("a", "b");
        cluster.Harness.DrainAll();
        Assert.Equal(2L, cluster.Clients["solo"].AwaitCommitted());
        Assert.Equal(2L, index ?? cluster.Clients["solo"].AwaitCommitted());
        Assert.Equal("b", cluster.Clients["solo"].Get("a"));
        Assert.Equal(2, cluster.Harness.Node("solo").LastApplied);
    }

    [Fact]
    public void IsolatedFollowerGetStaysStale_thenCatchesUpAfterHeal()
    {
        using var cluster = NewCluster();
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        cluster.Harness.Advance(80);
        cluster.Clients["n1"].Put("k", "old");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("old", cluster.Clients["n3"].Get("k"));

        cluster.Harness.Isolate("n3");
        cluster.Clients["n1"].Put("k", "new");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("new", cluster.Clients["n1"].Get("k"));
        Assert.Equal("old", cluster.Clients["n3"].Get("k"));

        cluster.Harness.Heal("n3");
        cluster.Harness.Advance(40);
        Assert.Equal("new", cluster.Clients["n3"].Get("k"));
    }

    [Fact]
    public void NonLeaderPutDelete_doesNotProposeOrChangeStores()
    {
        using var cluster = NewCluster();
        ElectLeaderN1(cluster);
        cluster.Clients["n1"].Put("k", "v");
        cluster.Harness.DrainAll();
        long logLen = cluster.Harness.Log("n2").LastIndex;

        Assert.Null(cluster.Clients["n2"].Put("k", "nope"));
        Assert.Null(cluster.Clients["n2"].Delete("k"));
        cluster.Harness.DrainAll();
        Assert.Equal(logLen, cluster.Harness.Log("n2").LastIndex);
        Assert.Equal("v", cluster.Clients["n1"].Get("k"));
        Assert.Equal("v", cluster.Clients["n2"].Get("k"));
    }

    [Fact]
    public void StaleLeaderUncommittedPut_doesNotApply_winnerAppliesAfterHeal()
    {
        using var cluster = NewCluster();
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(200));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        cluster.Harness.Advance(80);
        cluster.Harness.Isolate("n1");
        Assert.Null(cluster.Clients["n1"].Put("k", "stale"));
        cluster.Harness.DrainAll();
        Assert.Null(cluster.Clients["n1"].AwaitCommitted());
        Assert.Equal(1, cluster.Harness.Node("n1").LastApplied);
        Assert.Null(cluster.Clients["n1"].Get("k"));
        Assert.Null(cluster.Clients["n2"].Get("k"));

        cluster.Harness.Advance(400);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n2").Role);
        cluster.Clients["n2"].Put("k", "fresh");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("fresh", cluster.Clients["n2"].Get("k"));
        Assert.True(cluster.Harness.Node("n2").LastApplied >= 2);

        cluster.Harness.Heal("n1");
        cluster.Harness.Advance(40);
        Assert.Equal("fresh", cluster.Clients["n1"].Get("k"));
        Assert.True(cluster.Harness.Node("n1").LastApplied >= 2);
    }

    [Fact]
    public void StateMachineSafety_sameIndexSameKvEffect()
    {
        using var cluster = NewCluster();
        ElectLeaderN1(cluster);
        cluster.Clients["n1"].Put("a", "1");
        cluster.Harness.DrainAll();
        cluster.Clients["n1"].Put("a", "2");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("2", cluster.Clients["n1"].Get("a"));
        Assert.Equal("2", cluster.Clients["n2"].Get("a"));
        Assert.Equal("2", cluster.Clients["n3"].Get("a"));
        Assert.Equal(cluster.Harness.Node("n1").LastApplied, cluster.Harness.Node("n2").LastApplied);
    }

    [Fact]
    public void DuplicatePutSameClientSerial_appliedOnce()
    {
        using var cluster = NewCluster();
        ElectLeaderN1(cluster);
        var client = new KvClient(cluster.Harness.Node("n1"), cluster.Stores["n1"], "c1");
        client.Put("k", "once");
        cluster.Harness.DrainAll();
        Assert.Equal("once", cluster.Stores["n1"].Get("k"));
        byte[] replay = KvCommandCodec.EncodePut("k", "twice", "c1", 1);
        cluster.Harness.Propose("n1", replay);
        Assert.Equal("once", cluster.Stores["n1"].Get("k"));
        Assert.Equal("once", cluster.Stores["n2"].Get("k"));
    }

    [Fact]
    public void KeyRoutedPut_goesToMatchingRangeStore()
    {
        using var left = NewCluster();
        using var right = NewCluster();
        left.Add("a", [], TimeSpan.FromMilliseconds(40));
        right.Add("b", [], TimeSpan.FromMilliseconds(40));
        left.Harness.Advance(40);
        right.Harness.Advance(40);
        var router = new RangeKvClient(
        [
            new RangeBinding("a", "m", left.Clients["a"]),
            new RangeBinding("m", "", right.Clients["b"]),
        ]);
        router.Put("apple", "1");
        left.Harness.DrainAll();
        router.Put("mango", "2");
        right.Harness.DrainAll();
        Assert.Equal("1", left.Clients["a"].Get("apple"));
        Assert.Null(left.Clients["a"].Get("mango"));
        Assert.Equal("2", right.Clients["b"].Get("mango"));
    }

    private static void ElectLeaderN1(KvFixture cluster)
    {
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        cluster.Harness.Advance(80);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
    }

    private static KvFixture NewCluster() => new(Heartbeat);

    internal sealed class KvFixture : IDisposable
    {
        public KvFixture(TimeSpan heartbeat) => Heartbeat = heartbeat;

        public TimeSpan Heartbeat { get; }

        public ClusterHarness Harness { get; } = new();

        public Dictionary<string, InMemoryKvStore> Stores { get; } = [];

        public Dictionary<string, KvClient> Clients { get; } = [];

        public void Add(string id, IEnumerable<string> peers, TimeSpan election)
        {
            var store = new InMemoryKvStore();
            Stores[id] = store;
            Harness.AddNode(id, peers, election, Heartbeat, store);
            Clients[id] = new KvClient(Harness.Node(id), store);
        }

        public void Dispose() => Harness.CloseDurableHandles();
    }
}

public sealed class ReadIndexTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void IsolatedStaleLeader_linearizableGetRejected_afterNewLeaderCommitsDifferentPut()
    {
        using var cluster = new KvClusterTests.KvFixture(Heartbeat);
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(200));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        cluster.Harness.Advance(80);
        cluster.Clients["n1"].Put("k", "old");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("old", cluster.Clients["n1"].Get("k"));

        cluster.Harness.Isolate("n1");
        cluster.Harness.Advance(400);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n2").Role);
        cluster.Clients["n2"].Put("k", "fresh");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("fresh", cluster.Clients["n2"].Get("k"));

        Assert.NotEqual(Role.Leader, cluster.Harness.Node("n1").Role);
        Assert.Equal("old", cluster.Clients["n1"].Get("k"));
        Assert.Null(cluster.Harness.TakeLinearizableGet(cluster.Clients["n1"], "n1", "k"));
    }

    [Fact]
    public void IsolatedLeader_withoutNewElection_cannotCompleteReadIndex()
    {
        using var cluster = new KvClusterTests.KvFixture(Heartbeat);
        Elect(cluster);
        cluster.Clients["n1"].Put("k", "v");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("v", cluster.Harness.TakeLinearizableGet(cluster.Clients["n1"], "n1", "k"));

        cluster.Harness.Isolate("n1");
        Assert.Equal("v", cluster.Clients["n1"].Get("k"));
        Assert.True(cluster.Harness.Node("n1").QuorumLeaseValid);
        Assert.Equal("v", cluster.Clients["n1"].LinearizableGet("k"));
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
        cluster.Harness.Advance(80);
        Assert.NotEqual(Role.Leader, cluster.Harness.Node("n1").Role);
        Assert.False(cluster.Harness.Node("n1").QuorumLeaseValid);
        Assert.Null(cluster.Clients["n1"].LinearizableGet("k"));
    }

    [Fact]
    public void ThreeNode_putThenLinearizableGetOnLeader()
    {
        using var cluster = new KvClusterTests.KvFixture(Heartbeat);
        Elect(cluster);
        cluster.Clients["n1"].Put("city", "athens");
        cluster.Harness.DrainAll();
        Assert.Equal(2, cluster.Harness.Node("n1").CommitIndex);
        Assert.Equal(2, cluster.Harness.Node("n1").LastApplied);
        Assert.Equal("athens", cluster.Harness.TakeLinearizableGet(cluster.Clients["n1"], "n1", "city"));
    }

    [Fact]
    public void SingleNode_putThenLinearizableGetWithoutPeerRpcs()
    {
        using var cluster = new KvClusterTests.KvFixture(Heartbeat);
        cluster.Add("solo", [], TimeSpan.FromMilliseconds(50));
        cluster.Harness.Advance(50);
        cluster.Clients["solo"].Put("a", "b");
        cluster.Harness.DrainAll();
        Assert.Equal(2, cluster.Harness.Node("solo").LastApplied);
        Assert.Equal("b", cluster.Clients["solo"].LinearizableGet("a"));
    }

    [Fact]
    public void FollowerLinearizableGet_emptyWhileBoundedStaleGetHits()
    {
        using var cluster = new KvClusterTests.KvFixture(Heartbeat);
        Elect(cluster);
        cluster.Clients["n1"].Put("k", "v");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("v", cluster.Clients["n2"].Get("k"));
        Assert.Equal(Role.Follower, cluster.Harness.Node("n2").Role);
        Assert.True(cluster.Harness.Node("n2").FollowerReadLeaseValid);
        Assert.Null(cluster.Clients["n2"].LinearizableGet("k"));
        Assert.Equal("v", cluster.Clients["n2"].BoundedStaleGet("k"));
    }

    [Fact]
    public void FollowerBoundedStaleGet_emptyAfterLeaseExpires()
    {
        using var cluster = new KvClusterTests.KvFixture(Heartbeat);
        Elect(cluster);
        cluster.Clients["n1"].Put("k", "v");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("v", cluster.Clients["n2"].BoundedStaleGet("k"));
        cluster.Harness.Isolate("n2");
        cluster.Harness.Advance(RaftNode.PreVoteLeaderLeaseFactor * 20);
        Assert.Equal(Role.Follower, cluster.Harness.Node("n2").Role);
        Assert.False(cluster.Harness.Node("n2").FollowerReadLeaseValid);
        Assert.Null(cluster.Clients["n2"].BoundedStaleGet("k"));
        Assert.Null(cluster.Clients["n2"].LinearizableGet("k"));
        Assert.Equal("v", cluster.Clients["n2"].Get("k"));
    }

    [Fact]
    public void ReadIndexEmptyUntilCurrentTermEntryCommitted()
    {
        using var cluster = new KvClusterTests.KvFixture(Heartbeat);
        Elect(cluster);
        Assert.Equal(1, cluster.Harness.Node("n1").CommitIndex);
        Assert.Equal(1, cluster.Harness.Node("n1").BeginReadIndex());
        Assert.Null(cluster.Harness.TakeLinearizableGet(cluster.Clients["n1"], "n1", "k"));
        cluster.Clients["n1"].Put("k", "v");
        cluster.Harness.DrainAll();
        Assert.Equal("v", cluster.Harness.TakeLinearizableGet(cluster.Clients["n1"], "n1", "k"));
    }

    [Fact]
    public void FollowerPut_redirectsViaLeaderHint()
    {
        using var cluster = new KvClusterTests.KvFixture(Heartbeat);
        Elect(cluster);
        cluster.Harness.Advance(20);
        Assert.Null(cluster.Clients["n2"].Put("city", "athens"));
        Assert.Equal("n1", cluster.Clients["n2"].LeaderHint());
        var redirect = new RedirectingKvClient(cluster.Clients);
        long? proposed = redirect.Put("n2", "city", "athens");
        cluster.Harness.DrainAll();
        Assert.True(cluster.Clients["n1"].AwaitCommitted() is not null || proposed is not null);
        Assert.Equal("athens", cluster.Clients["n1"].Get("city"));
        Assert.Equal("athens", cluster.Clients["n2"].Get("city"));
    }

    private static void Elect(KvClusterTests.KvFixture cluster)
    {
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        cluster.Harness.Advance(80);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
    }
}

public sealed class SourceConventionsTests
{
    [Fact]
    public void RaftSourcesMustNotReferenceKvNamespace()
    {
        string raftDir = Path.Combine(FindRepoRoot(), "src", "Synkrolyn", "Raft");
        Assert.True(Directory.Exists(raftDir));
        var hits = new List<string>();
        foreach (string file in Directory.EnumerateFiles(raftDir, "*.cs"))
        {
            string text = File.ReadAllText(file);
            if (text.Contains("Synkrolyn.Kv", StringComparison.Ordinal))
            {
                hits.Add(file);
            }
        }

        Assert.Empty(hits);
    }

    [Fact]
    public void UnitTestsMustNotCallThreadSleep()
    {
        string testDir = Path.Combine(FindRepoRoot(), "tests");
        string banned = "Thread." + "Sleep";
        var hits = new List<string>();
        foreach (string file in Directory.EnumerateFiles(testDir, "*.cs", SearchOption.AllDirectories))
        {
            if (File.ReadAllText(file).Contains(banned, StringComparison.Ordinal))
            {
                hits.Add(file);
            }
        }

        Assert.Empty(hits);
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "Synkrolyn.slnx")))
            {
                return dir.FullName;
            }

            dir = dir.Parent;
        }

        return Directory.GetCurrentDirectory();
    }
}
