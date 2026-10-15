using Synkrolyn.Kv;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Kv;

/// <summary>Client sessions from Ongaro thesis section 6.3.</summary>
public sealed class KvSessionTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public async Task DuplicatePut_afterLeaderChange_appliesOnce()
    {
        using var cluster = NewCluster();
        ElectFastLeader(cluster);
        var client = new KvClient(cluster.Harness.Node("n1"), cluster.Stores["n1"]);
        long id = OpenSession(client, cluster, TimeSpan.FromMinutes(5));
        Assert.Null(client.Put("k", "once"));
        cluster.Harness.DrainAll();
        Assert.Equal("once", cluster.Stores["n1"].Get("k"));
        Assert.Equal("once", cluster.Stores["n2"].Get("k"));
        Assert.Equal(1, cluster.Stores["n2"].ReadSession(id).LastSeq);

        cluster.Harness.Isolate("n1");
        cluster.Harness.Advance(400);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n2").Role);
        client.Follow(cluster.Harness.Node("n2"), cluster.Stores["n2"]);
        Task<long?> retry = client.PutAsync("k", "once");
        Assert.False(retry.IsCompleted);
        cluster.Harness.DrainAll();
        Assert.True(retry.IsCompletedSuccessfully);
        Assert.NotNull(await retry);
        Assert.Equal("once", cluster.Stores["n2"].Get("k"));
        Assert.Equal("once", cluster.Stores["n3"].Get("k"));
        Assert.Equal(1, cluster.Stores["n2"].ReadSession(id).LastSeq);
        Assert.Equal(1, cluster.Stores["n3"].ReadSession(id).LastSeq);

        long stamp = cluster.Stores["n2"].ReadSession(id).LastActiveMillis;
        cluster.Harness.Propose("n2", KvCommandCodec.EncodeSessionPut("k", "twice", id, 1, 0, stamp));
        Assert.Equal("once", cluster.Stores["n2"].Get("k"));
        Assert.Equal("once", cluster.Stores["n3"].Get("k"));
        Assert.Equal(1, cluster.Stores["n2"].ReadSession(id).LastSeq);

        cluster.Harness.Heal("n1");
        cluster.Harness.Advance(40);
        Assert.Equal("once", cluster.Stores["n1"].Get("k"));
        Assert.Equal(1, cluster.Stores["n1"].ReadSession(id).LastSeq);
    }

    [Fact]
    public void SessionExpiry_matchesOnEveryReplica()
    {
        using var cluster = NewCluster();
        ElectFastLeader(cluster);
        var client = new KvClient(cluster.Harness.Node("n1"), cluster.Stores["n1"]);
        long id = OpenSession(client, cluster, TimeSpan.FromMilliseconds(1000));
        client.Put("k", "v");
        cluster.Harness.DrainAll();
        Assert.NotNull(client.AwaitCommitted());
        foreach (InMemoryKvStore store in cluster.Stores.Values)
        {
            Assert.False(store.ReadSession(id).Expired);
        }

        cluster.Harness.Advance(5_000);
        foreach (InMemoryKvStore store in cluster.Stores.Values)
        {
            Assert.False(store.ReadSession(id).Expired);
        }

        Assert.NotNull(client.RecordTime());
        cluster.Harness.DrainAll();
        long logTime = cluster.Stores["n1"].AppliedTimestamp();
        foreach (InMemoryKvStore store in cluster.Stores.Values)
        {
            Assert.Equal(0, store.SessionCount);
            Assert.Equal(logTime, store.AppliedTimestamp());
            Assert.Throws<KvSessionException>(() => store.ReadSession(id));
        }
    }

    [Fact]
    public void SessionState_survivesSnapshotAndRestore()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodeRegister(1_000, 5_000));
        store.Apply(2, KvCommandCodec.EncodeSessionPut("k", "once", 1, 1, 0, 1_000));
        byte[] snap = store.Snapshot();
        var restored = new InMemoryKvStore();
        restored.Restore(snap);
        Assert.Equal("once", restored.Get("k"));
        InMemoryKvStore.SessionView session = restored.ReadSession(1);
        Assert.Equal(1, session.LastSeq);
        Assert.Equal(0, session.AckFloor);
        Assert.Equal(1_000, session.LastActiveMillis);
        Assert.Equal(5_000, session.TimeoutMillis);
        Assert.Equal(1_000, session.LogTimeMillis);
        Assert.False(session.Expired);

        restored.Apply(3, KvCommandCodec.EncodeSessionPut("k", "twice", 1, 1, 0, 1_100));
        Assert.Equal("once", restored.Get("k"));
        Assert.Equal(1, restored.ReadSession(1).LastSeq);

        restored.Apply(4, KvCommandCodec.EncodeSessionPut("k", "two", 1, 2, 1, 1_200));
        Assert.Equal("two", restored.Get("k"));
        Assert.Equal(1, restored.ReadSession(1).AckFloor);
        restored.Apply(5, KvCommandCodec.EncodeSessionPut("k", "stale", 1, 1, 1, 1_300));
        Assert.Equal("two", restored.Get("k"));
        Assert.Equal(2, restored.ReadSession(1).LastSeq);
    }

    [Fact]
    public void SessionExpiry_skewedLeaderClock_followsElapsedNotProcessTime()
    {
        // Successor process clock is far below the applied timestamp.
        AssertSkewedLeader(leaderShift: 200_000, successorShift: 0);
        // Successor process clock is far above the applied timestamp.
        AssertSkewedLeader(leaderShift: 0, successorShift: 80_000_000);
    }

    [Fact]
    public void ExpiredUnknownAndOutOfOrder_areNotReportedAsApplied()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodeRegister(1_000, 5_000));
        store.Apply(2, KvCommandCodec.EncodeSessionPut("k", "once", 1, 1, 0, 1_000));
        store.Apply(3, KvCommandCodec.EncodeSessionPut("k", "nope", 1, 4, 0, 1_100));
        Assert.Equal(KvCommandError.OutOfOrder, store.CommandResult.Error);
        Assert.Equal("once", store.Get("k"));

        store.Apply(4, KvCommandCodec.EncodeSessionPut("k", "ghost", 99, 1, 0, 1_100));
        Assert.Equal(KvCommandError.SessionExpired, store.CommandResult.Error);
        Assert.Equal("once", store.Get("k"));

        store.Apply(5, KvCommandCodec.EncodeTick(7_000));
        store.Apply(6, KvCommandCodec.EncodeSessionPut("k", "late", 1, 2, 0, 7_000));
        Assert.Equal(KvCommandError.SessionExpired, store.CommandResult.Error);
        Assert.Equal("once", store.Get("k"));

        var duplicate = new InMemoryKvStore();
        duplicate.Apply(1, KvCommandCodec.EncodeRegister(1_000, 5_000));
        duplicate.Apply(2, KvCommandCodec.EncodeSessionPut("k", "once", 1, 1, 0, 1_000));
        duplicate.Apply(3, KvCommandCodec.EncodeSessionPut("k", "twice", 1, 1, 0, 1_050));
        Assert.Equal(KvCommandError.None, duplicate.CommandResult.Error);
        Assert.Equal("once", duplicate.CommandResult.CachedResponse);
        Assert.Equal("once", duplicate.Get("k"));

        using var cluster = NewCluster();
        cluster.Add("solo", [], TimeSpan.FromMilliseconds(50));
        cluster.Harness.Advance(50);
        var client = new KvClient(cluster.Harness.Node("solo"), cluster.Stores["solo"]);
        long id = OpenSession(client, cluster, TimeSpan.FromMilliseconds(5_000));
        client.Put("k", "once");
        cluster.Harness.DrainAll();
        Assert.NotNull(client.AwaitCommitted());
        client.PrepareSeqForTest(4);
        Assert.Null(client.Put("k", "skip"));
        cluster.Harness.DrainAll();
        Assert.Null(client.AwaitCommitted());
        Assert.Equal(KvCommandError.OutOfOrder, client.LastError);
        Assert.Equal("once", cluster.Stores["solo"].Get("k"));

        client.AttachSessionForTest(999, TimeSpan.FromMilliseconds(5_000));
        Assert.Null(client.Put("k", "missing"));
        cluster.Harness.DrainAll();
        Assert.Null(client.AwaitCommitted());
        Assert.Equal(KvCommandError.SessionExpired, client.LastError);
        Assert.Equal("once", cluster.Stores["solo"].Get("k"));
        cluster.Harness.DrainAll();
        Assert.NotEqual(0, client.ClientId);
        Assert.NotEqual(id, client.ClientId);
    }

    [Fact]
    public void ExpiredSession_isRemovedAndAbsentFromSnapshot()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodeRegister(1_000, 100));
        store.Apply(2, KvCommandCodec.EncodeSessionPut("k", "v", 1, 1, 0, 1_000));
        Assert.Equal(1, store.SessionCount);
        store.Apply(3, KvCommandCodec.EncodeTick(1_200));
        Assert.Equal(0, store.SessionCount);
        Assert.Throws<KvSessionException>(() => store.ReadSession(1));
        store.Apply(4, KvCommandCodec.EncodeTick(10));
        Assert.Equal(1_200, store.AppliedTimestamp());
        byte[] snap = store.Snapshot();
        var restored = new InMemoryKvStore();
        restored.Restore(snap);
        Assert.Equal(0, restored.SessionCount);
        Assert.Equal(1_200, restored.AppliedTimestamp());
        Assert.Throws<KvSessionException>(() => restored.ReadSession(1));
    }

    [Fact]
    public void Get_onExpiredSession_throws()
    {
        using var cluster = NewCluster();
        cluster.Add("solo", [], TimeSpan.FromMilliseconds(50));
        cluster.Harness.Advance(50);
        var client = new KvClient(cluster.Harness.Node("solo"), cluster.Stores["solo"]);
        OpenSession(client, cluster, TimeSpan.FromMilliseconds(100));
        client.Put("k", "v");
        cluster.Harness.DrainAll();
        Assert.Equal("v", client.Get("k"));
        cluster.Harness.Advance(500);
        Assert.Equal("v", client.Get("k"));
        Assert.NotNull(client.RecordTime());
        cluster.Harness.DrainAll();
        KvSessionException expired = Assert.Throws<KvSessionException>(() => client.Get("k"));
        Assert.Contains("expired", expired.Message, StringComparison.Ordinal);
        Task<string?> read = client.GetAsync("k");
        Assert.True(read.IsFaulted);
        Assert.Contains("expired", read.Exception!.InnerException!.Message, StringComparison.Ordinal);
    }

    private static void AssertSkewedLeader(long leaderShift, long successorShift)
    {
        using var cluster = new KvClusterTests.KvFixture(Heartbeat);
        var leaderClock = new ShiftedClock(cluster.Harness.Clock, leaderShift);
        var successorClock = new ShiftedClock(cluster.Harness.Clock, successorShift);
        var slowClock = new ShiftedClock(cluster.Harness.Clock, 0);
        var leaderStore = new InMemoryKvStore();
        var successorStore = new InMemoryKvStore();
        var slowStore = new InMemoryKvStore();
        cluster.Harness.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat, leaderStore, clock: leaderClock);
        cluster.Harness.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(200), Heartbeat, successorStore, clock: successorClock);
        cluster.Harness.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat, slowStore, clock: slowClock);
        cluster.Stores["n1"] = leaderStore;
        cluster.Stores["n2"] = successorStore;
        cluster.Stores["n3"] = slowStore;
        cluster.Harness.Advance(80);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
        var client = new KvClient(cluster.Harness.Node("n1"), leaderStore);
        long id = OpenSession(client, cluster, TimeSpan.FromMilliseconds(5_000));
        client.Put("k", "v");
        cluster.Harness.DrainAll();
        Assert.NotNull(client.AwaitCommitted());
        long applied = successorStore.AppliedTimestamp();
        cluster.Harness.Isolate("n1");
        cluster.Harness.Advance(400);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n2").Role);
        client.Follow(cluster.Harness.Node("n2"), successorStore);
        Assert.NotNull(client.RecordTime());
        cluster.Harness.DrainAll();
        Assert.Equal(1, successorStore.SessionCount);
        Assert.False(successorStore.ReadSession(id).Expired);
        Assert.True(successorStore.AppliedTimestamp() >= applied);

        cluster.Harness.Advance(6_000);
        Assert.NotNull(client.RecordTime());
        cluster.Harness.DrainAll();
        Assert.Equal(0, successorStore.SessionCount);
        Assert.Equal(0, slowStore.SessionCount);
        Assert.True(successorStore.AppliedTimestamp() > applied);
        long clock = successorStore.AppliedTimestamp();
        cluster.Harness.Advance(1_000);
        Assert.NotNull(client.RecordTime());
        cluster.Harness.DrainAll();
        Assert.True(successorStore.AppliedTimestamp() >= clock);
    }

    private static long OpenSession(KvClient client, KvClusterTests.KvFixture cluster, TimeSpan timeout)
    {
        for (int i = 0; i < 6 && client.ClientId == 0; i++)
        {
            client.Register(timeout);
            cluster.Harness.DrainAll();
        }

        Assert.True(client.ClientId > 0);
        return client.ClientId;
    }

    private static void ElectFastLeader(KvClusterTests.KvFixture cluster)
    {
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(200));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        cluster.Harness.Advance(80);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
    }

    private static KvClusterTests.KvFixture NewCluster() => new(Heartbeat);

    private sealed class ShiftedClock : IRaftClock
    {
        private readonly IRaftClock _inner;
        private readonly long _shift;

        public ShiftedClock(IRaftClock inner, long shift)
        {
            _inner = inner;
            _shift = shift;
        }

        public long Millis => _inner.Millis + _shift;

        public void OnAdvance(Action listener) => _inner.OnAdvance(listener);
    }
}
