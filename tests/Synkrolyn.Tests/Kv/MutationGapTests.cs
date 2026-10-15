using Synkrolyn.Kv;
using Synkrolyn.Tests.Raft;

namespace Synkrolyn.Tests.Kv;

/// <summary>
/// Session dedup, ack floor, and client-serial edges that a Basic mutation run left alive.
/// </summary>
public sealed class MutationGapTests
{
    [Fact]
    public void ClientSerial_doesNotDedupZeroSerial_orAnEmptyClient()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodePut("k", "one", "c", 0));
        store.Apply(2, KvCommandCodec.EncodePut("k", "two", "c", 0));
        Assert.Equal("two", store.Get("k"));

        store.Apply(3, KvCommandCodec.EncodePut("k", "three", "", 1));
        store.Apply(4, KvCommandCodec.EncodePut("k", "four", "", 1));
        Assert.Equal("four", store.Get("k"));

        store.Apply(5, KvCommandCodec.EncodePut("k", "five", "c2", 1));
        store.Apply(6, KvCommandCodec.EncodePut("k", "six", "c2", 1));
        Assert.Equal("five", store.Get("k"));
    }

    [Fact]
    public void EmptyCommand_consumesItsIndex()
    {
        var store = new InMemoryKvStore();
        store.Apply(5, []);
        store.Apply(4, KvCommandCodec.EncodePut("k", "stale"));
        Assert.Null(store.Get("k"));
        store.Apply(6, KvCommandCodec.EncodePut("k", "live"));
        Assert.Equal("live", store.Get("k"));
    }

    [Fact]
    public void AckFloor_discardsTheEqualSerial_andKeepsTheCacheBelowLastSeq()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodeRegister(1_000, 5_000));
        store.Apply(2, KvCommandCodec.EncodeSessionPut("k", "once", 1, 1, 0, 1_000));
        store.Apply(3, KvCommandCodec.EncodeSessionPut("k", "two", 1, 2, 0, 1_100));
        store.Apply(4, KvCommandCodec.EncodeSessionPut("k", "stale", 1, 2, 1, 1_200));
        Assert.Equal(KvCommandError.None, store.CommandResult.Error);
        Assert.Equal("two", store.CommandResult.CachedResponse);
        Assert.Equal("two", store.Get("k"));
        Assert.Equal(1, store.ReadSession(1).AckFloor);
        Assert.Equal(2, store.ReadSession(1).LastSeq);

        store.Apply(5, KvCommandCodec.EncodeSessionPut("k", "nope", 1, 1, 1, 1_300));
        Assert.Equal(KvCommandError.None, store.CommandResult.Error);
        Assert.Null(store.CommandResult.CachedResponse);
        Assert.Equal("two", store.Get("k"));
        Assert.Equal(2, store.ReadSession(1).LastSeq);
    }

    [Fact]
    public void Session_livesAtTheExactTimeout_andZeroTimeoutDoesNotExpire()
    {
        var bounded = new InMemoryKvStore();
        bounded.Apply(1, KvCommandCodec.EncodeRegister(1_000, 1_000));
        bounded.Apply(2, KvCommandCodec.EncodeSessionPut("k", "once", 1, 1, 0, 1_000));
        bounded.Apply(3, KvCommandCodec.EncodeTick(2_000));
        Assert.Equal(1, bounded.SessionCount);
        bounded.Apply(4, KvCommandCodec.EncodeSessionPut("k", "dup", 1, 1, 0, 1_000));
        Assert.Equal(KvCommandError.None, bounded.CommandResult.Error);
        Assert.Equal("once", bounded.CommandResult.CachedResponse);
        Assert.Equal("once", bounded.Get("k"));
        Assert.Equal(1_000, bounded.ReadSession(1).LastActiveMillis);
        bounded.Apply(5, KvCommandCodec.EncodeTick(2_001));
        Assert.Equal(0, bounded.SessionCount);

        var forever = new InMemoryKvStore();
        forever.Apply(1, KvCommandCodec.EncodeRegister(1_000, 0));
        forever.Apply(2, KvCommandCodec.EncodeSessionPut("k", "once", 1, 1, 0, 1_000));
        forever.Apply(3, KvCommandCodec.EncodeTick(50_000));
        Assert.Equal(1, forever.SessionCount);
        forever.Apply(4, KvCommandCodec.EncodeSessionPut("k", "dup", 1, 1, 0, 50_000));
        Assert.Equal(KvCommandError.None, forever.CommandResult.Error);
        Assert.Equal("once", forever.CommandResult.CachedResponse);
        Assert.False(forever.ReadSession(1).Expired);
    }

    [Fact]
    public void OutOfOrder_resumesAtTheAckedSeq_andSessionExpired_registersAgain()
    {
        using var cluster = new KvClusterTests.KvFixture(TimeSpan.FromMilliseconds(20));
        cluster.Add("solo", [], TimeSpan.FromMilliseconds(50));
        cluster.Harness.Advance(50);
        var client = new KvClient(cluster.Harness.Node("solo"), cluster.Stores["solo"]);
        long id = 0;
        for (int i = 0; i < 6 && id == 0; i++)
        {
            client.Register(TimeSpan.FromMinutes(5));
            cluster.Harness.DrainAll();
            id = client.ClientId;
        }

        Assert.True(id > 0);
        client.Put("k", "once");
        cluster.Harness.DrainAll();
        Assert.NotNull(client.AwaitCommitted());
        Assert.Equal("once", cluster.Stores["solo"].Get("k"));

        client.PrepareSeqForTest(4);
        Assert.Null(client.Put("k", "skip"));
        cluster.Harness.DrainAll();
        Assert.Null(client.AwaitCommitted());
        Assert.Equal(KvCommandError.OutOfOrder, client.LastError);
        client.Put("k", "two");
        cluster.Harness.DrainAll();
        Assert.NotNull(client.AwaitCommitted());
        Assert.Equal("two", cluster.Stores["solo"].Get("k"));
        Assert.Equal(2, cluster.Stores["solo"].ReadSession(id).LastSeq);

        client.AttachSessionForTest(999, TimeSpan.FromMinutes(5));
        Assert.Null(client.Put("k", "missing"));
        cluster.Harness.DrainAll();
        Assert.Null(client.AwaitCommitted());
        Assert.Equal(KvCommandError.SessionExpired, client.LastError);
        cluster.Harness.DrainAll();
        long recovered = client.ClientId;
        Assert.NotEqual(999, recovered);
        Assert.True(recovered > 0);
        client.Put("k", "fresh");
        cluster.Harness.DrainAll();
        Assert.NotNull(client.AwaitCommitted());
        Assert.Equal("fresh", cluster.Stores["solo"].Get("k"));
        Assert.Equal(1, cluster.Stores["solo"].ReadSession(recovered).LastSeq);
    }
}
