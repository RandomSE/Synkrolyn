using Synkrolyn.Kv;
using Synkrolyn.Raft;
using Synkrolyn.Tests.Raft;

namespace Synkrolyn.Tests.Kv;

public sealed class LeaseReadTests
{
    private static readonly TimeSpan Election = TimeSpan.FromMilliseconds(150);

    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(40);

    [Fact]
    public void HotLeaseRead_returnsWhileTheRaftLockIsHeld()
    {
        using var cluster = Start();
        KvClient client = cluster.Client;
        Assert.True(cluster.Leader.ReadIndexSatisfied());
        Assert.Equal("v", client.LinearizableGet("k"));

        using var entered = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        var holder = new Thread(() => cluster.Leader.HoldEventLockForTest(entered, release))
        {
            IsBackground = true,
        };
        holder.Start();
        Assert.True(entered.Wait(TimeSpan.FromSeconds(2)));
        string? value = null;
        var reader = new Thread(() => value = client.LinearizableGet("k"))
        {
            IsBackground = true,
        };
        reader.Start();
        bool finished = reader.Join(TimeSpan.FromMilliseconds(200));
        release.Set();
        Assert.True(holder.Join(TimeSpan.FromSeconds(2)));
        Assert.True(reader.Join(TimeSpan.FromSeconds(2)));
        Assert.True(finished, "lease read blocked on the raft lock");
        Assert.Equal("v", value);
    }

    [Fact]
    public void LeaseRead_dropsWhenTheClockPassesThePublishedExpiry()
    {
        using var cluster = Start();
        Assert.True(cluster.Leader.ReadIndexSatisfied());
        cluster.Harness.Clock.Advance(Election.Milliseconds);
        Assert.False(cluster.Leader.ReadIndexSatisfied());
        Assert.Null(cluster.Client.LinearizableGet("k"));
    }

    [Fact]
    public void LeaseRead_dropsOnStepDownBeforeTheExpiry()
    {
        using var cluster = Start();
        Assert.True(cluster.Leader.ReadIndexSatisfied());
        cluster.Leader.StepDownForTest();
        Assert.False(cluster.Leader.ReadIndexSatisfied());
        Assert.Null(cluster.Client.LinearizableGet("k"));
    }

    private static Started Start()
    {
        var harness = new ClusterHarness();
        var store = new InMemoryKvStore();
        harness.AddNode("n1", ["n2", "n3"], Election, Heartbeat, store);
        harness.AddNode("n2", ["n1", "n3"], Election, Heartbeat, new InMemoryKvStore());
        harness.AddNode("n3", ["n1", "n2"], Election, Heartbeat, new InMemoryKvStore());
        harness.Advance(Election.Milliseconds);
        RaftNode leader = harness.Node("n1");
        Assert.Equal(Role.Leader, leader.Role);
        var client = new KvClient(leader, store);
        client.Put("k", "v");
        harness.DrainAll();
        harness.Advance(Heartbeat.Milliseconds);
        Assert.Equal("v", client.Get("k"));
        Assert.True(leader.ReadIndexSatisfied());
        return new Started(harness, leader, client);
    }

    private sealed class Started : IDisposable
    {
        public Started(ClusterHarness harness, RaftNode leader, KvClient client)
        {
            Harness = harness;
            Leader = leader;
            Client = client;
        }

        public ClusterHarness Harness { get; }

        public RaftNode Leader { get; }

        public KvClient Client { get; }

        public void Dispose() => Harness.CloseDurableHandles();
    }
}
