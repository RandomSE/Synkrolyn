using System.Text;
using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;
using Synkrolyn.Tests.Kv;

namespace Synkrolyn.Tests.Raft;

public sealed class FastBackupRepairTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);
    private const int Divergence = 32;

    [Fact]
    public void DivergentTail32_atMostFourFailedAppendEntries()
    {
        var cluster = new ClusterHarness();
        AddTrio(cluster);
        cluster.Log("n1").Append(new LogEntry(1, 1, Cmd("prefix")));
        cluster.Log("n3").Append(new LogEntry(1, 1, Cmd("prefix")));
        cluster.Log("n2").Append(new LogEntry(1, 1, Cmd("prefix")));
        for (int i = 0; i < Divergence; i++)
        {
            long index = 2L + i;
            cluster.Log("n1").Append(new LogEntry(index, 5, Cmd("L" + i)));
            cluster.Log("n3").Append(new LogEntry(index, 5, Cmd("L" + i)));
            cluster.Log("n2").Append(new LogEntry(index, 2, Cmd("F" + i)));
        }

        cluster.Isolate("n2");
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);

        Rejects failed = CountFailed(cluster, "n1", "n2", () =>
        {
            cluster.Heal("n2");
            cluster.Advance(80);
        });

        Assert.InRange(failed.Count, 1, 4);
        Assert.NotNull(failed.First);
        Assert.Equal(2, failed.First.XTerm);
        Assert.Equal(2, failed.First.XIndex);
        Assert.Equal(0, failed.First.XLen);
        long commit = cluster.Node("n1").CommitIndex;
        Assert.True(commit >= 1L + Divergence);
        Assert.Equal(commit, cluster.Node("n2").CommitIndex);
        Assert.Equal(CommandsThrough(cluster.Log("n1"), commit), CommandsThrough(cluster.Log("n2"), commit));
        Assert.Equal(Cmd("prefix"), cluster.Log("n2").Read(1).Command);
        Assert.Equal(Cmd("L0"), cluster.Log("n2").Read(2).Command);
    }

    [Fact]
    public void FollowerShorterBy32_atMostFourFailedAppendEntries()
    {
        var cluster = new ClusterHarness();
        AddTrio(cluster);
        cluster.Log("n1").Append(new LogEntry(1, 1, Cmd("prefix")));
        cluster.Log("n3").Append(new LogEntry(1, 1, Cmd("prefix")));
        cluster.Log("n2").Append(new LogEntry(1, 1, Cmd("prefix")));
        for (int i = 0; i < Divergence; i++)
        {
            long index = 2L + i;
            cluster.Log("n1").Append(new LogEntry(index, 5, Cmd("L" + i)));
            cluster.Log("n3").Append(new LogEntry(index, 5, Cmd("L" + i)));
        }

        cluster.Isolate("n2");
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        Assert.Equal(1, cluster.Log("n2").LastIndex);

        Rejects failed = CountFailed(cluster, "n1", "n2", () =>
        {
            cluster.Heal("n2");
            cluster.Advance(80);
        });

        Assert.InRange(failed.Count, 1, 4);
        Assert.NotNull(failed.First);
        Assert.Equal(0, failed.First.XTerm);
        Assert.Equal(0, failed.First.XIndex);
        Assert.Equal(2, failed.First.XLen);
        long commit = cluster.Node("n1").CommitIndex;
        Assert.True(commit >= 1L + Divergence);
        Assert.Equal(commit, cluster.Node("n2").CommitIndex);
        Assert.Equal(Cmd("L0"), cluster.Log("n2").Read(2).Command);
    }

    private static void AddTrio(ClusterHarness cluster)
    {
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
    }

    private static Rejects CountFailed(ClusterHarness cluster, string leaderId, string followerId, Action body)
    {
        RaftNode leader = cluster.Node(leaderId);
        var failed = new Rejects();
        cluster.Transport.Reregister(leaderId, envelope =>
        {
            if (envelope.Payload is AppendEntriesResponse response && !response.Success && envelope.From == followerId)
            {
                failed.Count++;
                failed.First ??= response;
            }

            leader.Receive(envelope);
        });
        body();
        return failed;
    }

    private static List<string> CommandsThrough(IRaftLog log, long commit)
    {
        var commands = new List<string>();
        for (long i = 1; i <= commit; i++)
        {
            commands.Add(Encoding.UTF8.GetString(log.Read(i).Command));
        }

        return commands;
    }

    private static byte[] Cmd(string text) => Encoding.UTF8.GetBytes(text);

    private sealed class Rejects
    {
        public int Count;
        public AppendEntriesResponse? First;
    }
}

public sealed class MatchIndexInflightTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void DroppedLargeSuffix_delayedHeartbeatSuccess_mustNotAdvanceMatchIndexOrCommit()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);

        cluster.Isolate("n3");
        cluster.Transport.DelayNext("n1", "n2", TimeSpan.FromMilliseconds(50));
        cluster.Advance(20);

        cluster.Transport.DropNext("n1", "n2");
        cluster.Transport.DropNext("n1", "n2");
        cluster.Transport.DropNext("n1", "n2");
        cluster.Node("n1").Propose("one"u8.ToArray());
        cluster.Node("n1").Propose("two"u8.ToArray());
        cluster.Node("n1").Propose("three"u8.ToArray());
        cluster.DrainAll();

        Assert.True(cluster.Log("n1").LastIndex >= 3);
        Assert.True(cluster.Log("n2").LastIndex < cluster.Log("n1").LastIndex);

        cluster.Transport.DropNext("n1", "n2");
        cluster.Transport.DropNext("n1", "n2");
        cluster.Advance(50);
        Assert.True(cluster.Log("n2").LastIndex < 3);
        Assert.True(cluster.Node("n1").CommitIndex < 3);
    }
}

public sealed class DurableRestartTests : IDisposable
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);
    private readonly string _root = Directory.CreateTempSubdirectory("synkrolyn-restart-").FullName;
    private ClusterHarness? _cluster;
    private readonly Dictionary<string, InMemoryKvStore> _stores = [];
    private readonly Dictionary<string, KvClient> _clients = [];

    [Fact]
    public void CommittedPut_restartFollower_recoversLogAndCatchUpGetMatches()
    {
        _cluster = new ClusterHarness();
        AddDurable("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        AddDurable("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        AddDurable("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        _cluster.Advance(80);
        Assert.Equal(Role.Leader, _cluster.Node("n1").Role);
        Assert.Null(_clients["n1"].Put("city", "athens"));
        _cluster.DrainAll();
        _cluster.Advance(20);
        Assert.Equal(2L, _clients["n1"].AwaitCommitted());
        Assert.Equal("athens", _clients["n3"].Get("city"));

        Restart("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        Assert.Equal(0, _cluster.Node("n3").CommitIndex);
        Assert.Equal(0, _cluster.Node("n3").LastApplied);
        Assert.Null(_clients["n3"].Get("city"));
        Assert.Equal(2, _cluster.Log("n3").LastIndex);
        Assert.True(_cluster.State("n3").CurrentTerm >= 1);

        _cluster.Advance(40);
        Assert.Equal(2, _cluster.Node("n3").LastApplied);
        Assert.Equal("athens", _clients["n3"].Get("city"));
    }

    [Fact]
    public void RestartAllThree_logsKeepCommitted_newTermProposeReappliesKv()
    {
        _cluster = new ClusterHarness();
        AddDurable("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        AddDurable("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        AddDurable("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        _cluster.Advance(80);
        _clients["n1"].Put("k", "v");
        _cluster.DrainAll();
        _cluster.Advance(20);
        Assert.Equal(2, _cluster.Log("n1").LastIndex);

        _cluster.CloseDurableHandles();
        _cluster = new ClusterHarness();
        AddDurable("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        AddDurable("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        AddDurable("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        Assert.Equal(2, _cluster.Log("n1").LastIndex);
        Assert.Equal(2, _cluster.Log("n2").LastIndex);
        Assert.Equal(2, _cluster.Log("n3").LastIndex);
        Assert.Equal(0, _cluster.Node("n1").CommitIndex);
        Assert.Null(_clients["n1"].Get("k"));

        _cluster.Advance(80);
        Assert.Single(_cluster.Leaders());
        string leaderId = _cluster.Leaders()[0].NodeId;
        _clients[leaderId].Put("k2", "v2");
        _cluster.DrainAll();
        _cluster.Advance(20);
        long? next = _clients[leaderId].AwaitCommitted();
        Assert.NotNull(next);
        foreach (string id in new[] { "n1", "n2", "n3" })
        {
            Assert.Equal("v", _clients[id].Get("k"));
            Assert.Equal("v2", _clients[id].Get("k2"));
            Assert.True(_cluster.Node(id).LastApplied >= next);
        }
    }

    [Fact]
    public void RecoveredUncommittedTailIsPresentAndNotAppliedUntilCommit()
    {
        _cluster = new ClusterHarness();
        AddDurable("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        AddDurable("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        AddDurable("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        _cluster.Advance(80);
        _clients["n1"].Put("k", "committed");
        _cluster.DrainAll();
        _cluster.Advance(20);
        Assert.Equal(2, _cluster.Node("n1").LastApplied);

        _cluster.Isolate("n1");
        _clients["n1"].Put("k", "uncommitted");
        _cluster.DrainAll();
        Assert.Equal(2, _cluster.Node("n1").CommitIndex);
        Assert.Equal(3, _cluster.Log("n1").LastIndex);

        Restart("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        Assert.Equal(3, _cluster.Log("n1").LastIndex);
        Assert.Equal("uncommitted", KvCommandCodec.Decode(_cluster.Log("n1").Read(3).Command).Value);
        Assert.Equal(0, _cluster.Node("n1").CommitIndex);
        Assert.Null(_clients["n1"].Get("k"));
    }

    [Fact]
    public void CrashBeforeForce_waiterNotApplied_reopenDropsEntry()
    {
        string dir = Path.Combine(_root, "solo");
        _cluster = new ClusterHarness();
        var log = new FileRaftLog(dir);
        var state = new FilePersistentState(dir);
        _cluster.AddNode("solo", [], TimeSpan.FromMilliseconds(50), Heartbeat, IStateMachine.NoOp(), state, log);
        _cluster.Advance(50);
        Assert.Equal(Role.Leader, _cluster.Node("solo").Role);
        long next = log.LastIndex + 1;
        RaftNode.ApplyWaiter waiter = _cluster.Node("solo").RegisterApplyWaiter(next);
        Assert.Equal(next, _cluster.Node("solo").Propose("durable"u8.ToArray()));
        log.CrashWithoutForce();
        using var reopened = new FileRaftLog(dir);
        Assert.True(reopened.LastIndex < next);
        Assert.False(waiter.Applied);
        Assert.False(waiter.Failed);
    }

    [Fact]
    public void ApplyExecutor_drainReturnsBeforeStateMachineRuns()
    {
        _cluster = new ClusterHarness();
        var store = new InMemoryKvStore();
        _cluster.AddNode("solo", [], TimeSpan.FromMilliseconds(50), Heartbeat, store);
        _cluster.Advance(50);
        Action? pending = null;
        _cluster.Node("solo").SetApplyExecutor(action => pending = action);
        var client = new KvClient(_cluster.Node("solo"), store);
        client.Put("k", "v");
        _cluster.DrainAll();
        Assert.Null(store.Get("k"));
        Assert.NotNull(pending);
        pending!();
        _cluster.DrainAll();
        Assert.Equal("v", store.Get("k"));
    }

    [Fact]
    public void FileWal_batchPutsOneDrain_fewerForcesThanSequentialDrains()
    {
        int batchForces = PutCount(Path.Combine(_root, "batch"), 8, drainEach: false);
        int sequentialForces = PutCount(Path.Combine(_root, "seq"), 8, drainEach: true);
        Assert.True(batchForces < sequentialForces);
        Assert.True(batchForces > 0);
    }

    public void Dispose()
    {
        _cluster?.CloseDurableHandles();
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private int PutCount(string dir, int puts, bool drainEach)
    {
        _cluster?.CloseDurableHandles();
        _cluster = new ClusterHarness();
        var store = new InMemoryKvStore();
        var log = new FileRaftLog(dir);
        var state = new FilePersistentState(dir);
        _cluster.AddNode("solo", [], TimeSpan.FromMilliseconds(50), Heartbeat, store, state, log);
        _cluster.Advance(50);
        var client = new KvClient(_cluster.Node("solo"), store);
        for (int i = 0; i < puts; i++)
        {
            client.Put("k" + i, "v");
            if (drainEach)
            {
                _cluster.DrainAll();
            }
        }

        _cluster.DrainAll();
        int forces = log.ForceCount;
        _cluster.CloseDurableHandles();
        _cluster = null;
        return forces;
    }

    private void AddDurable(string id, IEnumerable<string> peers, TimeSpan election)
    {
        var store = new InMemoryKvStore();
        _stores[id] = store;
        var node = _cluster!.AddNode(id, peers, election, Heartbeat, store, new FilePersistentState(Dir(id)), new FileRaftLog(Dir(id)));
        _clients[id] = new KvClient(node, store);
    }

    private void Restart(string id, IEnumerable<string> peers, TimeSpan election)
    {
        var store = new InMemoryKvStore();
        _stores[id] = store;
        var node = _cluster!.ReplaceNode(id, peers, election, Heartbeat, store, new FilePersistentState(Dir(id)), new FileRaftLog(Dir(id)));
        _clients[id] = new KvClient(node, store);
    }

    private string Dir(string id) => Path.Combine(_root, id);
}
