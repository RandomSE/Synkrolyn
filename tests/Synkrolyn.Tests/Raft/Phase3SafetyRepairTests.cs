using System.Reflection;
using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;
using Synkrolyn.Tests.Kv;

namespace Synkrolyn.Tests.Raft;

/// <summary>
/// Review of 0778c22. Each test fails on that commit and locks in the safe behaviour.
/// </summary>
public class Phase3SafetyRepairTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void ConcurrentAppendAndCompact_onBothLogs_staysContiguous()
    {
        var memoryError = StressLog(new InMemoryRaftLog());
        Assert.Null(memoryError);
        string dir = Directory.CreateTempSubdirectory("synkrolyn-compact-race-").FullName;
        try
        {
            var fileError = StressLog(new FileRaftLog(dir));
            Assert.Null(fileError);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Apply_advancesWhileSnapshotSerializationIsBlocked()
    {
        var cluster = new ClusterHarness();
        var machine = new SlowEncodeMachine();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat, machine);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.NotNull(cluster.Propose("n1", "a"u8.ToArray()));
        cluster.DrainAll();
        RaftNode leader = cluster.Node("n1");
        long applied = leader.LastApplied;
        leader.SetSnapshotExecutor(action => ThreadPool.QueueUserWorkItem(_ => action()));
        machine.Arm();
        leader.Snapshot();
        Assert.True(machine.InEncode.Wait(TimeSpan.FromSeconds(2)));
        Assert.NotNull(leader.Propose("b"u8.ToArray()));
        cluster.DrainAll();
        try
        {
            Assert.True(leader.LastApplied > applied);
        }
        finally
        {
            machine.Release.Set();
        }
    }

    [Fact]
    public void CompactionCommit_runsOnTheRaftThread()
    {
        var cluster = new ClusterHarness();
        var log = new ThreadWatchLog();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat, log: log);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.NotNull(cluster.Propose("n1", "a"u8.ToArray()));
        cluster.DrainAll();
        RaftNode leader = cluster.Node("n1");
        using var executorDone = new ManualResetEventSlim(false);
        leader.SetSnapshotExecutor(action => ThreadPool.QueueUserWorkItem(_ =>
        {
            action();
            executorDone.Set();
        }));
        int raftThread = Environment.CurrentManagedThreadId;
        leader.Snapshot();
        Assert.True(executorDone.Wait(TimeSpan.FromSeconds(2)));
        leader.Drain();
        Assert.Equal(raftThread, log.CommitThreadId);
    }

    [Fact]
    public void OverwrittenProposal_doesNotReportSuccess()
    {
        using var cluster = new KvClusterTests.KvFixture(Heartbeat);
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(200));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        cluster.Harness.Advance(80);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
        cluster.Harness.Isolate("n1");
        Assert.Null(cluster.Clients["n1"].Put("k", "stale"));
        Assert.Equal(2, cluster.Harness.Log("n1").LastIndex);
        cluster.Harness.Advance(400);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n2").Role);
        cluster.Clients["n2"].Put("k", "fresh");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(20);
        Assert.Equal("fresh", cluster.Clients["n2"].Get("k"));
        cluster.Harness.Heal("n1");
        cluster.Harness.Advance(80);
        Assert.Equal("fresh", cluster.Clients["n1"].Get("k"));
        cluster.Harness.Isolate("n2");
        for (int i = 0; i < 40 && cluster.Harness.Node("n1").Role != Role.Leader; i++)
        {
            cluster.Harness.Advance(20);
        }

        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
        Assert.Null(cluster.Clients["n1"].AwaitCommitted());
    }

    [Fact]
    public void RestartedNode_insideTheLease_doesNotGrantAVote()
    {
        var heartbeat = TimeSpan.FromMilliseconds(20);
        using var cluster = new KvClusterTests.KvFixture(heartbeat);
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(150));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(150));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000));
        cluster.Harness.Advance(150);
        cluster.Clients["n1"].Put("k", "v");
        cluster.Harness.DrainAll();
        Assert.True(cluster.Harness.Node("n1").QuorumLeaseValid);
        IPersistentState state = cluster.Harness.State("n2");
        IRaftLog log = cluster.Harness.Log("n2");
        Assert.True(state.CurrentTerm > 0);
        cluster.Harness.Transport.PartitionBidirectional("n1", "n2");
        cluster.Harness.Transport.PartitionBidirectional("n3", "n2");
        var restarted = new RaftNode(
            "n2",
            ["n1", "n3"],
            cluster.Harness.Clock,
            cluster.Harness.Transport,
            state,
            log,
            TimeSpan.FromMilliseconds(150),
            heartbeat);
        restarted.SetElectionJitter(static timeout => timeout);
        cluster.Harness.Transport.Reregister("n2", restarted.Receive);
        restarted.Start();
        long term = state.CurrentTerm;
        restarted.Receive(new Envelope(
            "n3",
            "n2",
            new RequestVote(term + 1, "n3", log.LastIndex, log.LastTerm)));
        restarted.Drain();
        Assert.NotEqual("n3", state.VotedFor);
        Assert.True(cluster.Harness.Node("n1").QuorumLeaseValid);
        Assert.Equal("v", cluster.Clients["n1"].BoundedStaleGet("k"));
    }

    [Fact]
    public void TermZeroNode_grantsAVoteImmediately()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        RaftNode fresh = cluster.Node("n1");
        Assert.Equal(0, cluster.State("n1").CurrentTerm);
        fresh.Receive(new Envelope("n2", "n1", new RequestVote(1, "n2", 0, 0)));
        fresh.Drain();
        Assert.Equal("n2", cluster.State("n1").VotedFor);
    }

    [Fact]
    public void MoreThan64ConcurrentReads_allComplete()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.NotNull(cluster.Propose("n1", "v"u8.ToArray()));
        cluster.DrainAll();
        RaftNode leader = cluster.Node("n1");
        var tickets = new List<long>();
        for (int i = 0; i < 80; i++)
        {
            ReadIndexStart? started = leader.BeginReadIndex();
            Assert.NotNull(started);
            tickets.Add(started.Value.Ticket);
        }

        cluster.DrainAll();
        cluster.Advance(20);
        foreach (long ticket in tickets)
        {
            Assert.True(leader.ReadIndexSatisfied(ticket), "ticket " + ticket);
        }
    }

    [Fact]
    public void IdleRead_startsOneRoundWithoutTheHeartbeatTimer()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        Assert.NotNull(cluster.Propose("n1", "v"u8.ToArray()));
        cluster.DrainAll();
        int heartbeats = 0;
        RaftNode n2 = cluster.Node("n2");
        cluster.Transport.Reregister("n2", envelope =>
        {
            if (envelope.Payload is AppendEntries)
            {
                heartbeats++;
            }

            n2.Receive(envelope);
        });
        RaftNode leader = cluster.Node("n1");
        int before = heartbeats;
        var tickets = new List<long>();
        for (int i = 0; i < 8; i++)
        {
            ReadIndexStart? started = leader.BeginReadIndex();
            Assert.NotNull(started);
            tickets.Add(started.Value.Ticket);
        }

        leader.Drain();
        Assert.Equal(before + 1, heartbeats);
        cluster.DrainAll();
        foreach (long ticket in tickets)
        {
            Assert.True(leader.ReadIndexSatisfied(ticket), "ticket " + ticket);
        }
    }

    [Fact]
    public void SkewedFollowerClock_leaderLeaseShrinksByDriftBound()
    {
        var leaderClock = new FakeClock();
        var followerClock = new ScaledClock(leaderClock, 2);
        var transport = new InMemoryTransport(leaderClock);
        var leaderState = new InMemoryPersistentState();
        var followerState = new InMemoryPersistentState();
        var leaderLog = new InMemoryRaftLog();
        var followerLog = new InMemoryRaftLog();
        var leader = new RaftNode(
            "n1",
            ["n2"],
            leaderClock,
            transport,
            leaderState,
            leaderLog,
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(1_000));
        var follower = new RaftNode(
            "n2",
            ["n1"],
            followerClock,
            transport,
            followerState,
            followerLog,
            TimeSpan.FromMilliseconds(10_000),
            Heartbeat);
        leader.SetElectionJitter(static timeout => timeout);
        follower.SetElectionJitter(static timeout => timeout);
        leader.SetClockDriftBound(0.5);
        transport.Register("n1", leader.Receive);
        transport.Register("n2", follower.Receive);
        leader.Start();
        follower.Start();
        for (int i = 0; i < 40 && leader.Role != Role.Leader; i++)
        {
            leaderClock.Advance(10);
            leader.Drain();
            follower.Drain();
        }

        Assert.Equal(Role.Leader, leader.Role);
        Assert.NotNull(leader.Propose("v"u8.ToArray()));
        leader.Drain();
        follower.Drain();
        leader.Drain();
        Assert.True(leader.QuorumLeaseValid);
        transport.PartitionBidirectional("n1", "n2");
        leaderClock.Advance(40);
        leader.Drain();
        follower.Drain();
        Assert.True(leader.QuorumLeaseValid);
        follower.Receive(new Envelope(
            "intruder",
            "n2",
            new RequestVote(leader.CurrentTerm + 1, "intruder", followerLog.LastIndex, followerLog.LastTerm)));
        follower.Drain();
        Assert.Equal("n1", followerState.VotedFor);
        leaderClock.Advance(20);
        leader.Drain();
        Assert.False(leader.QuorumLeaseValid);
    }

    [Fact]
    public void Apply_doesNotWaitOnSnapshotEncode()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodePut("k", "a"));
        using var entered = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        store.WhileEncoding = () =>
        {
            entered.Set();
            release.Wait();
        };
        StateCapture capture = store.CaptureState();
        var encoder = new Thread(() => capture.Full());
        encoder.IsBackground = true;
        encoder.Start();
        Assert.True(entered.Wait(TimeSpan.FromSeconds(2)));
        using var applied = new ManualResetEventSlim(false);
        var applier = new Thread(() =>
        {
            store.Apply(2, KvCommandCodec.EncodePut("k", "b"));
            applied.Set();
        });
        applier.IsBackground = true;
        applier.Start();
        try
        {
            Assert.True(applied.Wait(TimeSpan.FromMilliseconds(200)));
            Assert.Equal("b", store.Get("k"));
        }
        finally
        {
            release.Set();
            Assert.True(encoder.Join(TimeSpan.FromSeconds(2)));
            Assert.True(applier.Join(TimeSpan.FromSeconds(2)));
        }
    }

    [Fact]
    public void ApplyAtOrBelowRestoredIndex_isSkipped()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodePut("k", "new"));
        store.Apply(2, KvCommandCodec.EncodePut("k", "newer"));
        byte[] snapshot = store.Snapshot();
        store.RestoreChecked(2, 2, snapshot);
        store.AfterEpochSample = () => store.RestoreChecked(2, 2, snapshot);
        store.Apply(1, KvCommandCodec.EncodePut("k", "old"));
        Assert.Equal("newer", store.Get("k"));
    }

    private static string? StressLog(IRaftLog log)
    {
        for (int i = 1; i <= 32; i++)
        {
            log.Append(new LogEntry(i, 1, [1]));
        }

        Exception? error = null;
        using var start = new Barrier(2);
        var appender = new Thread(() =>
        {
            start.SignalAndWait();
            for (int i = 0; i < 3000 && error is null; i++)
            {
                try
                {
                    long next = log.LastIndex + 1;
                    log.Append(new LogEntry(next, 1, [1]));
                }
                catch (Exception ex)
                {
                    error ??= ex;
                }
            }
        });
        var compactor = new Thread(() =>
        {
            start.SignalAndWait();
            for (int i = 0; i < 400 && error is null; i++)
            {
                try
                {
                    long last = log.LastIndex;
                    long included = log.LastIncludedIndex;
                    if (last > included + 4)
                    {
                        log.CompactThrough(last - 2, 1, [1]);
                    }
                }
                catch (Exception ex)
                {
                    error ??= ex;
                }
            }
        });
        appender.Start();
        compactor.Start();
        appender.Join();
        compactor.Join();
        if (error is not null)
        {
            return error.GetType().Name + ": " + error.Message;
        }

        long first = log.FirstIndex;
        long end = log.LastIndex;
        for (long index = first; index <= end; index++)
        {
            LogEntry entry = log.Read(index);
            if (entry.Index != index)
            {
                return "index " + index + " held " + entry.Index;
            }
        }

        if (log is IDisposable disposable)
        {
            disposable.Dispose();
        }

        return null;
    }

    private sealed class SlowEncodeMachine : IStateMachine
    {
        public ManualResetEventSlim InEncode { get; } = new(false);

        public ManualResetEventSlim Release { get; } = new(false);

        public void Arm() => Release.Reset();

        public void Apply(long index, byte[] command)
        {
        }

        public byte[] Snapshot()
        {
            InEncode.Set();
            Release.Wait();
            return [1];
        }
    }

    /// <summary>Records the thread that publishes a compaction.</summary>
    private sealed class ThreadWatchLog : IRaftLog
    {
        private readonly InMemoryRaftLog _inner = new();

        public ManualResetEventSlim Committed { get; } = new(false);

        public int CommitThreadId { get; private set; }

        public void Append(LogEntry entry) => _inner.Append(entry);

        public LogEntry Read(long index) => _inner.Read(index);

        public long LastIndex => _inner.LastIndex;

        public long LastTerm => _inner.LastTerm;

        public long DurableIndex => _inner.DurableIndex;

        public void TruncateFrom(long index) => _inner.TruncateFrom(index);

        public long LastIncludedIndex => _inner.LastIncludedIndex;

        public long LastIncludedTerm => _inner.LastIncludedTerm;

        public byte[] SnapshotBytes() => _inner.SnapshotBytes();

        public void CompactThrough(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot) =>
            _inner.CompactThrough(lastIncludedIndex, lastIncludedTerm, snapshot);

        public StagedCompaction StageCompaction(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot)
        {
            return new StagedCompaction(
                static () => { },
                () =>
                {
                    CommitThreadId = Environment.CurrentManagedThreadId;
                    _inner.CompactThrough(lastIncludedIndex, lastIncludedTerm, snapshot);
                    Committed.Set();
                });
        }

        public void Force() => _inner.Force();
    }

    /// <summary>Follower clock. One inner advance moves this clock by <paramref name="scale"/>.</summary>
    private sealed class ScaledClock : IRaftClock
    {
        private readonly FakeClock _inner;
        private readonly int _scale;

        public ScaledClock(FakeClock inner, int scale)
        {
            _inner = inner;
            _scale = scale;
        }

        public long Millis => _inner.Millis * _scale;

        public void OnAdvance(Action listener) => _inner.OnAdvance(listener);
    }
}
