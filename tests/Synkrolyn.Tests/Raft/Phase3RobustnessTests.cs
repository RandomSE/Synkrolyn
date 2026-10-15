using System.Net;
using System.Net.Sockets;
using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

public class Phase3RobustnessTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);
    private static readonly ManualResetEventSlim Park = new(false);

    [Fact]
    public void Finding6_appendBatch_staysUnderFrameCap()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        cluster.Isolate("n1");
        RaftNode leader = cluster.Node("n1");
        byte[] command = new byte[100_000];
        for (int i = 0; i < 12; i++)
        {
            command[0] = (byte)i;
            Assert.NotNull(leader.Propose(command));
        }

        leader.Drain();
        Assert.Equal(Role.Leader, leader.Role);
        Assert.True(cluster.Log("n1").LastIndex >= 12);
        int maxBytes = 0;
        RaftNode n2 = cluster.Node("n2");
        cluster.Transport.Reregister("n2", envelope =>
        {
            if (envelope.Payload is AppendEntries append)
            {
                int bytes = 0;
                foreach (LogEntry entry in append.Entries)
                {
                    bytes += entry.Command.Length;
                }

                if (bytes > maxBytes)
                {
                    maxBytes = bytes;
                }
            }

            n2.Receive(envelope);
        });
        cluster.Heal("n1");
        cluster.Advance(20);
        Assert.True(maxBytes > 0);
        Assert.True(maxBytes <= 512 * 1024);
    }

    [Fact]
    public void Finding6_defaultSnapshotChunk_fitsInFrame()
    {
        var cluster = new ClusterHarness();
        RaftNode node = cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        Assert.InRange(node.SnapshotChunkSize, 1, FrameCodec.DefaultMaxPayloadBytes / 2);
    }

    [Fact]
    public void Finding6_snapshotLargerThanOneRecord_reopens()
    {
        string dir = Directory.CreateTempSubdirectory("synkrolyn-big-snap-").FullName;
        try
        {
            byte[] snapshot = new byte[ChecksummedRecords.MaxPayloadBytes + 64];
            snapshot[0] = 7;
            snapshot[^1] = 9;
            using (var log = new FileRaftLog(dir))
            {
                log.Append(new LogEntry(1, 1, [1]));
                log.Force();
                log.CompactThrough(1, 1, snapshot);
            }

            using var reopened = new FileRaftLog(dir);
            Assert.Equal(snapshot, reopened.SnapshotBytes());
            Assert.Equal(1, reopened.LastIncludedIndex);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Finding14_crashBetweenSnapshotAndLogRename_reopens()
    {
        string dir = Directory.CreateTempSubdirectory("synkrolyn-rename-gap-").FullName;
        try
        {
            byte[] first = [1, 2];
            byte[] second = [3, 4, 5];
            using (var log = new FileRaftLog(dir))
            {
                log.Append(new LogEntry(1, 1, [1]));
                log.Append(new LogEntry(2, 1, [2]));
                log.Append(new LogEntry(3, 1, [3]));
                log.Force();
                log.CompactThrough(1, 1, first);
            }

            byte[] payload = new byte[16 + second.Length];
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(payload, 2);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(payload.AsSpan(8), 1);
            second.CopyTo(payload.AsSpan(16));
            string snapshotTmp = Path.Combine(dir, FileRaftLog.SnapshotFileName + ".partial");
            using (var channel = new FileStream(snapshotTmp, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                ChecksummedRecords.WriteRecords(channel, payload);
                channel.Flush(true);
            }

            FilePersistentState.Replace(snapshotTmp, Path.Combine(dir, FileRaftLog.SnapshotFileName));
            using var reopened = new FileRaftLog(dir);
            Assert.Equal(2, reopened.LastIncludedIndex);
            Assert.Equal(1, reopened.LastIncludedTerm);
            Assert.Equal(second, reopened.SnapshotBytes());
            Assert.Equal(3, reopened.LastIndex);
            Assert.Equal(new byte[] { 3 }, reopened.Read(3).Command);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Finding6_mailboxOverflow_dropsInboundRpc()
    {
        var cluster = new ClusterHarness();
        RaftNode node = cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        var noise = new AppendEntries(1, "n2", 0, 0, [], 0, 1);
        for (int i = 0; i < 20_000; i++)
        {
            node.Receive(new Envelope("n2", "n1", noise));
        }

        Assert.False(node.Fatal);
        node.Drain();
        Assert.False(node.Fatal);
    }

    [Fact]
    public void Finding6_unknownRpc_failStopsInsteadOfThrowing()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        RaftNode node = cluster.Node("n1");
        node.Receive(new Envelope("n2", "n1", "not-an-rpc"));
        Exception? error = Record.Exception(() => node.Drain());
        Assert.Null(error);
        Assert.True(node.Fatal);
        Assert.NotEqual(Role.Leader, node.Role);
    }

    [Fact]
    public void Finding8_heartbeat_resendsCurrentSnapshotChunk()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        cluster.Propose("n1", "a"u8.ToArray());
        cluster.Node("n1").Snapshot();
        cluster.DrainAll();
        cluster.ReplaceNode(
            "n3",
            ["n1", "n2"],
            TimeSpan.FromMilliseconds(400),
            Heartbeat,
            IStateMachine.NoOp(),
            new InMemoryPersistentState(),
            new InMemoryRaftLog());
        int chunks = 0;
        RaftNode n3 = cluster.Node("n3");
        RaftNode leader = cluster.Node("n1");
        cluster.Transport.Reregister("n1", envelope =>
        {
            if (envelope.Payload is InstallSnapshotResponse)
            {
                return;
            }

            leader.Receive(envelope);
        });
        cluster.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is InstallSnapshot)
            {
                chunks++;
            }

            n3.Receive(envelope);
        });
        for (int i = 0; i < 8 && chunks < 1; i++)
        {
            cluster.Advance(20);
        }

        int afterFirst = chunks;
        Assert.True(afterFirst >= 1);
        cluster.Advance(20);
        Assert.True(chunks > afterFirst);
    }

    [Fact]
    public void InstallSnapshot_resentOffsetZero_doesNotRestart()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        RaftNode follower = cluster.Node("n2");
        long term = cluster.Node("n1").CurrentTerm;
        var replies = new List<InstallSnapshotResponse>();
        cluster.Transport.Reregister("n1", envelope =>
        {
            if (envelope.Payload is InstallSnapshotResponse response)
            {
                replies.Add(response);
            }
        });
        byte[] chunk = new byte[8];
        follower.Receive(new Envelope("n1", "n2", new InstallSnapshot(term, "n1", 20, term, 0, chunk, false)));
        follower.Drain();
        follower.Receive(new Envelope("n1", "n2", new InstallSnapshot(term, "n1", 20, term, 8, chunk, false)));
        follower.Drain();
        Assert.Equal(16, replies[^1].NextOffset);
        int beforeResend = replies.Count;
        follower.Receive(new Envelope("n1", "n2", new InstallSnapshot(term, "n1", 20, term, 0, chunk, false)));
        follower.Drain();
        Assert.Equal(16, replies[^1].NextOffset);
        Assert.True(replies.Count > beforeResend);
        follower.Receive(new Envelope("n1", "n2", new InstallSnapshot(term, "n1", 20, term, 16, chunk, false)));
        follower.Drain();
        Assert.Equal(24, replies[^1].NextOffset);
    }

    [Fact]
    public void InstallSnapshot_droppedConnection_resumesFromLastAckedOffset()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        RaftNode leader = cluster.Node("n1");
        Assert.Equal(Role.Leader, leader.Role);
        leader.SnapshotChunkSize = 16;
        cluster.Propose("n1", "alpha-value"u8.ToArray());
        cluster.Propose("n1", "beta-value-that-is-longer"u8.ToArray());
        leader.Snapshot();
        cluster.DrainAll();
        Assert.True(cluster.Log("n1").SnapshotBytes().Length > 16);
        cluster.ReplaceNode(
            "n3",
            ["n1", "n2"],
            TimeSpan.FromMilliseconds(400),
            Heartbeat,
            IStateMachine.NoOp(),
            new InMemoryPersistentState(),
            new InMemoryRaftLog());
        var offsets = new List<long>();
        RaftNode n3 = cluster.Node("n3");
        cluster.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is not InstallSnapshot snap)
            {
                n3.Receive(envelope);
                return;
            }

            offsets.Add(snap.Offset);
            // The first chunk is delivered. The first try at every later offset is
            // the connection dying mid-install. A repeat of that same offset is the
            // resume, and it must not have fallen back to offset 0.
            int seen = 0;
            foreach (long offset in offsets)
            {
                if (offset == snap.Offset)
                {
                    seen++;
                }
            }

            if (snap.Offset == 0 || seen > 1)
            {
                n3.Receive(envelope);
            }
        });
        cluster.Transport.Reregister("n1", leader.Receive);
        for (int i = 0; i < 40 && !ResumedFromAck(offsets); i++)
        {
            cluster.Advance(20);
        }

        Assert.True(offsets.Count >= 2, "snapshot transfer did not resume");
        Assert.Equal(0, offsets[0]);
        long resumed = offsets[1];
        Assert.NotEqual(0, resumed);
        Assert.Contains(resumed, offsets.Skip(2));
        Assert.DoesNotContain(0L, offsets.Skip(1));
    }

    [Fact]
    public void Finding7_applyOverlappingRestore_keepsTheCommand()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodePut("k", "a"));
        byte[] snapshot = store.Snapshot();
        store.AfterEpochSample = () => store.Restore(snapshot);
        store.Apply(2, KvCommandCodec.EncodePut("k", "b"));
        Assert.Equal("b", store.Get("k"));
    }

    [Fact]
    public void Finding14_secondLogOnSameDirectory_isRejected()
    {
        string dir = Directory.CreateTempSubdirectory("synkrolyn-lock-").FullName;
        try
        {
            var first = new FileRaftLog(dir);
            Assert.Throws<IOException>(() => new FileRaftLog(dir));
            first.Dispose();
            using var again = new FileRaftLog(dir);
            again.Append(new LogEntry(1, 1, [1]));
            again.Force();
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Finding14_partialForce_isRolledBackBeforeRetry()
    {
        string dir = Directory.CreateTempSubdirectory("synkrolyn-force-").FullName;
        try
        {
            var failOnce = true;
            var log = new FileRaftLog(dir, channel =>
            {
                if (!failOnce)
                {
                    return;
                }

                failOnce = false;
                channel.Write(new byte[8]);
                throw new IOException("torn");
            });
            log.Append(new LogEntry(1, 1, "kept"u8.ToArray()));
            Assert.Throws<IOException>(() => log.Force());
            log.Force();
            log.Dispose();
            using var reopened = new FileRaftLog(dir);
            Assert.Equal("kept"u8.ToArray(), reopened.Read(1).Command);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Finding14_faultHooks_doNotCrossInstances()
    {
        string tornDir = Directory.CreateTempSubdirectory("synkrolyn-hook-torn-").FullName;
        string cleanDir = Directory.CreateTempSubdirectory("synkrolyn-hook-clean-").FullName;
        try
        {
            var torn = new FileRaftLog(tornDir, channel =>
            {
                channel.Write(new byte[8]);
                throw new IOException("torn");
            });
            var cleanCalls = 0;
            var clean = new FileRaftLog(cleanDir, _ => Interlocked.Increment(ref cleanCalls));
            torn.Append(new LogEntry(1, 1, "lost"u8.ToArray()));
            clean.Append(new LogEntry(1, 1, "kept"u8.ToArray()));
            var start = new Barrier(2);
            Exception? tornError = null;
            Exception? cleanError = null;
            var tornThread = new Thread(() =>
            {
                start.SignalAndWait();
                tornError = Record.Exception(() => torn.Force());
            });
            var cleanThread = new Thread(() =>
            {
                start.SignalAndWait();
                cleanError = Record.Exception(() => clean.Force());
            });
            tornThread.Start();
            cleanThread.Start();
            tornThread.Join();
            cleanThread.Join();
            Assert.IsType<IOException>(tornError);
            Assert.Equal("torn", tornError.Message);
            Assert.Null(cleanError);
            Assert.Equal(1, cleanCalls);
            torn.CrashWithoutForce();
            clean.Dispose();
            using var reopenedTorn = new FileRaftLog(tornDir);
            using var reopenedClean = new FileRaftLog(cleanDir);
            Assert.Equal(0, reopenedTorn.LastIndex);
            Assert.Equal("kept"u8.ToArray(), reopenedClean.Read(1).Command);
        }
        finally
        {
            Directory.Delete(tornDir, true);
            Directory.Delete(cleanDir, true);
        }
    }

    [Fact]
    public void Finding16_unackedHeartbeats_doNotGrowSendTimesWithoutBound()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(150), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(150);
        cluster.Transport.PartitionBidirectional("n1", "n3");
        for (int i = 0; i < 40; i++)
        {
            cluster.Advance(20);
        }

        Assert.InRange(cluster.Node("n1").PendingAeStampCount("n3"), 1, 12);
    }

    [Fact]
    public void Finding16_repeatedConflict_doesNotRereadTheWholeLog()
    {
        string dir = Directory.CreateTempSubdirectory("synkrolyn-scan-").FullName;
        try
        {
            var log = new FileRaftLog(dir);
            var cluster = new ClusterHarness();
            cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat, log: log);
            cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(400), Heartbeat);
            cluster.Advance(80);
            cluster.Isolate("n1");
            RaftNode leader = cluster.Node("n1");
            for (int i = 0; i < 40; i++)
            {
                Assert.NotNull(leader.Propose([(byte)i]));
            }

            leader.Drain();
            long term = leader.CurrentTerm;
            int before = log.DiskReadCount;
            var reject = new AppendEntriesResponse(term, false, 0, 0, term, 1, 0);
            leader.Receive(new Envelope("n2", "n1", reject));
            leader.Drain();
            leader.Receive(new Envelope("n2", "n1", reject));
            leader.Drain();
            int reread = log.DiskReadCount - before;
            Assert.True(reread < 40, "two conflicts reread " + reread);
            cluster.CloseDurableHandles();
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Finding7_restoreAndSnapshot_waitUntilApplyIsIdle()
    {
        var cluster = new ClusterHarness();
        var machine = new BlockingMachine { BlockAt = 2 };
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        RaftNode follower = cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(400), Heartbeat, machine);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        Assert.Equal(1, follower.LastApplied);
        follower.SetApplyExecutor(action => ThreadPool.QueueUserWorkItem(_ => action()));
        cluster.Propose("n1", "v"u8.ToArray());
        Assert.True(machine.Entered.Wait(TimeSpan.FromSeconds(2)));
        follower.Snapshot();
        Assert.Equal(0, cluster.Log("n2").LastIncludedIndex);
        byte[] snap = MembershipSnapshot.Encode(["n1", "n2"], [], [9]);
        long term = cluster.Node("n1").CurrentTerm;
        follower.Receive(new Envelope("n1", "n2", new InstallSnapshot(term, "n1", 20, term, 0, snap, true)));
        follower.Drain();
        Assert.Equal(0, machine.Restores);
        machine.Release.Set();
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (machine.Restores < 1 && DateTime.UtcNow < deadline)
        {
            follower.Drain();
            Park.Wait(TimeSpan.FromMilliseconds(1));
        }

        Assert.Equal(1, machine.Restores);
        Assert.False(machine.ApplyAfterRestore);
        Assert.Equal(20, follower.LastApplied);
        Assert.True(cluster.Log("n2").LastIncludedIndex >= 1);
    }

    [Fact]
    public void Finding6_raftThreadFault_failStops()
    {
        var clock = new FaultAfterClock(3);
        var transport = new SocketTransport("n1", new RpcWireCodec());
        transport.Bind();
        var node = new RaftNode(
            "n1",
            ["n2"],
            clock,
            transport,
            new InMemoryPersistentState(),
            new InMemoryRaftLog(),
            TimeSpan.FromMilliseconds(10_000),
            Heartbeat);
        using RaftRuntime runtime = RaftRuntime.ForTests(node, transport, clock);
        runtime.Start();
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (!node.Fatal && DateTime.UtcNow < deadline)
        {
            Park.Wait(TimeSpan.FromMilliseconds(1));
        }

        Assert.True(node.Fatal);
        Assert.NotNull(runtime.Uncaught);
    }

    [Fact]
    public void Finding16_fakeClock_isNotInTheProductionAssembly()
    {
        Assert.Null(typeof(RaftNode).Assembly.GetType("Synkrolyn.Raft.FakeClock"));
    }

    [Fact]
    public void Finding13_dispose_closesAcceptedSockets()
    {
        var transport = new SocketTransport("n1", new RpcWireCodec());
        transport.Bind();
        transport.SetHandler(_ => { });
        transport.Start();
        using var raw = new TcpClient();
        raw.Connect(IPAddress.Loopback, transport.LocalPort);
        transport.Dispose();
        // One millisecond. An open socket returns false. A reset or a FIN is readable with nothing buffered.
        bool closed = raw.Client.Poll(1_000, SelectMode.SelectRead) && raw.Client.Available == 0;
        Assert.True(closed);
    }

    private static bool ResumedFromAck(List<long> offsets)
    {
        if (offsets.Count < 2 || offsets[1] == 0)
        {
            return false;
        }

        long resumed = offsets[1];
        int seen = 0;
        foreach (long offset in offsets)
        {
            if (offset == resumed)
            {
                seen++;
            }
        }

        return seen >= 2;
    }

    private sealed class BlockingMachine : IStateMachine
    {
        public long BlockAt { get; init; } = long.MaxValue;

        public ManualResetEventSlim Entered { get; } = new(false);

        public ManualResetEventSlim Release { get; } = new(false);

        public int Restores;

        public int Applies;

        public bool ApplyAfterRestore;

        public void Apply(long index, byte[] command)
        {
            if (index >= BlockAt)
            {
                Entered.Set();
                Release.Wait();
                if (Restores > 0)
                {
                    ApplyAfterRestore = true;
                }
            }

            Applies++;
        }

        public byte[] Snapshot() => [1];

        public void Restore(byte[] snapshot) => Restores++;
    }

    private sealed class FaultAfterClock : IRaftClock
    {
        private readonly int _failAt;
        private int _calls;

        public FaultAfterClock(int failAt) => _failAt = failAt;

        public long Millis
        {
            get
            {
                int call = Interlocked.Increment(ref _calls);
                if (call >= _failAt)
                {
                    throw new InvalidOperationException("clock fault");
                }

                return 0;
            }
        }

        public void OnAdvance(Action listener)
        {
        }
    }
}
