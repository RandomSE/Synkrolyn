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
    public void Finding7_concurrentApplyAndRead_doesNotThrow()
    {
        var store = new InMemoryKvStore();
        var failures = new List<Exception>();
        var start = new ManualResetEventSlim(false);
        var writers = new Thread(() =>
        {
            start.Wait();
            for (int i = 1; i <= 4_000; i++)
            {
                try
                {
                    store.Apply(i, KvCommandCodec.EncodePut("k", i.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                }
                catch (Exception ex)
                {
                    lock (failures)
                    {
                        failures.Add(ex);
                    }
                }
            }
        });
        var readers = new Thread(() =>
        {
            start.Wait();
            for (int i = 0; i < 4_000; i++)
            {
                try
                {
                    _ = store.Get("k");
                    _ = store.Snapshot();
                }
                catch (Exception ex)
                {
                    lock (failures)
                    {
                        failures.Add(ex);
                    }
                }
            }
        });
        writers.Start();
        readers.Start();
        start.Set();
        writers.Join();
        readers.Join();
        Assert.Empty(failures);
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
        raw.ReceiveTimeout = 1000;
        transport.Dispose();
        try
        {
            int read = raw.GetStream().Read(new byte[8]);
            Assert.Equal(0, read);
        }
        catch (IOException ex) when (ex.InnerException is SocketException socket)
        {
            // Stop resets a handshake that has not been accepted yet (10054).
            // A timeout means Dispose left the socket open.
            Assert.NotEqual(SocketError.TimedOut, socket.SocketErrorCode);
        }
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
