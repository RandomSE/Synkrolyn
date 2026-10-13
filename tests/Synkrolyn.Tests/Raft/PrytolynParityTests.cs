using System.Buffers.Binary;
using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

public class PrytolynParityTests
{
    [Fact]
    public void FakeClock_explicitStart_negativeStart_overflow_andListenerOrder()
    {
        Assert.Equal(0, new FakeClock().Millis);
        Assert.Equal(40, new FakeClock(40).Millis);
        Assert.Throws<ArgumentException>(() => new FakeClock(-1));
        var clock = new FakeClock(long.MaxValue - 1);
        Assert.Throws<OverflowException>(() => clock.Advance(2));
        Assert.Equal(long.MaxValue - 1, clock.Millis);
        clock.Advance(0);
        Assert.Equal(long.MaxValue - 1, clock.Millis);

        var ordered = new FakeClock();
        Assert.Throws<ArgumentNullException>(() => ordered.OnAdvance(null!));
        var seen = new List<int>();
        ordered.OnAdvance(() => seen.Add(1));
        ordered.OnAdvance(() => seen.Add(2));
        ordered.Advance(0);
        Assert.Empty(seen);
        ordered.Advance(1);
        ordered.Advance(1);
        Assert.Equal([1, 2, 1, 2], seen);
    }

    [Fact]
    public void SystemClock_rejectsNullListener()
    {
        var clock = new SystemRaftClock(() => 0);
        Assert.Throws<ArgumentNullException>(() => clock.OnAdvance(null!));
        clock.OnAdvance(() => { });
    }

    [Fact]
    public void InMemoryPersistentState_termAndVoteRules()
    {
        var state = new InMemoryPersistentState();
        Assert.Equal(0, state.CurrentTerm);
        Assert.Null(state.VotedFor);
        state.SetCurrentTerm(0);
        Assert.Equal(0, state.CurrentTerm);
        state.SetCurrentTerm(2);
        Assert.Equal(2, state.CurrentTerm);
        state.RecordVote("n1");
        state.RecordVote("n1");
        state.SetCurrentTerm(2);
        Assert.Equal("n1", state.VotedFor);
        Assert.Throws<InvalidOperationException>(() => state.RecordVote("n2"));
        Assert.Throws<ArgumentException>(() => state.RecordVote(" "));
        Assert.Throws<ArgumentException>(() => state.RecordVote(null!));
        state.SetCurrentTerm(4);
        Assert.Null(state.VotedFor);
        Assert.Throws<ArgumentException>(() => state.SetCurrentTerm(3));
        Assert.Throws<ArgumentException>(() => state.SetCurrentTerm(0));
        Assert.Throws<ArgumentException>(() => state.SetCurrentTerm(-1));
    }

    [Fact]
    public void FilePersistentState_advancingTermClearsVoteOnDisk()
    {
        string dir = Directory.CreateTempSubdirectory("synkrolyn-parity-hs-").FullName;
        try
        {
            using (var state = new FilePersistentState(dir))
            {
                state.SetCurrentTerm(1);
                state.RecordVote("n1");
                state.SetCurrentTerm(2);
                Assert.Null(state.VotedFor);
            }

            using var reopened = new FilePersistentState(dir);
            Assert.Equal(2, reopened.CurrentTerm);
            Assert.Null(reopened.VotedFor);
            Assert.Throws<ArgumentException>(() => reopened.SetCurrentTerm(1));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void InMemoryRaftLog_truncateEdges_emptyCommand_andRejectedTerms()
    {
        var log = new InMemoryRaftLog();
        Assert.Throws<ArgumentException>(() => log.Read(0));
        Assert.Throws<ArgumentException>(() => log.TruncateFrom(0));
        log.Append(new LogEntry(1, 1, []));
        Assert.Empty(log.Read(1).Command);
        log.Append(new LogEntry(2, 1, "b"u8.ToArray()));
        log.TruncateFrom(3);
        Assert.Equal(2, log.LastIndex);
        Assert.Throws<ArgumentException>(() => log.TruncateFrom(4));
        log.TruncateFrom(2);
        Assert.Equal(1, log.LastIndex);
        log.Force();
        Assert.Equal(1, log.DurableIndex);
        log.TruncateFrom(1);
        Assert.Equal(0, log.LastIndex);
        Assert.Equal(0, log.DurableIndex);
        log.Append(new LogEntry(1, 1, "a"u8.ToArray()));
        log.CompactThrough(1, 1, []);
        Assert.Throws<ArgumentException>(() => log.TruncateFrom(1));
        Assert.Throws<ArgumentException>(() => log.Append(new LogEntry(1, 1, "dup"u8.ToArray())));
        Assert.Throws<ArgumentException>(() => new LogEntry(1, -1, []));
        Assert.Throws<ArgumentNullException>(() => new LogEntry(1, 1, null!));
        Assert.Throws<ArgumentNullException>(() => log.Append(null!));
    }

    [Fact]
    public void KvCommand_blankKey_emptyValue_andGarbageDoesNotMutate()
    {
        Assert.Equal(
            [nameof(KvCommandCodec.Op.Put), nameof(KvCommandCodec.Op.Delete), nameof(KvCommandCodec.Op.Register), nameof(KvCommandCodec.Op.Tick)],
            Enum.GetNames<KvCommandCodec.Op>());
        KvCommandCodec.Command empty = KvCommandCodec.Decode(KvCommandCodec.EncodePut("k", ""));
        Assert.Equal("", empty.Value);
        KvCommandCodec.Command serial = KvCommandCodec.Decode(KvCommandCodec.EncodePut("k", "v", "c1", 3));
        Assert.Equal("c1", serial.ClientId);
        Assert.Equal(3, serial.Serial);
        KvCommandCodec.Command old = KvCommandCodec.Decode(KvCommandCodec.EncodePut("k", "v"));
        Assert.Equal(0, old.Serial);
        Assert.Throws<ArgumentException>(() => KvCommandCodec.EncodePut(" ", "v"));
        Assert.Throws<ArgumentException>(() => KvCommandCodec.Decode([0x7F]));
        Assert.Throws<ArgumentException>(() => KvCommandCodec.Decode([0x01, 0x00]));

        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodePut("k", "v"));
        Assert.Throws<ArgumentException>(() => store.Apply(2, [0x7F]));
        Assert.Equal("v", store.Get("k"));
    }

    [Fact]
    public void FrameCodec_rejectsOverCapAndNegativeLength()
    {
        Assert.Equal(8, FrameCodec.Encode(new byte[4], 4).Length);
        byte[] emptyFrame = FrameCodec.Encode([], 0);
        Assert.Empty(Assert.Single(new FrameCodec.Decoder(0).Push(emptyFrame)));
        Assert.Throws<ArgumentException>(() => FrameCodec.Encode(new byte[8], 4));
        var over = new FrameCodec.Decoder(4);
        byte[] length = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(length, 5);
        Assert.Throws<ArgumentException>(() => over.Push(length));
        var negative = new FrameCodec.Decoder(4);
        BinaryPrimitives.WriteInt32BigEndian(length, -1);
        Assert.Throws<ArgumentException>(() => negative.Push(length));
        Assert.Throws<ArgumentException>(() => new FrameCodec.Decoder(-1));
    }

    [Fact]
    public void DeltaSnapshot_rejectsMissingBase_truncatedSerial_wrongBase_andZeroSerialCount()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodePut("a", "1", "c1", 7));
        byte[] full = store.Snapshot();
        byte[] orphan = KvSnapshotDelta.Encode(0, new Dictionary<string, string>(), [], new Dictionary<string, long>());
        Assert.Throws<ArgumentException>(() => new InMemoryKvStore().Restore(orphan));

        var wrong = new InMemoryKvStore();
        wrong.Restore(full);
        Assert.Throws<ArgumentException>(() => wrong.RestoreChecked(0, 0, store.SnapshotDelta(5)));
        Assert.Equal("1", wrong.Get("a"));

        byte[] chopped = KvSnapshotDelta.Encode(0, new Dictionary<string, string>(), [], new Dictionary<string, long> { ["c1"] = 7 });
        var truncated = new InMemoryKvStore();
        truncated.Restore(full);
        Assert.Throws<ArgumentException>(() => truncated.RestoreChecked(0, 0, chopped[..^3]));
        Assert.Equal("1", truncated.Get("a"));

        var cleared = new InMemoryKvStore();
        cleared.Restore(full);
        cleared.RestoreChecked(0, 0, orphan);
        cleared.Apply(2, KvCommandCodec.EncodePut("a", "nope", "c1", 7));
        Assert.Equal("nope", cleared.Get("a"));

        var withSerial = new InMemoryKvStore();
        withSerial.Apply(1, KvCommandCodec.EncodePut("a", "1", "c1", 7));
        byte[] baseSnap = withSerial.Snapshot();
        withSerial.Apply(2, KvCommandCodec.EncodePut("b", "2"));
        var merged = new InMemoryKvStore();
        merged.Restore(baseSnap);
        merged.RestoreChecked(0, 0, withSerial.SnapshotDelta(0));
        merged.Apply(3, KvCommandCodec.EncodePut("a", "nope", "c1", 7));
        Assert.Equal("1", merged.Get("a"));
        Assert.Equal("2", merged.Get("b"));
    }

    [Fact]
    public void RangeContains_includesStart_andExcludesEnd()
    {
        var cluster = new ClusterHarness();
        RaftNode node = cluster.AddNode("n1", [], TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(20));
        var client = new KvClient(node, new InMemoryKvStore());
        var range = new RangeBinding("m", "t", client);
        Assert.False(range.Contains("a"));
        Assert.True(range.Contains("m"));
        Assert.True(range.Contains("s"));
        Assert.False(range.Contains("t"));
        Assert.True(new RangeBinding("m", "", client).Contains("z"));
    }

    [Fact]
    public void PreVote_higherIdWithStrictlyNewerLog_isGranted()
    {
        var cluster = new ClusterHarness();
        TimeSpan heartbeat = TimeSpan.FromMilliseconds(20);
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(500), heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(80), heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(100_000), heartbeat);
        cluster.Isolate("n1");
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n2").Role);
        Assert.NotNull(cluster.Propose("n2", "cmd"u8.ToArray()));
        cluster.DrainAll();
        Assert.True(cluster.Log("n2").LastIndex > cluster.Log("n1").LastIndex);
        cluster.Advance(420);
        cluster.Heal("n1");
        Assert.True(cluster.Node("n1").CurrentTerm < cluster.Node("n2").CurrentTerm || cluster.Log("n1").LastIndex == 0);
        var grants = new List<bool>();
        cluster.Transport.Reregister("n2", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && response.PreVote)
            {
                grants.Add(response.VoteGranted);
            }
        });
        IRaftLog newer = cluster.Log("n2");
        cluster.Transport.Send(
            "n2",
            "n1",
            new RequestVote(cluster.Node("n2").CurrentTerm, "n2", newer.LastIndex, newer.LastTerm, true));
        cluster.DrainAll();
        Assert.Equal([true], grants);
    }

    [Fact]
    public void InMemoryTransport_partitionSelf_stackedDrop_reverseIndependence_andRejectedDelay()
    {
        var clock = new FakeClock();
        var transport = new InMemoryTransport(clock);
        var a = new List<Envelope>();
        var b = new List<Envelope>();
        var c = new List<Envelope>();
        transport.Register("a", a.Add);
        transport.Register("b", b.Add);
        transport.Register("c", c.Add);
        Assert.Throws<InvalidOperationException>(() => transport.Register("a", a.Add));
        Assert.Throws<ArgumentException>(() => transport.Register(" ", a.Add));
        Assert.Throws<ArgumentNullException>(() => transport.Register("d", null!));
        Assert.Throws<ArgumentNullException>(() => new InMemoryTransport(null!));
        Assert.Throws<ArgumentException>(() => transport.PartitionBidirectional("a", "a"));
        Assert.Throws<ArgumentException>(() => transport.DropNext("a", "missing"));
        Assert.Throws<ArgumentException>(() => transport.HealBidirectional("missing", "a"));
        Assert.Throws<ArgumentException>(() => transport.Send("a", "b", "x", TimeSpan.FromMilliseconds(-1)));
        Assert.Throws<ArgumentNullException>(() => transport.Send("a", "b", null!));
        Assert.False(transport.Send("a", "missing", "x"));
        Assert.Throws<ArgumentException>(() => transport.Send("missing", "a", "x"));
        Assert.Throws<ArgumentException>(() => new Envelope(" ", "a", "x"));

        transport.DropNext("a", "b");
        transport.DropNext("a", "b");
        transport.Send("a", "b", "1");
        transport.Send("a", "b", "2");
        transport.Send("a", "b", "3");
        Assert.Equal("3", Assert.Single(b).Payload);
        transport.Send("b", "a", "back");
        Assert.Equal("back", Assert.Single(a).Payload);
        transport.Send("a", "c", "side");
        Assert.Equal("side", Assert.Single(c).Payload);
        transport.HealBidirectional("a", "b");
        transport.Send("a", "b", "after-heal");
        Assert.Equal("after-heal", b[^1].Payload);
        transport.PartitionBidirectional("a", "b");
        transport.PartitionBidirectional("a", "b");
        transport.Send("a", "c", "outsider");
        Assert.Equal("outsider", c[^1].Payload);
        int before = b.Count;
        transport.Send("a", "b", "held");
        Assert.Equal(before, b.Count);
        transport.SetLinkDelay("c", "a", TimeSpan.FromMilliseconds(10));
        transport.Send("c", "a", "later");
        Assert.DoesNotContain(a, envelope => Equals(envelope.Payload, "later"));
        clock.Advance(10);
        Assert.Contains(a, envelope => Equals(envelope.Payload, "later"));
    }

    [Fact]
    public void ElectionJitter_identityElectsAtT_doubleTimeoutDoesNot()
    {
        var identity = new ClusterHarness();
        RaftNode solo = identity.AddNode("solo", [], TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(20));
        solo.SetElectionJitter(static timeout => timeout);
        identity.Advance(49);
        Assert.Equal(Role.Follower, solo.Role);
        identity.Advance(1);
        Assert.Equal(Role.Leader, solo.Role);

        var doubled = new ClusterHarness();
        RaftNode slow = doubled.AddNode("solo", [], TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(20));
        slow.SetElectionJitter(static timeout => timeout * 2);
        doubled.Advance(50);
        Assert.Equal(Role.Follower, slow.Role);
        doubled.Advance(50);
        Assert.Equal(Role.Leader, slow.Role);
    }

    [Fact]
    public void FiveNode_electsOneLeader()
    {
        var cluster = new ClusterHarness();
        TimeSpan heartbeat = TimeSpan.FromMilliseconds(20);
        cluster.AddNode("n1", ["n2", "n3", "n4", "n5"], TimeSpan.FromMilliseconds(100), heartbeat);
        cluster.AddNode("n2", ["n1", "n3", "n4", "n5"], TimeSpan.FromMilliseconds(500), heartbeat);
        cluster.AddNode("n3", ["n1", "n2", "n4", "n5"], TimeSpan.FromMilliseconds(500), heartbeat);
        cluster.AddNode("n4", ["n1", "n2", "n3", "n5"], TimeSpan.FromMilliseconds(500), heartbeat);
        cluster.AddNode("n5", ["n1", "n2", "n3", "n4"], TimeSpan.FromMilliseconds(500), heartbeat);
        cluster.Advance(100);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        Assert.Single(cluster.Leaders());
    }

    [Fact]
    public void FollowerAlreadyVoted_deniesTheOtherCandidate()
    {
        var cluster = new ClusterHarness();
        TimeSpan heartbeat = TimeSpan.FromMilliseconds(20);
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(300), heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(300), heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(300), heartbeat);
        var toN1 = new List<bool>();
        var toN3 = new List<bool>();
        cluster.Transport.Reregister("n1", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && !response.PreVote)
            {
                toN1.Add(response.VoteGranted);
                return;
            }

            cluster.Node("n1").Receive(envelope);
        });
        cluster.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is RequestVoteResponse response && !response.PreVote)
            {
                toN3.Add(response.VoteGranted);
                return;
            }

            cluster.Node("n3").Receive(envelope);
        });
        cluster.Transport.Send("n1", "n2", new RequestVote(1, "n1", 0, 0));
        cluster.DrainAll();
        cluster.Transport.Send("n3", "n2", new RequestVote(1, "n3", 0, 0));
        cluster.DrainAll();
        Assert.Equal([true], toN1);
        Assert.Equal([false], toN3);
    }

    [Fact]
    public void HigherTermAppendEntries_stepsReceiverDown_olderTermLeavesTermUnchanged()
    {
        var cluster = new ClusterHarness();
        TimeSpan heartbeat = TimeSpan.FromMilliseconds(20);
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100), heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), heartbeat);
        cluster.Advance(100);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        long term = cluster.Node("n1").CurrentTerm;
        cluster.Transport.Send("n2", "n1", new AppendEntries(0, "n2", 0, 0, [], 0));
        cluster.DrainAll();
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        Assert.Equal(term, cluster.Node("n1").CurrentTerm);
        cluster.Transport.Send("n2", "n1", new AppendEntries(term + 1, "n2", 0, 0, [], 0));
        cluster.DrainAll();
        Assert.Equal(Role.Follower, cluster.Node("n1").Role);
        Assert.Equal(term + 1, cluster.Node("n1").CurrentTerm);
    }

    [Fact]
    public void BlankNodeId_andSelfPeer_areRejected()
    {
        var clock = new FakeClock();
        var transport = new InMemoryTransport(clock);
        Assert.Throws<ArgumentException>(() => new RaftNode(
            " ",
            [],
            clock,
            transport,
            new InMemoryPersistentState(),
            new InMemoryRaftLog(),
            TimeSpan.FromMilliseconds(50),
            TimeSpan.FromMilliseconds(20)));
        Assert.Throws<ArgumentException>(() => new RaftNode(
            "n1",
            ["n1"],
            clock,
            transport,
            new InMemoryPersistentState(),
            new InMemoryRaftLog(),
            TimeSpan.FromMilliseconds(50),
            TimeSpan.FromMilliseconds(20)));
    }

    [Fact]
    public void ColdMajorityWithoutCnewMajority_cannotCommitWhileJoint()
    {
        var cluster = new ClusterHarness();
        TimeSpan heartbeat = TimeSpan.FromMilliseconds(20);
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100), heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), heartbeat);
        cluster.Advance(100);
        cluster.AddNode("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000), heartbeat);
        cluster.Node("n1").SetHoldJoint(true);
        cluster.Isolate("n2");
        cluster.Isolate("n4");
        Assert.NotNull(cluster.Node("n1").EnterJoint(["n1", "n2", "n4"]));
        cluster.DrainAll();
        long joint = cluster.Log("n1").LastIndex;
        Assert.True(cluster.Node("n1").InJointConsensus);
        Assert.True(cluster.Node("n1").CommitIndex < joint);
        cluster.Heal("n4");
        cluster.Advance(200);
        Assert.True(cluster.Node("n1").CommitIndex >= joint);
    }

    [Fact]
    public void ProductionStart_setsSnapshotThreshold_forTestsStaysZero()
    {
        var testClock = new FakeClock();
        var testTransport = new SocketTransport("t", new RpcWireCodec());
        testTransport.Bind();
        var testNode = new RaftNode(
            "t",
            [],
            testClock,
            testTransport,
            new InMemoryPersistentState(),
            new InMemoryRaftLog(),
            TimeSpan.FromMilliseconds(1_000),
            TimeSpan.FromMilliseconds(200));
        using (RaftRuntime tests = RaftRuntime.ForTests(testNode, testTransport, testClock))
        {
            Assert.False(tests.ProductionJitterEnabled);
            tests.Start();
            Assert.Equal(0, testNode.SnapshotThreshold);
        }

        var prodClock = new FakeClock();
        var prodTransport = new SocketTransport("p", new RpcWireCodec());
        prodTransport.Bind();
        var prodNode = new RaftNode(
            "p",
            [],
            prodClock,
            prodTransport,
            new InMemoryPersistentState(),
            new InMemoryRaftLog(),
            TimeSpan.FromMilliseconds(1_000),
            TimeSpan.FromMilliseconds(200));
        using RaftRuntime production = RaftRuntime.ForProduction(prodNode, prodTransport, prodClock);
        Assert.True(production.ProductionJitterEnabled);
        production.Start();
        Assert.Equal(RaftRuntime.DefaultProductionSnapshotThreshold, prodNode.SnapshotThreshold);
    }

    [Fact]
    public void MembershipV1_decodesAsNotJoint()
    {
        byte[] voters = EncodeIds(["n1"]);
        byte[] learners = EncodeIds([]);
        byte[] blob = new byte[4 + 1 + voters.Length + learners.Length + 4];
        "PTCF"u8.CopyTo(blob);
        blob[4] = 1;
        voters.CopyTo(blob.AsSpan(5));
        learners.CopyTo(blob.AsSpan(5 + voters.Length));
        MembershipSnapshot.Decoded decoded = MembershipSnapshot.Decode(blob);
        Assert.False(decoded.InJoint);
        Assert.Contains("n1", decoded.Voters);
        MembershipSnapshot.Decoded current = MembershipSnapshot.Decode(MembershipSnapshot.Encode(["n1"], [], []));
        Assert.False(current.InJoint);
    }

    [Fact]
    public void FileKv_applyDoesNotPersist_snapshotAndCloseDo()
    {
        string dir = Directory.CreateTempSubdirectory("synkrolyn-parity-kv-").FullName;
        try
        {
            using var store = new FileKvStore(dir);
            int created = store.PersistCount;
            store.Apply(1, KvCommandCodec.EncodePut("k", "v", "c1", 1));
            Assert.Equal(created, store.PersistCount);
            Assert.Equal("v", store.Get("k"));
            store.Snapshot();
            Assert.Equal(created + 1, store.PersistCount);
            store.Dispose();
            Assert.Equal(created + 2, store.PersistCount);
            using var reopened = new FileKvStore(dir);
            Assert.Equal("v", reopened.Get("k"));
            reopened.Apply(2, KvCommandCodec.EncodePut("k", "two", "c1", 1));
            Assert.Equal("v", reopened.Get("k"));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void KvSnapshot_restoreReplacesState_roundTripsDeletes_andEmptySnapshotStaysEmpty()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodePut("keep", "1"));
        store.Apply(2, KvCommandCodec.EncodeDelete("keep"));
        store.Apply(3, KvCommandCodec.EncodePut("new", "2"));
        byte[] snap = store.Snapshot();
        var stale = new InMemoryKvStore();
        stale.Apply(1, KvCommandCodec.EncodePut("stale", "x"));
        stale.Restore(snap);
        Assert.Null(stale.Get("stale"));
        Assert.Null(stale.Get("keep"));
        Assert.Equal("2", stale.Get("new"));
        var empty = new InMemoryKvStore();
        empty.Restore(empty.Snapshot());
        Assert.Null(empty.Get("new"));
    }

    private static byte[] EncodeIds(string[] ids)
    {
        int size = 4;
        byte[][] encoded = new byte[ids.Length][];
        for (int i = 0; i < ids.Length; i++)
        {
            encoded[i] = System.Text.Encoding.UTF8.GetBytes(ids[i]);
            size += 4 + encoded[i].Length;
        }

        byte[] buf = new byte[size];
        BinaryPrimitives.WriteInt32BigEndian(buf, ids.Length);
        int pos = 4;
        foreach (byte[] bytes in encoded)
        {
            BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(pos), bytes.Length);
            pos += 4;
            bytes.CopyTo(buf.AsSpan(pos));
            pos += bytes.Length;
        }

        return buf;
    }
}
