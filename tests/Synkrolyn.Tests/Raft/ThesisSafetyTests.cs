using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

/// <summary>
/// Thesis safety regressions. Each test names the review finding it locks.
/// </summary>
public class ThesisSafetyTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void Finding1_configIsLiveOnAppend_andSecondChangeIsRejectedUntilCommit()
    {
        var cluster = NewTrio();
        Elect(cluster);
        cluster.AddNode("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n5", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Isolate("n2");
        cluster.Isolate("n3");
        Assert.NotNull(cluster.Node("n1").AddLearner("n4"));
        cluster.Node("n1").Drain();
        Assert.Contains("n4", cluster.Node("n1").LearnerPeerIds);
        Assert.True(cluster.Node("n1").CommitIndex < cluster.Log("n1").LastIndex);
        Assert.Null(cluster.Node("n1").AddLearner("n5"));
        Assert.Null(cluster.Node("n1").RemoveServer("n2"));
    }

    [Fact]
    public void Finding1_membershipChangeRequiresCommittedCurrentTermEntry()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Node("n1").StartElection();
        cluster.Node("n1").Drain();
        cluster.Node("n2").Drain();
        cluster.Node("n3").Drain();
        cluster.Node("n1").Drain();
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        Assert.True(cluster.Node("n1").CommitIndex < cluster.Log("n1").LastIndex);
        Assert.Null(cluster.Node("n1").AddLearner("n4"));
    }

    [Fact]
    public void Finding1_singleServerChangeRejectedWhileJoint()
    {
        var cluster = NewTrio();
        Elect(cluster);
        cluster.Node("n1").SetHoldJoint(true);
        Assert.NotNull(cluster.Node("n1").EnterJoint(["n1", "n2"]));
        cluster.DrainAll();
        Assert.True(cluster.Node("n1").InJointConsensus);
        Assert.Null(cluster.Node("n1").AddLearner("n4"));
        Assert.Null(cluster.Node("n1").RemoveServer("n2"));
    }

    [Fact]
    public void Finding1_uncommittedConfigRevertsOnTruncation()
    {
        var cluster = NewTrio();
        Elect(cluster);
        cluster.AddNode("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        Assert.NotNull(cluster.Node("n1").AddLearner("n4"));
        cluster.Node("n2").Drain();
        Assert.Contains("n4", cluster.Node("n2").LearnerPeerIds);
        Assert.True(cluster.Node("n2").CommitIndex < cluster.Log("n2").LastIndex);
        long added = cluster.Log("n2").LastIndex;
        long higher = cluster.Node("n1").CurrentTerm + 1;
        var conflict = new AppendEntries(
            higher,
            "n1",
            added - 1,
            cluster.Log("n2").Read(added - 1).Term,
            [new LogEntry(added, higher, "other"u8.ToArray())],
            cluster.Node("n2").CommitIndex);
        cluster.Node("n2").Receive(new Envelope("n1", "n2", conflict));
        cluster.Node("n2").Drain();
        Assert.DoesNotContain("n4", cluster.Node("n2").LearnerPeerIds);
        Assert.Equal("other"u8.ToArray(), cluster.Log("n2").Read(added).Command);
    }

    [Fact]
    public void Finding1_restartReplaysUncommittedSingleServerConfig()
    {
        var cluster = new ClusterHarness();
        var state = new InMemoryPersistentState();
        var log = new InMemoryRaftLog();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat, state: state, log: log);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        cluster.Isolate("n2");
        cluster.Isolate("n3");
        Assert.NotNull(cluster.Node("n1").AddLearner("n4"));
        cluster.Node("n1").Drain();
        Assert.Contains("n4", cluster.Node("n1").LearnerPeerIds);

        var restarted = new RaftNode(
            "n1",
            ["n2", "n3"],
            cluster.Clock,
            cluster.Transport,
            state,
            log,
            TimeSpan.FromMilliseconds(80),
            Heartbeat);
        cluster.Transport.Reregister("n1", restarted.Receive);
        restarted.Start();
        Assert.Contains("n4", restarted.LearnerPeerIds);
    }

    [Fact]
    public void Finding1_twoValuesAtOneIndex_overlappingMembershipCannotBothCommit()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("a", ["b", "c"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("b", ["a", "c"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("c", ["a", "b"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("a").Role);
        cluster.AddNode("d", ["a", "b", "c"], TimeSpan.FromMilliseconds(10_000), Heartbeat);

        Assert.NotNull(cluster.Node("a").AddLearner("d"));
        cluster.DrainAll();
        cluster.Advance(20);
        Assert.Contains("d", cluster.Node("a").LearnerPeerIds);
        Assert.True(cluster.Node("a").MatchIndex("d") >= cluster.Node("a").CommitIndex);
        cluster.Isolate("b");

        Assert.NotNull(cluster.Node("a").PromoteVoter("d"));
        cluster.DrainAll();
        long? removed = cluster.Node("a").RemoveServer("c");
        Assert.NotNull(removed);
        for (int i = 0; i < 8; i++)
        {
            cluster.Transport.DropNext("a", "c");
        }

        cluster.DrainAll();
        cluster.Transport.PartitionBidirectional("a", "c");
        cluster.Transport.PartitionBidirectional("d", "c");
        cluster.Transport.PartitionBidirectional("d", "b");
        cluster.Transport.HealBidirectional("b", "c");
        cluster.Node("c").StartElection();
        cluster.DrainAll();
        if (cluster.Node("c").Role == Role.Leader)
        {
            cluster.Node("c").Propose("from-c"u8.ToArray());
            cluster.DrainAll();
        }

        if (cluster.Node("a").Role == Role.Leader)
        {
            cluster.Node("a").Propose("from-a"u8.ToArray());
            cluster.DrainAll();
        }

        long horizon = Math.Max(cluster.Log("a").LastIndex, cluster.Log("c").LastIndex);
        for (long index = 1; index <= horizon; index++)
        {
            byte[]? left = CommittedCommand(cluster, "a", index);
            byte[]? right = CommittedCommand(cluster, "c", index);
            if (left is not null && right is not null)
            {
                Assert.Equal(left, right);
            }
        }
    }

    [Fact]
    public void Finding10_snapshotStoresConfigAsOfLastIncludedIndex()
    {
        var cluster = NewTrio();
        Elect(cluster);
        cluster.Node("n1").Propose("seed"u8.ToArray());
        cluster.DrainAll();
        long applied = cluster.Node("n1").LastApplied;
        cluster.AddNode("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Isolate("n2");
        cluster.Isolate("n3");
        Assert.NotNull(cluster.Node("n1").AddLearner("n4"));
        cluster.Node("n1").Drain();
        Assert.Contains("n4", cluster.Node("n1").LearnerPeerIds);
        cluster.Node("n1").SnapshotThrough(applied);
        MembershipSnapshot.Decoded decoded = MembershipSnapshot.Decode(cluster.Log("n1").SnapshotBytes());
        Assert.True(decoded.HasConfig);
        Assert.DoesNotContain("n4", decoded.Learners);
        Assert.Contains("n1", decoded.Voters);
        Assert.Contains("n2", decoded.Voters);
        Assert.Contains("n3", decoded.Voters);
    }

    [Fact]
    public void Finding9_nonVoterLeaderDoesNotCountItself_andStepsDownAfterCnewCommits()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("a", ["b", "c"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("b", ["a", "c"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("c", ["a", "b"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("a").Role);
        cluster.Node("a").SetHoldJoint(true);
        Assert.NotNull(cluster.Node("a").EnterJoint(["b"]));
        cluster.DrainAll();
        Assert.True(cluster.Node("a").InJointConsensus);
        Assert.NotNull(cluster.Node("a").LeaveJoint());
        cluster.DrainAll();
        Assert.False(cluster.Node("a").IsVoter);
        Assert.NotEqual(Role.Leader, cluster.Node("a").Role);

        var cluster2 = new ClusterHarness();
        cluster2.AddNode("a", ["b", "c"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster2.AddNode("b", ["a", "c"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster2.AddNode("c", ["a", "b"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster2.Advance(80);
        cluster2.Node("a").SetHoldJoint(true);
        Assert.NotNull(cluster2.Node("a").EnterJoint(["b"]));
        cluster2.DrainAll();
        cluster2.Isolate("b");
        Assert.NotNull(cluster2.Node("a").LeaveJoint());
        cluster2.Node("a").Drain();
        Assert.False(cluster2.Node("a").IsVoter);
        cluster2.Advance(80);
        Assert.False(cluster2.Node("a").QuorumLeaseValid);
        Assert.NotEqual(Role.Leader, cluster2.Node("a").Role);
    }

    [Fact]
    public void Finding4_installSnapshotAtOrBehindCommitEchoesAndDoesNotRollBack()
    {
        var cluster = NewTrio();
        Elect(cluster);
        cluster.Node("n1").Propose("keep"u8.ToArray());
        cluster.DrainAll();
        long commit = cluster.Node("n2").CommitIndex;
        Assert.True(commit >= 1);
        Assert.Equal(0, cluster.Log("n2").LastIncludedIndex);
        byte[] bogus = MembershipSnapshot.Encode(["n1", "n2", "n3"], [], "rolled"u8.ToArray());
        var install = new InstallSnapshot(cluster.Node("n1").CurrentTerm, "n1", 1, 1, 0, bogus, true);
        cluster.Node("n2").Receive(new Envelope("n1", "n2", install));
        cluster.Node("n2").Drain();
        Assert.Equal(0, cluster.Node("n2").SnapshotsInstalled);
        Assert.Equal(0, cluster.Log("n2").LastIncludedIndex);
        Assert.True(cluster.Node("n2").CommitIndex >= commit);
        Assert.Equal("keep"u8.ToArray(), cluster.Log("n2").Read(cluster.Log("n2").LastIndex).Command);
    }

    [Fact]
    public void Finding4_conflictingSnapshotTermDiscardsSuffix()
    {
        var entries = new List<LogEntry>
        {
            new(1, 1, "a"u8.ToArray()),
            new(2, 1, "b"u8.ToArray()),
            new(3, 2, "stale"u8.ToArray()),
        };
        LogCompaction.Apply(entries, 0, 2, 4, "snap"u8.ToArray());
        Assert.Empty(entries);
    }

    [Fact]
    public void Finding4_staleFailureDoesNotPullNextIndexBelowMatch()
    {
        var cluster = NewTrio();
        Elect(cluster);
        cluster.Propose("n1", "one"u8.ToArray());
        cluster.Propose("n1", "two"u8.ToArray());
        long match = cluster.Node("n1").MatchIndex("n2");
        Assert.True(match >= 2);
        var stale = new AppendEntriesResponse(cluster.Node("n1").CurrentTerm, false, 0, 1, 0, 1, 1, 1);
        cluster.Node("n1").Receive(new Envelope("n2", "n1", stale));
        cluster.Node("n1").Drain();
        Assert.True(cluster.Node("n1").NextIndex("n2") >= match + 1);
    }

    [Fact]
    public void Finding5_truncateOnlyAtFirstConflictingEntry()
    {
        var cluster = new ClusterHarness();
        var log = new RecordingLog();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat, log: log);
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        cluster.Propose("n1", "a"u8.ToArray());
        cluster.Propose("n1", "b"u8.ToArray());
        cluster.Propose("n1", "c"u8.ToArray());
        Assert.True(cluster.Log("n2").LastIndex >= 3);
        log.TruncateCalls.Clear();
        long prev = 1;
        long prevTerm = cluster.Log("n2").Read(prev).Term;
        var batch = new AppendEntries(
            cluster.Node("n1").CurrentTerm,
            "n1",
            prev,
            prevTerm,
            [
                cluster.Log("n2").Read(2),
                new LogEntry(3, cluster.Node("n1").CurrentTerm + 1, "conflict"u8.ToArray()),
            ],
            cluster.Node("n2").CommitIndex);
        cluster.Node("n2").Receive(new Envelope("n1", "n2", batch));
        cluster.Node("n2").Drain();
        Assert.Contains(3L, log.TruncateCalls);
        Assert.DoesNotContain(2L, log.TruncateCalls);
        Assert.Equal(cluster.Log("n2").Read(2).Command, log.Inner.Read(2).Command);
    }

    [Fact]
    public void Finding11_failedApplyDoesNotAdvanceLastApplied()
    {
        var cluster = new ClusterHarness();
        var machine = new ThrowingMachine();
        cluster.AddNode("n1", [], TimeSpan.FromMilliseconds(40), Heartbeat, machine);
        cluster.Advance(40);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
        long applied = cluster.Node("n1").LastApplied;
        machine.Throw = true;
        try
        {
            cluster.Node("n1").Propose("boom"u8.ToArray());
            cluster.Node("n1").Drain();
        }
        catch (InvalidOperationException)
        {
            // inline apply on main propagates; the queued completion is the bug
        }

        cluster.Node("n1").Drain();
        Assert.Equal(applied, cluster.Node("n1").LastApplied);
        Assert.True(cluster.Node("n1").Fatal);
    }

    private static ClusterHarness NewTrio()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        return cluster;
    }

    private static void Elect(ClusterHarness cluster)
    {
        cluster.Advance(80);
        Assert.Equal(Role.Leader, cluster.Node("n1").Role);
    }

    private static byte[]? CommittedCommand(ClusterHarness cluster, string id, long index)
    {
        RaftNode node = cluster.Node(id);
        IRaftLog log = cluster.Log(id);
        if (node.CommitIndex < index || index > log.LastIndex || index <= log.LastIncludedIndex)
        {
            return null;
        }

        return log.Read(index).Command;
    }

    private sealed class ThrowingMachine : IStateMachine
    {
        public bool Throw { get; set; }

        public void Apply(long index, byte[] command)
        {
            if (Throw)
            {
                throw new InvalidOperationException("apply failed");
            }
        }
    }

    private sealed class RecordingLog : IRaftLog
    {
        private readonly InMemoryRaftLog _inner = new();

        public InMemoryRaftLog Inner => _inner;

        public List<long> TruncateCalls { get; } = [];

        public void Append(LogEntry entry) => _inner.Append(entry);

        public LogEntry Read(long index) => _inner.Read(index);

        public long LastIndex => _inner.LastIndex;

        public long LastTerm => _inner.LastTerm;

        public long DurableIndex => _inner.DurableIndex;

        public void TruncateFrom(long index)
        {
            TruncateCalls.Add(index);
            _inner.TruncateFrom(index);
        }

        public long LastIncludedIndex => _inner.LastIncludedIndex;

        public long LastIncludedTerm => _inner.LastIncludedTerm;

        public byte[] SnapshotBytes() => _inner.SnapshotBytes();

        public void CompactThrough(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot) =>
            _inner.CompactThrough(lastIncludedIndex, lastIncludedTerm, snapshot);

        public void Force() => _inner.Force();
    }
}
