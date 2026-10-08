using Synkrolyn.Net;
using Synkrolyn.Raft;
using Synkrolyn.Tests.Kv;

namespace Synkrolyn.Tests.Raft;

/// <summary>
/// Phase 2: ReadIndex tickets, lease bound, PreVote rounds, leadership transfer, and the
/// claim tests that were missing on main.
/// </summary>
public class Phase2ReadElectionTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void Finding2_completedReadIndex_doesNotAuthorizeALaterRead()
    {
        using var cluster = new KvClusterTests.KvFixture(Heartbeat);
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(200));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000));
        cluster.Harness.Advance(200);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
        cluster.Clients["n1"].Put("k", "v");
        cluster.Harness.DrainAll();
        Assert.Equal(2, cluster.Harness.Node("n1").BeginReadIndex()?.Index);
        cluster.Harness.DrainAll();
        Assert.Equal("v", cluster.Clients["n1"].LinearizableGet("k"));

        cluster.Harness.Transport.SetLinkDelay("n2", "n1", TimeSpan.FromMilliseconds(220));
        cluster.Harness.Transport.SetLinkDelay("n3", "n1", TimeSpan.FromMilliseconds(220));
        cluster.Harness.Advance(20);
        cluster.Harness.Advance(220);
        RaftNode leader = cluster.Harness.Node("n1");
        Assert.Equal(Role.Leader, leader.Role);
        Assert.False(leader.QuorumLeaseValid);
        Assert.Null(cluster.Clients["n1"].LinearizableGet("k"));
    }

    [Fact]
    public void Finding2_concurrentReads_shareTheNextAppendEntries()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
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
        Assert.NotNull(leader.BeginReadIndex());
        Assert.NotNull(leader.BeginReadIndex());
        // An idle leader starts one round. The second read piggybacks on it.
        Assert.Equal(before + 1, heartbeats);
    }

    [Fact]
    public void Finding3_staleLeaderLease_expiresWhenPreVoteWindowOpens()
    {
        var heartbeat = TimeSpan.FromMilliseconds(40);
        using var cluster = new KvClusterTests.KvFixture(heartbeat);
        cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(150));
        cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(150));
        cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(150));
        cluster.Harness.Advance(150);
        Assert.Equal(Role.Leader, cluster.Harness.Node("n1").Role);
        cluster.Clients["n1"].Put("k", "old");
        cluster.Harness.DrainAll();
        cluster.Harness.Advance(40);

        cluster.Harness.Transport.PartitionBidirectional("n1", "n3");
        cluster.Harness.Advance(40);
        cluster.Harness.Transport.PartitionBidirectional("n1", "n2");
        long elapsed = 0;
        while (elapsed < 1000
            && cluster.Harness.Node("n2").Role != Role.Leader
            && cluster.Harness.Node("n3").Role != Role.Leader)
        {
            cluster.Harness.Advance(10);
            elapsed += 10;
        }

        bool n3Leader = cluster.Harness.Node("n3").Role == Role.Leader;
        bool n2Leader = cluster.Harness.Node("n2").Role == Role.Leader;
        Assert.True(n2Leader || n3Leader, "failover elapsed " + elapsed);
        Assert.False(cluster.Harness.Node("n1").QuorumLeaseValid);
        string fresh = n3Leader ? "n3" : "n2";
        cluster.Clients[fresh].Put("k", "fresh");
        cluster.Harness.DrainAll();
        Assert.Equal("fresh", cluster.Clients[fresh].Get("k"));
        Assert.Null(cluster.Clients["n1"].LinearizableGet("k"));
        Assert.Equal("old", cluster.Clients["n1"].Get("k"));
        Assert.True(elapsed <= 150 + 40, "failover elapsed " + elapsed);
    }

    [Fact]
    public void Finding15_latePreVoteGrant_doesNotStartElection()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Transport.PartitionBidirectional("n1", "n3");
        cluster.Transport.DelayNext("n2", "n1", TimeSpan.FromMilliseconds(150));
        cluster.Advance(100);
        cluster.Transport.DropNext("n2", "n1");
        cluster.Advance(100);
        long term = cluster.Node("n1").CurrentTerm;
        Assert.Equal(Role.Follower, cluster.Node("n1").Role);
        cluster.Advance(50);
        Assert.Equal(Role.Follower, cluster.Node("n1").Role);
        Assert.Equal(term, cluster.Node("n1").CurrentTerm);
    }

    [Fact]
    public void Finding15_validAppendEntries_clearsPreCandidate()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        long term = cluster.Node("n1").CurrentTerm;
        cluster.Isolate("n2");
        cluster.Advance(400);
        Assert.Equal(Role.Follower, cluster.Node("n2").Role);
        Assert.Equal(term, cluster.Node("n2").CurrentTerm);
        cluster.Heal("n2");
        cluster.Transport.PartitionBidirectional("n2", "n3");
        cluster.Advance(20);
        cluster.Node("n2").Receive(new Envelope("n3", "n2", new RequestVoteResponse(term, true, true)));
        cluster.Node("n2").Drain();
        Assert.Equal(Role.Follower, cluster.Node("n2").Role);
        Assert.Equal(term, cluster.Node("n2").CurrentTerm);
    }

    [Fact]
    public void Finding17_timeoutNowHigherTerm_campaignsAtTermPlusOne()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.AddNode("n2", ["n1"], TimeSpan.FromMilliseconds(10_000), Heartbeat);
        cluster.Node("n1").Receive(new Envelope("n2", "n1", new TimeoutNow(4, "n2")));
        cluster.Node("n1").Drain();
        Assert.Equal(5, cluster.Node("n1").CurrentTerm);
        Assert.Equal(Role.Candidate, cluster.Node("n1").Role);
    }

    [Fact]
    public void Finding17_transfer_keepsLeaderUntilTargetWinsThenResumesOnTimeout()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(300), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(300), Heartbeat);
        cluster.Advance(100);
        cluster.Propose("n1", "catch-up"u8.ToArray());
        RaftNode leader = cluster.Node("n1");
        long term = leader.CurrentTerm;
        Assert.True(leader.TransferLeadership("n2"));
        Assert.Equal(Role.Leader, leader.Role);
        Assert.Equal(LeadershipTransferStatus.AwaitingWinner, leader.TransferStatus);
        Assert.Null(leader.Propose("after-timeout-now"u8.ToArray()));
        cluster.DrainAll();
        Assert.Equal(Role.Leader, cluster.Node("n2").Role);
        Assert.Equal(term + 1, cluster.Node("n2").CurrentTerm);
        Assert.Equal(LeadershipTransferStatus.Succeeded, leader.TransferStatus);
        Assert.Equal(Role.Follower, leader.Role);

        RaftNode n2 = cluster.Node("n2");
        cluster.Transport.Reregister("n3", envelope =>
        {
            if (envelope.Payload is TimeoutNow)
            {
                return;
            }

            cluster.Node("n3").Receive(envelope);
        });
        Assert.True(n2.TransferLeadership("n3"));
        Assert.Equal(Role.Leader, n2.Role);
        Assert.Null(n2.Propose("paused"u8.ToArray()));
        cluster.Advance(300);
        Assert.Equal(LeadershipTransferStatus.Aborted, n2.TransferStatus);
        Assert.Equal(Role.Leader, n2.Role);
        Assert.NotNull(n2.Propose("resumed"u8.ToArray()));
    }

    [Fact]
    public void Finding17_voteFromNonVoter_doesNotElect()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        cluster.Isolate("n1");
        cluster.Node("n1").StartElection();
        cluster.Node("n1").Drain();
        Assert.Equal(Role.Candidate, cluster.Node("n1").Role);
        long term = cluster.Node("n1").CurrentTerm;
        cluster.Node("n1").Receive(new Envelope("learner", "n1", new RequestVoteResponse(term, true)));
        cluster.Node("n1").Drain();
        Assert.Equal(Role.Candidate, cluster.Node("n1").Role);
        Assert.Equal(term, cluster.Node("n1").CurrentTerm);
    }

    [Fact]
    public void Follower_acksAppendEntriesOnlyAfterForce()
    {
        var clock = new FakeClock();
        var inner = new InMemoryTransport(clock);
        var logs = new Dictionary<string, IRaftLog>();
        var early = new List<string>();
        var transport = new ForceObservingTransport(inner, logs, early);
        var nodes = new Dictionary<string, RaftNode>();
        string dir2 = Directory.CreateTempSubdirectory("synkrolyn-f2-").FullName;
        string dir3 = Directory.CreateTempSubdirectory("synkrolyn-f3-").FullName;
        try
        {
            AddDurable("n1", ["n2", "n3"], new InMemoryRaftLog(), 80);
            AddDurable("n2", ["n1", "n3"], new FileRaftLog(dir2), 400);
            AddDurable("n3", ["n1", "n2"], new FileRaftLog(dir3), 400);
            clock.Advance(80);
            Drain();
            Assert.Equal(Role.Leader, nodes["n1"].Role);
            Assert.NotNull(nodes["n1"].Propose("payload"u8.ToArray()));
            Drain();
            Assert.Empty(early);
            Assert.True(logs["n2"].DurableIndex >= nodes["n1"].CommitIndex);
            Assert.True(logs["n3"].DurableIndex >= nodes["n1"].CommitIndex);
            Assert.True(((FileRaftLog)logs["n2"]).ForceCount >= 1);
            Assert.True(((FileRaftLog)logs["n3"]).ForceCount >= 1);
        }
        finally
        {
            foreach (IRaftLog log in logs.Values)
            {
                if (log is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }

            Directory.Delete(dir2, true);
            Directory.Delete(dir3, true);
        }

        void AddDurable(string id, string[] peers, IRaftLog log, int electionMs)
        {
            logs[id] = log;
            var node = new RaftNode(
                id,
                peers,
                clock,
                transport,
                new InMemoryPersistentState(),
                log,
                TimeSpan.FromMilliseconds(electionMs),
                Heartbeat);
            node.SetElectionJitter(static timeout => timeout);
            node.SnapshotThreshold = 0;
            inner.Register(id, node.Receive);
            nodes[id] = node;
            node.Start();
        }

        void Drain()
        {
            bool progress;
            do
            {
                progress = false;
                foreach (RaftNode node in nodes.Values)
                {
                    if (node.Drain())
                    {
                        progress = true;
                    }
                }
            }
            while (progress);
        }
    }

    [Fact]
    public void Figure8_oldTermEntriesOnAMajority_commitOnlyWithCurrentTermEntry()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(500), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(500), Heartbeat);
        cluster.Advance(80);
        RaftNode leader = cluster.Node("n1");
        long oldTerm = leader.CurrentTerm;
        cluster.Isolate("n1");
        for (int i = 0; i < 16; i++)
        {
            Assert.NotNull(leader.Propose(new[] { (byte)i }));
        }

        leader.Drain();
        long oldTail = cluster.Log("n1").LastIndex;
        Assert.True(oldTail >= 17);
        Assert.Equal(1, leader.CommitIndex);
        cluster.Advance(80);
        Assert.Equal(Role.Follower, leader.Role);
        cluster.Heal("n1");
        cluster.Transport.PartitionBidirectional("n2", "n3");
        HoldCurrentTermEntries(cluster, "n2", oldTerm);
        HoldCurrentTermEntries(cluster, "n3", oldTerm);
        for (int i = 0; i < 80 && leader.Role != Role.Leader; i++)
        {
            cluster.Advance(10);
        }

        cluster.Transport.HealBidirectional("n2", "n3");
        Assert.Equal(Role.Leader, leader.Role);
        Assert.True(leader.CurrentTerm > oldTerm);
        Assert.Equal(oldTerm, cluster.Log("n1").Read(oldTail).Term);
        Assert.Equal(leader.CurrentTerm, cluster.Log("n1").Read(cluster.Log("n1").LastIndex).Term);
        Assert.True(cluster.Log("n2").LastIndex >= oldTail || cluster.Log("n3").LastIndex >= oldTail);
        Assert.Equal(1, leader.CommitIndex);
        cluster.Transport.Reregister("n2", cluster.Node("n2").Receive);
        cluster.Transport.Reregister("n3", cluster.Node("n3").Receive);
        cluster.Advance(40);
        Assert.True(leader.CommitIndex >= cluster.Log("n1").LastIndex);
        Assert.Equal(leader.CurrentTerm, cluster.Log("n1").Read(leader.CommitIndex).Term);
        Assert.Equal(new[] { (byte)0 }, cluster.Log("n1").Read(2).Command);
        Assert.True(leader.CommitIndex >= 2);
    }

    [Fact]
    public void Section541_deniesStaleLogAndGrantsMatchingLog()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        cluster.Propose("n1", "durable"u8.ToArray());
        RaftNode n1 = cluster.Node("n1");
        long term = n1.CurrentTerm;
        long last = cluster.Log("n1").LastIndex;
        long lastTerm = cluster.Log("n1").LastTerm;
        n1.Receive(new Envelope("n3", "n1", new RequestVote(term + 1, "n3", 0, 0)));
        n1.Drain();
        Assert.Null(cluster.State("n1").VotedFor);
        Assert.NotEqual(Role.Leader, n1.Role);
        n1.Receive(new Envelope("n2", "n1", new RequestVote(term + 2, "n2", last, lastTerm)));
        n1.Drain();
        Assert.Equal("n2", cluster.State("n1").VotedFor);
    }

    [Fact]
    public void RequestVote_afterCompaction_usesSnapshotIndexAndTerm()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        cluster.Propose("n1", "kept"u8.ToArray());
        cluster.Node("n1").Snapshot();
        cluster.DrainAll();
        long included = cluster.Log("n1").LastIncludedIndex;
        long includedTerm = cluster.Log("n1").LastIncludedTerm;
        Assert.True(included >= 1);
        RaftNode n1 = cluster.Node("n1");
        long term = n1.CurrentTerm;
        n1.Receive(new Envelope("n3", "n1", new RequestVote(term + 1, "n3", included, includedTerm - 1)));
        n1.Drain();
        Assert.Null(cluster.State("n1").VotedFor);
        n1.Receive(new Envelope("n2", "n1", new RequestVote(term + 2, "n2", included, includedTerm)));
        n1.Drain();
        Assert.Equal("n2", cluster.State("n1").VotedFor);
    }

    [Fact]
    public void Leader_appendOnly_ignoresConflictingAppendEntries()
    {
        var cluster = new ClusterHarness();
        cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat);
        cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat);
        cluster.Advance(80);
        cluster.Propose("n1", "keep"u8.ToArray());
        cluster.Isolate("n1");
        cluster.Advance(80);
        cluster.Heal("n1");
        for (int i = 0; i < 50 && cluster.Node("n1").Role != Role.Leader; i++)
        {
            cluster.Advance(10);
        }

        RaftNode leader = cluster.Node("n1");
        Assert.Equal(Role.Leader, leader.Role);
        long term = leader.CurrentTerm;
        Assert.True(term > 1);
        Assert.Equal("keep"u8.ToArray(), cluster.Log("n1").Read(2).Command);
        long prevTerm = cluster.Log("n1").Read(1).Term;
        var conflict = new AppendEntries(
            term,
            "n2",
            1,
            prevTerm,
            [new LogEntry(2, term, "overwrite"u8.ToArray())],
            0,
            1);
        leader.Receive(new Envelope("n2", "n1", conflict));
        leader.Drain();
        Assert.Equal(Role.Leader, leader.Role);
        Assert.Equal(term, leader.CurrentTerm);
        Assert.Equal("keep"u8.ToArray(), cluster.Log("n1").Read(2).Command);
    }

    private static void HoldCurrentTermEntries(ClusterHarness cluster, string id, long oldTerm)
    {
        RaftNode node = cluster.Node(id);
        cluster.Transport.Reregister(id, envelope =>
        {
            if (envelope.Payload is AppendEntries append
                && (append.PrevLogTerm > oldTerm || append.Entries.Any(entry => entry.Term > oldTerm)))
            {
                if (cluster.Log(id).LastIndex > 1)
                {
                    return;
                }
            }

            node.Receive(envelope);
        });
    }

    private sealed class ForceObservingTransport : ITransport
    {
        private readonly InMemoryTransport _inner;
        private readonly Dictionary<string, IRaftLog> _logs;
        private readonly List<string> _early;

        public ForceObservingTransport(InMemoryTransport inner, Dictionary<string, IRaftLog> logs, List<string> early)
        {
            _inner = inner;
            _logs = logs;
            _early = early;
        }

        public void Send(string sender, string recipient, object payload)
        {
            if (payload is AppendEntriesResponse { Success: true } ack
                && _logs.TryGetValue(sender, out IRaftLog? log)
                && log.DurableIndex < ack.MatchIndex)
            {
                _early.Add(sender + ":" + ack.MatchIndex + "<" + log.DurableIndex);
            }

            _inner.Send(sender, recipient, payload);
        }
    }
}
