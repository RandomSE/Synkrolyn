using Synkrolyn.Kv;
using Synkrolyn.Raft;
using Synkrolyn.Tests.Kv;

namespace Synkrolyn.Tests.Raft;

public sealed class MembershipChangeTests : IDisposable
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);
    private readonly KvClusterTests.KvFixture _cluster = new(Heartbeat);
    private readonly string _root = Directory.CreateTempSubdirectory("synkrolyn-membership-").FullName;

    [Fact]
    public void AddLearner_doesNotChangeMajority_untilPromotedAfterCatchUp()
    {
        Elect();
        _cluster.Clients["n1"].Put("k", "v");
        _cluster.Harness.DrainAll();

        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        Assert.True(_cluster.Harness.Node("n4").IsVoter);

        Assert.NotNull(_cluster.Harness.Node("n1").AddLearner("n4"));
        _cluster.Harness.DrainAll();
        _cluster.Harness.Advance(20);

        Assert.Contains("n4", _cluster.Harness.Node("n1").LearnerPeerIds);
        Assert.Equal(["n2", "n3"], _cluster.Harness.Node("n1").VoterPeerIds.OrderBy(id => id).ToArray());
        Assert.False(_cluster.Harness.Node("n4").IsVoter);
        Assert.Equal("v", _cluster.Stores["n4"].Get("k"));

        Assert.NotNull(_cluster.Harness.Node("n1").PromoteVoter("n4"));
        _cluster.Harness.DrainAll();
        Assert.Equal(["n2", "n3", "n4"], _cluster.Harness.Node("n1").VoterPeerIds.OrderBy(id => id).ToArray());
        Assert.DoesNotContain("n4", _cluster.Harness.Node("n1").LearnerPeerIds);
        Assert.True(_cluster.Harness.Node("n4").IsVoter);

        _cluster.Clients["n1"].Put("after", "promote");
        _cluster.Harness.DrainAll();
        Assert.Equal("promote", _cluster.Stores["n4"].Get("after"));
    }

    [Fact]
    public void PromoteVoter_refusedWhileEmptyLearnerIsBehind()
    {
        Elect();
        _cluster.Clients["n1"].Put("k", "v");
        _cluster.Harness.DrainAll();
        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        _cluster.Harness.Isolate("n4");
        Assert.NotNull(_cluster.Harness.Node("n1").AddLearner("n4"));
        _cluster.Harness.DrainAll();
        Assert.Null(_cluster.Harness.Node("n1").PromoteVoter("n4"));
        Assert.Contains("n4", _cluster.Harness.Node("n1").LearnerPeerIds);
        Assert.DoesNotContain("n4", _cluster.Harness.Node("n1").VoterPeerIds);
    }

    [Fact]
    public void RemoveVoter_oldNodeNoLongerRequiredForCommit()
    {
        Elect();
        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        _cluster.Harness.Node("n1").AddLearner("n4");
        _cluster.Harness.DrainAll();
        _cluster.Harness.Advance(20);
        _cluster.Harness.Node("n1").PromoteVoter("n4");
        _cluster.Harness.DrainAll();

        Assert.NotNull(_cluster.Harness.Node("n1").RemoveServer("n3"));
        _cluster.Harness.DrainAll();
        Assert.Equal(["n2", "n4"], _cluster.Harness.Node("n1").VoterPeerIds.OrderBy(id => id).ToArray());

        _cluster.Harness.Isolate("n3");
        _cluster.Clients["n1"].Put("gone", "n3");
        _cluster.Harness.DrainAll();
        Assert.Equal("n3", _cluster.Stores["n1"].Get("gone"));
        Assert.Equal("n3", _cluster.Stores["n4"].Get("gone"));
    }

    [Fact]
    public void RemoveVoterFromTwo_returnsEmpty_learnerRemoveReturnsIndex()
    {
        _cluster.Add("n1", ["n2"], TimeSpan.FromMilliseconds(80));
        _cluster.Add("n2", ["n1"], TimeSpan.FromMilliseconds(400));
        _cluster.Harness.Advance(80);
        Assert.Equal(Role.Leader, _cluster.Harness.Node("n1").Role);
        _cluster.Add("n4", ["n1", "n2"], TimeSpan.FromMilliseconds(10_000));
        Assert.NotNull(_cluster.Harness.Node("n1").AddLearner("n4"));
        _cluster.Harness.DrainAll();
        Assert.Null(_cluster.Harness.Node("n1").RemoveServer("n2"));
        Assert.NotNull(_cluster.Harness.Node("n1").RemoveServer("n4"));
        _cluster.Harness.DrainAll();
        Assert.DoesNotContain("n4", _cluster.Harness.Node("n1").LearnerPeerIds);
    }

    [Fact]
    public void EmptyLearner_cannotWinElection()
    {
        Elect();
        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        _cluster.Harness.Node("n1").AddLearner("n4");
        _cluster.Harness.DrainAll();
        _cluster.Harness.Node("n4").StartElection();
        _cluster.Harness.DrainAll();
        Assert.NotEqual(Role.Leader, _cluster.Harness.Node("n4").Role);
        Assert.False(_cluster.Harness.Node("n4").IsVoter);
    }

    [Fact]
    public void SnapshotAndHardState_persistConfig_restoreOnReplace()
    {
        string dir = Path.Combine(_root, "n1");
        var store = new InMemoryKvStore();
        var state = new FilePersistentState(dir);
        var log = new FileRaftLog(dir);
        _cluster.Harness.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat, store, state, log);
        _cluster.Stores["n1"] = store;
        _cluster.Clients["n1"] = new KvClient(_cluster.Harness.Node("n1"), store);
        _cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        _cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        _cluster.Harness.Advance(80);
        Assert.Equal(Role.Leader, _cluster.Harness.Node("n1").Role);

        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        _cluster.Harness.Node("n1").AddLearner("n4");
        _cluster.Harness.DrainAll();
        _cluster.Harness.Advance(20);
        Assert.True(File.Exists(Path.Combine(dir, FilePersistentState.ConfigFileName)));
        _cluster.Harness.Node("n1").Snapshot();

        var restored = new InMemoryKvStore();
        var state2 = new FilePersistentState(dir);
        var log2 = new FileRaftLog(dir);
        _cluster.Harness.ReplaceNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat, restored, state2, log2);
        Assert.Contains("n4", _cluster.Harness.Node("n1").LearnerPeerIds);
        Assert.Equal(["n2", "n3"], _cluster.Harness.Node("n1").VoterPeerIds.OrderBy(id => id).ToArray());
    }

    public void Dispose()
    {
        _cluster.Dispose();
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private void Elect()
    {
        _cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        _cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        _cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        _cluster.Harness.Advance(80);
        Assert.Equal(Role.Leader, _cluster.Harness.Node("n1").Role);
    }
}

public sealed class JointConsensusTests : IDisposable
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);
    private readonly KvClusterTests.KvFixture _cluster = new(Heartbeat);
    private readonly string _root = Directory.CreateTempSubdirectory("synkrolyn-joint-").FullName;

    [Fact]
    public void EnterJoint_leaderUsesJointQuorumBeforeCommit()
    {
        Elect();
        _cluster.Clients["n1"].Put("seed", "1");
        _cluster.Harness.DrainAll();
        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        Assert.NotNull(_cluster.Harness.Node("n1").EnterJoint(["n1", "n2", "n4"]));
        long jointIndex = LatestJointIndex("n1");
        Assert.True(jointIndex > 0);
        Assert.True(_cluster.Harness.Node("n1").CommitIndex < jointIndex);
        Assert.True(_cluster.Harness.Node("n1").InJointConsensus);
        Assert.Equal(["n1", "n2", "n3"], _cluster.Harness.Node("n1").ColdVoterIds.OrderBy(id => id).ToArray());
        Assert.Equal(["n1", "n2", "n4"], _cluster.Harness.Node("n1").CnewVoterIds.OrderBy(id => id).ToArray());

        _cluster.Harness.Isolate("n2");
        _cluster.Harness.Isolate("n4");
        _cluster.Clients["n1"].Put("joint", "blocked");
        _cluster.Harness.DrainAll();
        Assert.Null(_cluster.Stores["n1"].Get("joint"));
        Assert.True(_cluster.Harness.Node("n1").CommitIndex < jointIndex);
    }

    [Fact]
    public void FollowerAppendsJoint_inJointBeforeFollowerCommit()
    {
        Elect();
        _cluster.Clients["n1"].Put("seed", "1");
        _cluster.Harness.DrainAll();
        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        _cluster.Harness.Isolate("n2");
        _cluster.Harness.Isolate("n4");
        Assert.NotNull(_cluster.Harness.Node("n1").EnterJoint(["n1", "n2", "n4"]));
        _cluster.Harness.DrainAll();
        long jointIndex = LatestJointIndex("n3");
        Assert.True(jointIndex > 0);
        Assert.True(_cluster.Harness.Node("n3").InJointConsensus);
        Assert.True(_cluster.Harness.Node("n3").CommitIndex < jointIndex);
    }

    [Fact]
    public void HoldJointFalse_autoAppendsCnewAfterJointCommits()
    {
        Elect();
        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        Assert.NotNull(_cluster.Harness.Node("n1").EnterJoint(["n1", "n2", "n4"]));
        _cluster.Harness.DrainAll();
        _cluster.Harness.Advance(20);
        Assert.True(LogHasCnew("n1"));
        Assert.False(_cluster.Harness.Node("n1").InJointConsensus);
        Assert.Equal(["n2", "n4"], _cluster.Harness.Node("n1").VoterPeerIds.OrderBy(id => id).ToArray());
        Assert.Null(_cluster.Harness.Node("n1").LeaveJoint());

        _cluster.Harness.Isolate("n3");
        _cluster.Clients["n1"].Put("after", "cnew");
        _cluster.Harness.DrainAll();
        Assert.Equal("cnew", _cluster.Stores["n1"].Get("after"));
        Assert.Equal("cnew", _cluster.Stores["n4"].Get("after"));
    }

    [Fact]
    public void HoldJointTrue_staysJointUntilLeaveJoint()
    {
        Elect();
        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        _cluster.Harness.Node("n1").SetHoldJoint(true);
        Assert.NotNull(_cluster.Harness.Node("n1").EnterJoint(["n1", "n2", "n4"]));
        _cluster.Harness.DrainAll();
        _cluster.Harness.Advance(20);
        Assert.True(_cluster.Harness.Node("n1").InJointConsensus);
        Assert.False(LogHasCnew("n1"));
        Assert.NotNull(_cluster.Harness.Node("n1").LeaveJoint());
        _cluster.Harness.DrainAll();
        _cluster.Harness.Advance(20);
        Assert.False(_cluster.Harness.Node("n1").InJointConsensus);
        Assert.Equal(["n2", "n4"], _cluster.Harness.Node("n1").VoterPeerIds.OrderBy(id => id).ToArray());
    }

    [Fact]
    public void TruncateUncommittedJoint_revertsToColdQuorum()
    {
        Elect();
        _cluster.Clients["n1"].Put("seed", "1");
        _cluster.Harness.DrainAll();
        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        _cluster.Harness.Isolate("n2");
        _cluster.Harness.Isolate("n3");
        _cluster.Harness.Isolate("n4");
        Assert.NotNull(_cluster.Harness.Node("n1").EnterJoint(["n1", "n2", "n4"]));
        Assert.True(_cluster.Harness.Node("n1").InJointConsensus);
        long jointIndex = LatestJointIndex("n1");
        Assert.True(_cluster.Harness.Node("n1").CommitIndex < jointIndex);

        _cluster.Harness.Isolate("n1");
        _cluster.Harness.Heal("n2");
        _cluster.Harness.Heal("n3");
        _cluster.Harness.StartElections("n2");
        Assert.Equal(Role.Leader, _cluster.Harness.Node("n2").Role);
        _cluster.Clients["n2"] = new KvClient(_cluster.Harness.Node("n2"), _cluster.Stores["n2"]);
        _cluster.Clients["n2"].Put("fresh", "ok");
        _cluster.Harness.DrainAll();
        _cluster.Harness.Advance(20);

        _cluster.Harness.Heal("n1");
        _cluster.Harness.Advance(40);
        Assert.False(_cluster.Harness.Node("n1").InJointConsensus);
        long commit = _cluster.Harness.Node("n2").CommitIndex;
        Assert.Equal(
            _cluster.Harness.Log("n2").Read(commit).Command,
            _cluster.Harness.Log("n1").Read(commit).Command);

        _cluster.Harness.Isolate("n4");
        _cluster.Clients["n2"].Put("cold", "again");
        _cluster.Harness.DrainAll();
        Assert.Equal("again", _cluster.Stores["n2"].Get("cold"));
    }

    [Fact]
    public void InstallSnapshot_midJointOntoBlankNode_restoresColdAndCnew()
    {
        Elect();
        _cluster.Clients["n1"].Put("seed", "1");
        _cluster.Harness.DrainAll();
        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        _cluster.Harness.Node("n1").SetHoldJoint(true);
        Assert.NotNull(_cluster.Harness.Node("n1").EnterJoint(["n1", "n2", "n4"]));
        _cluster.Harness.DrainAll();
        _cluster.Harness.Advance(20);
        Assert.True(_cluster.Harness.Node("n1").InJointConsensus);
        _cluster.Harness.Node("n1").Snapshot();
        _cluster.Harness.DrainAll();
        var blank = new InMemoryKvStore();
        _cluster.Stores["n3"] = blank;
        var blankNode = _cluster.Harness.ReplaceNode(
            "n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400), Heartbeat, blank, new InMemoryPersistentState(), new InMemoryRaftLog());
        _cluster.Clients["n3"] = new KvClient(blankNode, blank);
        Assert.Equal(0, _cluster.Harness.Log("n3").LastIncludedIndex);
        Assert.False(_cluster.Harness.Node("n3").InJointConsensus);
        _cluster.Harness.Advance(40);
        Assert.True(_cluster.Harness.Node("n3").SnapshotsInstalled >= 1);
        Assert.True(_cluster.Harness.Node("n3").InJointConsensus);
        Assert.Equal(["n1", "n2", "n3"], _cluster.Harness.Node("n3").ColdVoterIds.OrderBy(id => id).ToArray());
        Assert.Equal(["n1", "n2", "n4"], _cluster.Harness.Node("n3").CnewVoterIds.OrderBy(id => id).ToArray());
    }

    [Fact]
    public void HoldJoint_snapshotCloseReopen_restoresColdAndCnew()
    {
        string dir = Path.Combine(_root, "n1");
        var store = new InMemoryKvStore();
        var state = new FilePersistentState(dir);
        var log = new FileRaftLog(dir);
        _cluster.Harness.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat, store, state, log);
        _cluster.Stores["n1"] = store;
        _cluster.Clients["n1"] = new KvClient(_cluster.Harness.Node("n1"), store);
        _cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        _cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        _cluster.Harness.Advance(80);
        _cluster.Add("n4", ["n1", "n2", "n3"], TimeSpan.FromMilliseconds(10_000));
        _cluster.Harness.Node("n1").SetHoldJoint(true);
        Assert.NotNull(_cluster.Harness.Node("n1").EnterJoint(["n1", "n2", "n4"]));
        _cluster.Harness.DrainAll();
        _cluster.Harness.Advance(20);
        Assert.True(_cluster.Harness.Node("n1").InJointConsensus);
        _cluster.Harness.Node("n1").Snapshot();

        var restored = new InMemoryKvStore();
        var state2 = new FilePersistentState(dir);
        var log2 = new FileRaftLog(dir);
        _cluster.Harness.ReplaceNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80), Heartbeat, restored, state2, log2);
        Assert.True(_cluster.Harness.Node("n1").InJointConsensus);
        Assert.Equal(["n1", "n2", "n3"], _cluster.Harness.Node("n1").ColdVoterIds.OrderBy(id => id).ToArray());
        Assert.Equal(["n1", "n2", "n4"], _cluster.Harness.Node("n1").CnewVoterIds.OrderBy(id => id).ToArray());
        MembershipSnapshot.Decoded config = MembershipSnapshot.Decode(state2.MembershipBlob());
        Assert.True(config.InJoint);
    }

    public void Dispose()
    {
        _cluster.Dispose();
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private void Elect()
    {
        _cluster.Add("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(80));
        _cluster.Add("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(400));
        _cluster.Add("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(400));
        _cluster.Harness.Advance(80);
        Assert.Equal(Role.Leader, _cluster.Harness.Node("n1").Role);
    }

    private long LatestJointIndex(string id)
    {
        IRaftLog log = _cluster.Harness.Log(id);
        long found = 0;
        for (long i = log.FirstIndex; i <= log.LastIndex; i++)
        {
            if (i <= log.LastIncludedIndex)
            {
                continue;
            }

            byte[] command = log.Read(i).Command;
            if (MembershipCodec.IsMembership(command) && MembershipCodec.Decode(command).Kind == MembershipCodec.Kind.Joint)
            {
                found = i;
            }
        }

        return found;
    }

    private bool LogHasCnew(string id)
    {
        IRaftLog log = _cluster.Harness.Log(id);
        for (long i = log.FirstIndex; i <= log.LastIndex; i++)
        {
            if (i <= log.LastIncludedIndex)
            {
                continue;
            }

            byte[] command = log.Read(i).Command;
            if (MembershipCodec.IsMembership(command) && MembershipCodec.Decode(command).Kind == MembershipCodec.Kind.Cnew)
            {
                return true;
            }
        }

        return false;
    }
}
