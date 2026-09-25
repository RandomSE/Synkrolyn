using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

public class ReplicationAndPureImprovementTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);
    private readonly ClusterHarness _cluster = new();

    [Fact]
    public void ThreeNode_propose_commitsOnMajority_andThirdCatchesUp()
    {
        Elect();
        long? index = _cluster.Propose("n1", "a"u8.ToArray());
        Assert.NotNull(index);
        Assert.True(_cluster.Node("n1").CommitIndex >= index);
        Assert.Equal(_cluster.Log("n1").Read(index!.Value).Command, _cluster.Log("n2").Read(index.Value).Command);
        _cluster.Advance(20);
        Assert.Equal("a"u8.ToArray(), _cluster.Log("n3").Read(index.Value).Command);
    }

    [Fact]
    public void Figure8_oldTermMajority_doesNotCommitUntilCurrentTermEntry()
    {
        _cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100), Heartbeat);
        _cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(300), Heartbeat);
        _cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(300), Heartbeat);
        _cluster.Advance(100);
        Assert.Equal(Role.Leader, _cluster.Node("n1").Role);
        _cluster.Isolate("n1");
        _cluster.Advance(300);
        Assert.NotEqual(Role.Leader, _cluster.Node("n1").Role);
        RaftNode leader = _cluster.Leaders().Single();
        Assert.True(leader.CommitIndex >= 1);
        Assert.Equal(leader.CurrentTerm, _cluster.Log(leader.NodeId).Read(leader.CommitIndex).Term);
    }

    [Fact]
    public void Leader_doesNotCountItselfUntilForce()
    {
        _cluster.AddNode("solo", [], TimeSpan.FromMilliseconds(50), Heartbeat);
        _cluster.Advance(50);
        RaftNode solo = _cluster.Node("solo");
        long committed = solo.CommitIndex;
        long durable = _cluster.Log("solo").DurableIndex;
        long? index = solo.Propose("x"u8.ToArray());
        Assert.NotNull(index);
        Assert.Equal(committed, solo.CommitIndex);
        Assert.Equal(durable, _cluster.Log("solo").DurableIndex);
        Assert.True(_cluster.Log("solo").LastIndex >= index);
        solo.Drain();
        Assert.Equal(index, solo.CommitIndex);
        Assert.True(_cluster.Log("solo").DurableIndex >= index);
    }

    [Fact]
    public void Leader_sendsAppendEntriesBeforeItsOwnForce()
    {
        Elect();
        RaftNode leader = _cluster.Node("n1");
        long durable = _cluster.Log("n1").DurableIndex;
        long? index = leader.Propose("parallel"u8.ToArray());
        Assert.Equal(durable, _cluster.Log("n1").DurableIndex);
        _cluster.Node("n2").Drain();
        Assert.Equal(index, _cluster.Log("n2").LastIndex);
        Assert.Equal(durable, _cluster.Log("n1").DurableIndex);
        leader.Drain();
        Assert.True(_cluster.Log("n1").DurableIndex >= index);
    }

    [Fact]
    public void Transfer_reportsSuccessOnlyAfterTargetWins_andTimeoutAborts()
    {
        Elect();
        _cluster.Propose("n1", "catch-up"u8.ToArray());
        RaftNode leader = _cluster.Node("n1");
        Assert.True(leader.TransferLeadership("n2"));
        Assert.Equal(Role.Follower, leader.Role);
        Assert.Equal(LeadershipTransferStatus.AwaitingWinner, leader.TransferStatus);
        _cluster.DrainAll();
        Assert.Equal(Role.Leader, _cluster.Node("n2").Role);
        Assert.Equal(LeadershipTransferStatus.Succeeded, leader.TransferStatus);

        _cluster.Isolate("n3");
        Assert.True(_cluster.Node("n2").TransferLeadership("n3"));
        Assert.Equal(Role.Follower, _cluster.Node("n2").Role);
        Assert.Equal(LeadershipTransferStatus.AwaitingWinner, _cluster.Node("n2").TransferStatus);
        _cluster.Advance(300);
        Assert.Equal(LeadershipTransferStatus.Aborted, _cluster.Node("n2").TransferStatus);
        Assert.NotEqual(Role.Leader, _cluster.Node("n3").Role);
    }

    [Fact]
    public void ReorderedAppendEntries_commitIndexDoesNotDecrease()
    {
        Elect();
        _cluster.Propose("n1", "one"u8.ToArray());
        long commit = _cluster.Node("n2").CommitIndex;
        var stale = new AppendEntries(1, "n1", 0, 0, [], 0, 1);
        _cluster.Node("n2").Receive(new Synkrolyn.Net.Envelope("n1", "n2", stale));
        _cluster.Node("n2").Drain();
        Assert.True(_cluster.Node("n2").CommitIndex >= commit);
    }

    private void Elect()
    {
        _cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100), Heartbeat);
        _cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(300), Heartbeat);
        _cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(300), Heartbeat);
        _cluster.Advance(100);
        Assert.Equal(Role.Leader, _cluster.Node("n1").Role);
    }
}
