using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

public class RaftNodeElectionTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);
    private readonly ClusterHarness _cluster = new();

    [Fact]
    public void ThreeNode_shortestTimeoutWins_exactlyOneLeader()
    {
        _cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100), Heartbeat);
        _cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(300), Heartbeat);
        _cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(300), Heartbeat);
        _cluster.Advance(100);
        Assert.Equal(Role.Leader, _cluster.Node("n1").Role);
        Assert.Equal(Role.Follower, _cluster.Node("n2").Role);
        Assert.Equal(Role.Follower, _cluster.Node("n3").Role);
        Assert.Equal("n1", _cluster.Node("n2").LeaderId);
        Assert.Single(_cluster.Leaders());
    }

    [Fact]
    public void HeartbeatsPreventSecondElection()
    {
        _cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100), Heartbeat);
        _cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(300), Heartbeat);
        _cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(300), Heartbeat);
        _cluster.Advance(100);
        _cluster.Advance(200);
        Assert.Equal(Role.Leader, _cluster.Node("n1").Role);
        Assert.Single(_cluster.Leaders());
    }

    [Fact]
    public void SingleNode_electsItself()
    {
        _cluster.AddNode("solo", [], TimeSpan.FromMilliseconds(50), Heartbeat);
        _cluster.Advance(50);
        Assert.Equal(Role.Leader, _cluster.Node("solo").Role);
        Assert.Equal(1, _cluster.Node("solo").CurrentTerm);
    }

    [Fact]
    public void SplitVote_thenLaterTimeoutElectsOneLeader()
    {
        _cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100), Heartbeat);
        _cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(250), Heartbeat);
        _cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(250), Heartbeat);
        _cluster.StartElections("n1", "n2", "n3");
        Assert.Empty(_cluster.Leaders());
        Assert.Equal(1, _cluster.Node("n1").CurrentTerm);
        _cluster.Advance(100);
        Assert.Single(_cluster.Leaders());
        Assert.Equal(Role.Leader, _cluster.Node("n1").Role);
        Assert.True(_cluster.Node("n1").CurrentTerm > 1);
    }

    [Fact]
    public void PreVote_partitionedFollowerDoesNotBumpHealthyLeaderTerm()
    {
        _cluster.AddNode("n1", ["n2", "n3"], TimeSpan.FromMilliseconds(100), Heartbeat);
        _cluster.AddNode("n2", ["n1", "n3"], TimeSpan.FromMilliseconds(300), Heartbeat);
        _cluster.AddNode("n3", ["n1", "n2"], TimeSpan.FromMilliseconds(300), Heartbeat);
        _cluster.Advance(100);
        long term = _cluster.Node("n1").CurrentTerm;
        _cluster.Isolate("n3");
        _cluster.Advance(300);
        _cluster.Advance(300);
        Assert.Equal(Role.Leader, _cluster.Node("n1").Role);
        Assert.Equal(term, _cluster.Node("n1").CurrentTerm);
        Assert.True(_cluster.Node("n3").CurrentTerm <= term);
    }
}
