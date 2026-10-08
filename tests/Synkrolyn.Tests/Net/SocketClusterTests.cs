using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Net;

public sealed class SocketClusterTests
{
    [Fact]
    public void ThreeNodeTcp_electsExactlyOneLeader_andPutReplicates()
    {
        using var cluster = SocketCluster.Start();
        WaitUntil("exactly one leader", () => cluster.Leaders().Count == 1);
        Assert.Single(cluster.Leaders());
        SocketNode leader = cluster.Leaders()[0];
        leader.Client.Put("city", "athens");
        WaitUntil("put committed", () => leader.Client.AwaitCommitted() is not null);
        long index = leader.Client.AwaitCommitted()!.Value;
        SocketNode follower = cluster.Nodes.First(node => node.Id != leader.Id);
        WaitUntil("follower applied", () => follower.Node.LastApplied >= index);
        Assert.Equal("athens", follower.Client.Get("city"));
        Assert.Null(leader.Runtime.Uncaught);
    }

    [Fact]
    public void ConnectFailuresAndReset_originalPutStillCommits()
    {
        using var cluster = SocketCluster.Start(
            ["n1", "n2", "n3"],
            SocketCluster.ElectionTimeouts,
            TimeSpan.FromMilliseconds(40));
        WaitUntil("exactly one leader", () => cluster.Leaders().Count == 1);
        Assert.Single(cluster.Leaders());
        foreach (SocketNode node in cluster.Nodes)
        {
            node.Transport.FailNextConnects(2);
            node.Transport.DropInbound();
        }

        SocketNode leader = cluster.Leaders()[0];
        leader.Client.Put("city", "athens");
        WaitUntil("put committed", () => leader.Client.AwaitCommitted() is not null);
        long index = leader.Client.AwaitCommitted()!.Value;
        SocketNode follower = cluster.Nodes.First(node => node.Id != leader.Id);
        WaitUntil("follower applied", () => follower.Node.LastApplied >= index);
        Assert.Equal("athens", follower.Client.Get("city"));
        Assert.All(cluster.Nodes, node => Assert.Null(node.Runtime.Uncaught));
    }

    [Fact]
    public void CloseFollowerSockets_leaderStillCommits_thenReconnect()
    {
        using var cluster = SocketCluster.Start();
        WaitUntil("exactly one leader", () => cluster.Leaders().Count == 1);
        SocketNode leader = cluster.Leaders()[0];
        SocketNode follower = cluster.Nodes.First(node => node.Id != leader.Id);
        SocketNode other = cluster.Nodes.First(node => node.Id != leader.Id && node.Id != follower.Id);
        leader.Transport.DisconnectPeer(follower.Id);
        other.Transport.DisconnectPeer(follower.Id);
        follower.Transport.DisconnectAll();
        leader.Client.Put("k", "v");
        WaitUntil("put committed", () => leader.Client.AwaitCommitted() is not null);
        long index = leader.Client.AwaitCommitted()!.Value;
        WaitUntil("majority applied", () => other.Node.LastApplied >= index);
        Assert.Equal("v", other.Client.Get("k"));
        leader.Transport.ReconnectPeer(follower.Id);
        other.Transport.ReconnectPeer(follower.Id);
        follower.Transport.ReconnectAll();
        WaitUntil("follower catch-up", () => follower.Node.LastApplied >= index);
        Assert.Equal("v", follower.Client.Get("k"));
    }

    [Fact]
    public void TruncatedPeerFrame_doesNotThrowOnRaftThread()
    {
        using var cluster = SocketCluster.Start();
        WaitUntil("exactly one leader", () => cluster.Leaders().Count == 1);
        SocketNode target = cluster.Nodes[0];
        using var raw = new TcpClient();
        raw.Connect(IPAddress.Loopback, target.Transport.LocalPort);
        raw.GetStream().Write([0, 0, 0, 20, 1, 2, 3]);
        WaitUntil("still a leader", () => cluster.Leaders().Count == 1);
        Assert.All(cluster.Nodes, node => Assert.Null(node.Runtime.Uncaught));
    }

    private static readonly ManualResetEventSlim Park = new(false);

    private static void WaitUntil(string message, Func<bool> condition)
    {
        var started = Stopwatch.StartNew();
        while (!condition())
        {
            if (started.Elapsed > TimeSpan.FromSeconds(12))
            {
                throw new TimeoutException("timed out: " + message);
            }

            // Yield spins a core. On a 2-vCPU runner that keeps the Raft thread off
            // the CPU until CheckQuorum has already stepped the leader down.
            Park.Wait(TimeSpan.FromMilliseconds(1));
        }
    }
}

internal sealed class SocketCluster : IDisposable
{
    private SocketCluster(IReadOnlyList<SocketNode> nodes) => Nodes = nodes;

    public IReadOnlyList<SocketNode> Nodes { get; }

    public List<SocketNode> Leaders() => Nodes.Where(node => node.Node.Role == Role.Leader).ToList();

    /// <summary>Election timeouts for the three-node socket fixture. Host ships 150 ms.</summary>
    public static readonly TimeSpan[] ElectionTimeouts = [TimeSpan.FromMilliseconds(120), TimeSpan.FromMilliseconds(350), TimeSpan.FromMilliseconds(600)];

    public static SocketCluster Start() =>
        Start(["n1", "n2", "n3"], ElectionTimeouts, TimeSpan.FromMilliseconds(40));

    public static SocketCluster Start(string[] ids, TimeSpan[] elections, TimeSpan heartbeat)
    {
        var transports = new List<SocketTransport>();
        foreach (string id in ids)
        {
            var transport = new SocketTransport(id, new RpcWireCodec());
            transport.Bind();
            transports.Add(transport);
        }

        for (int i = 0; i < ids.Length; i++)
        {
            for (int j = 0; j < ids.Length; j++)
            {
                if (i != j)
                {
                    transports[i].SetPeer(ids[j], new IPEndPoint(IPAddress.Loopback, transports[j].LocalPort));
                }
            }
        }

        var nodes = new List<SocketNode>();
        for (int i = 0; i < ids.Length; i++)
        {
            string id = ids[i];
            string[] peers = ids.Where(other => other != id).ToArray();
            var clock = new SystemRaftClock();
            var store = new InMemoryKvStore();
            var node = new RaftNode(id, peers, clock, transports[i], new InMemoryPersistentState(), new InMemoryRaftLog(), elections[i], heartbeat, store);
            RaftRuntime runtime = RaftRuntime.ForTests(node, transports[i], clock);
            runtime.Start();
            nodes.Add(new SocketNode(id, transports[i], node, new KvClient(node, store), runtime));
        }

        return new SocketCluster(nodes);
    }

    public void Dispose()
    {
        foreach (SocketNode node in Nodes)
        {
            node.Runtime.Dispose();
        }
    }
}

internal sealed record SocketNode(string Id, SocketTransport Transport, RaftNode Node, KvClient Client, RaftRuntime Runtime);
