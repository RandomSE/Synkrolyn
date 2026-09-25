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

    [Fact]
    public void RecordsElectionWindowTrials_doesNotGateMedian()
    {
        var windows = new List<long>();
        for (int trial = 0; trial < 3; trial++)
        {
            windows.Add(RunOneWindow());
        }

        Assert.Equal(3, windows.Count);
        Assert.All(windows, ms => Assert.True(ms >= 0));
    }

    private static long RunOneWindow()
    {
        using var cluster = SocketCluster.Start(
            ["n1", "n2", "n3"],
            [TimeSpan.FromMilliseconds(80), TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(400)],
            TimeSpan.FromMilliseconds(40));
        WaitUntil("leader", () => cluster.Leaders().Count == 1);
        SocketNode oldLeader = cluster.Leaders()[0];
        oldLeader.Client.Put("before", "yes");
        WaitUntil("prior", () => oldLeader.Client.AwaitCommitted() is not null);
        long t0 = Stopwatch.GetTimestamp();
        foreach (SocketNode node in cluster.Nodes)
        {
            if (node.Id != oldLeader.Id)
            {
                node.Transport.DisconnectPeer(oldLeader.Id);
            }
        }

        oldLeader.Transport.DisconnectAll();
        SocketNode? winner = null;
        string? proposedOn = null;
        WaitUntil("successor commits", () =>
        {
            winner = cluster.Nodes.FirstOrDefault(node => node.Id != oldLeader.Id && node.Node.Role == Role.Leader);
            if (winner is null)
            {
                return false;
            }

            if (proposedOn != winner.Id)
            {
                winner.Client.Put("after", "ok");
                proposedOn = winner.Id;
            }

            return winner.Client.AwaitCommitted() is not null;
        });
        return (long)Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
    }

    private static void WaitUntil(string message, Func<bool> condition)
    {
        var started = Stopwatch.StartNew();
        while (!condition())
        {
            if (started.Elapsed > TimeSpan.FromSeconds(12))
            {
                throw new TimeoutException("timed out: " + message);
            }

            Thread.Yield();
        }
    }
}

internal sealed class SocketCluster : IDisposable
{
    private SocketCluster(IReadOnlyList<SocketNode> nodes) => Nodes = nodes;

    public IReadOnlyList<SocketNode> Nodes { get; }

    public List<SocketNode> Leaders() => Nodes.Where(node => node.Node.Role == Role.Leader).ToList();

    public static SocketCluster Start() =>
        Start(["n1", "n2", "n3"], [TimeSpan.FromMilliseconds(120), TimeSpan.FromMilliseconds(350), TimeSpan.FromMilliseconds(600)], TimeSpan.FromMilliseconds(40));

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
