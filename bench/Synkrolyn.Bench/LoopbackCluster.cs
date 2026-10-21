using System.Diagnostics;
using System.Net;
using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Bench;

/// <summary>Three loopback nodes in this process. In-memory log and KV. Identity election jitter.</summary>
internal sealed class LoopbackCluster : IDisposable
{
    public static readonly TimeSpan ElectionTimeout = TimeSpan.FromMilliseconds(150);

    public static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(40);

    private readonly List<Node> _nodes = [];

    private bool _disposed;

    private LoopbackCluster()
    {
    }

    public static LoopbackCluster Start()
    {
        var cluster = new LoopbackCluster();
        var clock = new SystemRaftClock();
        string[] ids = ["n1", "n2", "n3"];
        try
        {
            foreach (string id in ids)
            {
                var transport = new SocketTransport(id, new RpcWireCodec());
                transport.Bind();
                var store = new InMemoryKvStore();
                string[] peers = ids.Where(other => other != id).ToArray();
                var raft = new RaftNode(
                    id,
                    peers,
                    clock,
                    transport,
                    new InMemoryPersistentState(),
                    new InMemoryRaftLog(),
                    ElectionTimeout,
                    Heartbeat,
                    store);
                var runtime = RaftRuntime.ForTests(raft, transport, clock);
                cluster._nodes.Add(new Node(id, transport, raft, store, runtime));
            }

            foreach (Node self in cluster._nodes)
            {
                foreach (Node peer in cluster._nodes)
                {
                    if (peer.Id != self.Id)
                    {
                        self.Transport.SetPeer(peer.Id, new IPEndPoint(IPAddress.Loopback, peer.Transport.LocalPort));
                    }
                }
            }

            foreach (Node node in cluster._nodes)
            {
                node.Runtime.Start();
            }

            return cluster;
        }
        catch
        {
            cluster.Dispose();
            throw;
        }
    }

    public Node WaitForLeader(TimeSpan timeout)
    {
        var wait = Stopwatch.StartNew();
        while (wait.Elapsed < timeout)
        {
            ThrowIfFaulted();
            foreach (Node node in _nodes)
            {
                if (node.Raft.Role == Role.Leader)
                {
                    return node;
                }
            }

            Thread.Yield();
        }

        throw new TimeoutException("no leader within " + timeout);
    }

    public void Stop(Node node)
    {
        if (!_nodes.Remove(node))
        {
            throw new InvalidOperationException("node " + node.Id + " is not in the cluster");
        }

        node.Runtime.Dispose();
    }

    public void ThrowIfFaulted()
    {
        foreach (Node node in _nodes)
        {
            if (node.Runtime.Uncaught is not null)
            {
                throw new InvalidOperationException("raft thread fault on " + node.Id, node.Runtime.Uncaught);
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (Node node in _nodes.ToArray())
        {
            node.Runtime.Dispose();
        }

        _nodes.Clear();
    }
}

internal sealed class Node
{
    public Node(string id, SocketTransport transport, RaftNode raft, InMemoryKvStore store, RaftRuntime runtime)
    {
        Id = id;
        Transport = transport;
        Raft = raft;
        Store = store;
        Runtime = runtime;
    }

    public string Id { get; }

    public SocketTransport Transport { get; }

    public RaftNode Raft { get; }

    public InMemoryKvStore Store { get; }

    public RaftRuntime Runtime { get; }
}
