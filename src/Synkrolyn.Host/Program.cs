using System.Net;
using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
};

if (args.Contains("--help") || args.Contains("-h"))
{
    Console.WriteLine("Synkrolyn.Host");
    Console.WriteLine("  Starts a localhost 3-node Raft KV cluster, puts a key, and reads it back.");
    Console.WriteLine("  Usage: dotnet run --project src/Synkrolyn.Host");
    return 0;
}

using var runtime = await StartClusterAsync(cts.Token).ConfigureAwait(false);
string? value = runtime.Get("hello");
Console.WriteLine("hello=" + (value ?? "<missing>"));
return value == "world" ? 0 : 1;

static async Task<HostCluster> StartClusterAsync(CancellationToken cancellationToken)
{
    var clock = new SystemRaftClock();
    var nodes = new List<(SocketTransport Transport, RaftNode Node, InMemoryKvStore Store, KvClient Client, RaftRuntime Runtime)>();
    string[] ids = ["n1", "n2", "n3"];
    foreach (string id in ids)
    {
        var codec = new RpcWireCodec();
        var transport = new SocketTransport(id, codec);
        transport.Bind();
        var store = new InMemoryKvStore();
        var peers = ids.Where(other => other != id).ToArray();
        var node = new RaftNode(
            id,
            peers,
            clock,
            transport,
            new InMemoryPersistentState(),
            new InMemoryRaftLog(),
            TimeSpan.FromMilliseconds(150),
            TimeSpan.FromMilliseconds(40),
            store);
        var client = new KvClient(node, store, id);
        var runtime = RaftRuntime.ForProduction(node, transport, clock);
        nodes.Add((transport, node, store, client, runtime));
    }

    foreach (var self in nodes)
    {
        foreach (var peer in nodes)
        {
            if (peer.Node.NodeId != self.Node.NodeId)
            {
                self.Transport.SetPeer(peer.Node.NodeId, new IPEndPoint(IPAddress.Loopback, peer.Transport.LocalPort));
            }
        }
    }

    foreach (var node in nodes)
    {
        node.Runtime.Start();
    }

    var started = System.Diagnostics.Stopwatch.StartNew();
    KvClient? leader = null;
    while (started.Elapsed < TimeSpan.FromSeconds(8) && leader is null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        leader = nodes.Where(n => n.Node.Role == Role.Leader).Select(n => n.Client).FirstOrDefault();
        if (leader is null)
        {
            await Task.Delay(20, cancellationToken).ConfigureAwait(false);
        }
    }

    if (leader is null)
    {
        throw new InvalidOperationException("cluster did not elect a leader");
    }

    leader.Put("hello", "world");

    while (started.Elapsed < TimeSpan.FromSeconds(8) && leader.AwaitCommitted() is null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Task.Delay(20, cancellationToken).ConfigureAwait(false);
    }

    return new HostCluster(nodes, leader);
}

internal sealed class HostCluster : IDisposable
{
    private readonly List<(SocketTransport Transport, RaftNode Node, InMemoryKvStore Store, KvClient Client, RaftRuntime Runtime)> _nodes;
    private readonly KvClient _leader;

    public HostCluster(
        List<(SocketTransport Transport, RaftNode Node, InMemoryKvStore Store, KvClient Client, RaftRuntime Runtime)> nodes,
        KvClient leader)
    {
        _nodes = nodes;
        _leader = leader;
    }

    public string? Get(string key) => _leader.Get(key);

    public void Dispose()
    {
        foreach (var node in _nodes)
        {
            node.Runtime.Dispose();
        }
    }
}
