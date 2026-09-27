using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Net;

public class SocketDeliveryTests
{
    [Fact]
    public void FailedConnect_retriesTheSameFrame()
    {
        var received = new ConcurrentQueue<Envelope>();
        using var server = new SocketTransport("b", new RpcWireCodec());
        server.Bind();
        server.SetHandler(received.Enqueue);
        server.Start();
        using var client = new SocketTransport("a", new RpcWireCodec());
        client.Bind();
        client.Start();
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, server.LocalPort));
        client.FailNextConnects(3);
        client.Send("a", "b", new RequestVote(1, "a", 0, 0));
        Assert.True(WaitFor(() => !received.IsEmpty));
        Assert.True(received.TryDequeue(out Envelope? envelope));
        var vote = Assert.IsType<RequestVote>(envelope.Payload);
        Assert.Equal(1, vote.Term);
        Assert.Equal("a", vote.CandidateId);
    }

    [Fact]
    public void PeerStartsAfterFirstSend_deliversTheQueuedFrame()
    {
        var received = new ConcurrentQueue<Envelope>();
        using var server = new SocketTransport("b", new RpcWireCodec());
        server.Bind();
        server.SetHandler(received.Enqueue);
        server.Start();
        using var closed = new TcpListener(IPAddress.Loopback, 0);
        closed.Start();
        int closedPort = ((IPEndPoint)closed.LocalEndpoint).Port;
        closed.Stop();
        using var client = new SocketTransport("a", new RpcWireCodec());
        client.Bind();
        client.Start();
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, closedPort));
        client.Send("a", "b", new RequestVote(4, "a", 3, 3));
        new ManualResetEventSlim(false).Wait(50);
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, server.LocalPort));
        Assert.True(WaitFor(() => received.Any(envelope => envelope.Payload is RequestVote vote && vote.Term == 4)));
    }

    [Fact]
    public void ResetInbound_laterFrameStillArrives()
    {
        var received = new ConcurrentQueue<Envelope>();
        using var server = new SocketTransport("b", new RpcWireCodec());
        server.Bind();
        server.SetHandler(received.Enqueue);
        server.Start();
        using var client = new SocketTransport("a", new RpcWireCodec());
        client.Bind();
        client.Start();
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, server.LocalPort));
        client.Send("a", "b", new RequestVote(1, "a", 0, 0));
        Assert.True(WaitFor(() => !received.IsEmpty));
        server.DropInbound();
        client.Send("a", "b", new RequestVote(2, "a", 1, 1));
        Assert.True(WaitFor(() => received.Any(envelope => envelope.Payload is RequestVote vote && vote.Term == 2)));
    }

    private static bool WaitFor(Func<bool> condition)
    {
        var started = System.Diagnostics.Stopwatch.StartNew();
        while (!condition())
        {
            if (started.Elapsed > TimeSpan.FromSeconds(2))
            {
                return false;
            }

            new ManualResetEventSlim(false).Wait(5);
        }

        return true;
    }
}
