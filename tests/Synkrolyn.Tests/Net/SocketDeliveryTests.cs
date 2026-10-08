using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
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
    public void ResetInbound_oneFrameArrivesWithoutALaterFrame()
    {
        var received = new ConcurrentQueue<Envelope>();
        using var entered = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        using var server = new SocketTransport("b", new RpcWireCodec());
        server.Bind();
        server.SetHandler(envelope =>
        {
            received.Enqueue(envelope);
            if (envelope.Payload is RequestVote vote && vote.Term == 1)
            {
                entered.Set();
                release.Wait();
            }
        });
        server.Start();
        using var client = new SocketTransport("a", new RpcWireCodec());
        client.Bind();
        client.Start();
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, server.LocalPort));
        try
        {
            client.Send("a", "b", new RequestVote(1, "a", 0, 0));
            Assert.True(entered.Wait(TimeSpan.FromSeconds(2)));
            client.Send("a", "b", new RequestVote(2, "a", 1, 1));
            Assert.True(WaitFor(() => InboundUnread(server) > 0), "frame was not sitting unread in the accept socket");
            server.DropInbound();
            Assert.True(
                WaitFor(() => received.Any(envelope => envelope.Payload is RequestVote vote && vote.Term == 2)),
                "the one frame written before the reset never arrived, and nothing was sent after it");
        }
        finally
        {
            release.Set();
        }
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

    [Fact]
    public void SlowReader_receivesALargeFrame()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var client = new SocketTransport("a", new RpcWireCodec());
        client.Bind();
        client.Start();
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port));
        byte[] command = new byte[64 * 1024];
        command[0] = 9;
        var started = System.Diagnostics.Stopwatch.StartNew();
        client.Send("a", "b", new AppendEntries(1, "a", 0, 0, [new LogEntry(1, 1, command)], 0));

        // Read on this thread so a paused background reader cannot eat the 3 s
        // budget. The listener accepts once; a writer that drops a live socket
        // and connects again leaves a second connection or a different port.
        int waitUs = (int)Remaining(started).TotalMilliseconds * 1000;
        Assert.True(listener.Server.Poll(waitUs, SelectMode.SelectRead));
        using TcpClient accepted = listener.AcceptTcpClient();
        Assert.False(listener.Pending());
        // The handshake can complete before the writer publishes the socket.
        Assert.True(WaitFor(() => OutboundIsBlocking(client), Remaining(started)));
        int remainMs = Math.Max(1, (int)Remaining(started).TotalMilliseconds);
        accepted.ReceiveTimeout = remainMs;
        var decoder = new FrameCodec.Decoder();
        byte[] buf = new byte[8192];
        byte[]? payload = null;
        NetworkStream stream = accepted.GetStream();
        while (payload is null && started.Elapsed < TimeSpan.FromSeconds(3))
        {
            int n;
            try
            {
                n = stream.Read(buf, 0, buf.Length);
            }
            catch (IOException)
            {
                continue;
            }

            if (n <= 0)
            {
                break;
            }

            foreach (byte[] frame in decoder.Push(buf.AsSpan(0, n).ToArray()))
            {
                payload = frame;
            }
        }

        Assert.False(listener.Pending());
        Assert.Equal(((IPEndPoint)accepted.Client.RemoteEndPoint!).Port, OutboundLocalPort(client));
        Assert.NotNull(payload);
        IMessageCodec.Decoded decoded = new RpcWireCodec().Decode(payload);
        var append = Assert.IsType<AppendEntries>(decoded.Payload);
        Assert.Equal(9, append.Entries[0].Command[0]);
        Assert.Equal(command.Length, append.Entries[0].Command.Length);
    }

    private static int OutboundLocalPort(SocketTransport transport)
    {
        TcpClient? client = Outbound(transport);
        return client is null ? -1 : ((IPEndPoint)client.Client.LocalEndPoint!).Port;
    }

    private static bool OutboundIsBlocking(SocketTransport transport)
    {
        TcpClient? client = Outbound(transport);
        if (client is null)
        {
            return false;
        }

        try
        {
            return client.Client.Blocking;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }

    private static TcpClient? Outbound(SocketTransport transport)
    {
        var outbound = (Dictionary<string, TcpClient>)typeof(SocketTransport)
            .GetField("_outbound", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(transport)!;
        try
        {
            return outbound.TryGetValue("b", out TcpClient? client) ? client : null;
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }

    private static int InboundUnread(SocketTransport transport)
    {
        FieldInfo field = typeof(SocketTransport).GetField("_inbound", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var clients = (List<TcpClient>)field.GetValue(transport)!;
        TcpClient[] copy = clients.ToArray();
        int unread = 0;
        foreach (TcpClient client in copy)
        {
            try
            {
                unread += client.Available;
            }
            catch (ObjectDisposedException)
            {
                // Dropped while the waiter was sampling.
            }
        }

        return unread;
    }

    private static TimeSpan Remaining(System.Diagnostics.Stopwatch started)
    {
        TimeSpan left = TimeSpan.FromSeconds(3) - started.Elapsed;
        return left > TimeSpan.Zero ? left : TimeSpan.Zero;
    }

    private static bool WaitFor(Func<bool> condition) => WaitFor(condition, TimeSpan.FromSeconds(2));

    private static bool WaitFor(Func<bool> condition, TimeSpan budget)
    {
        var started = System.Diagnostics.Stopwatch.StartNew();
        while (!condition())
        {
            if (started.Elapsed > budget)
            {
                return false;
            }

            new ManualResetEventSlim(false).Wait(5);
        }

        return true;
    }
}
