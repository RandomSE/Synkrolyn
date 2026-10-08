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
        var frames = new ConcurrentQueue<byte[]>();
        using var stop = new ManualResetEventSlim(false);
        var reader = new Thread(() =>
        {
            using TcpClient accepted = listener.AcceptTcpClient();
            accepted.ReceiveBufferSize = 1024;
            accepted.ReceiveTimeout = 1000;
            NetworkStream stream = accepted.GetStream();
            var decoder = new FrameCodec.Decoder();
            byte[] buf = new byte[8192];
            using var pause = new ManualResetEventSlim(false);
            int paced = 0;
            while (!stop.IsSet)
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
                    return;
                }

                foreach (byte[] payload in decoder.Push(buf.AsSpan(0, n).ToArray()))
                {
                    frames.Enqueue(payload);
                    stop.Set();
                }

                // A few pauses keep the sender blocked longer than the old 75 ms
                // write cap without stretching the whole frame out to seconds.
                if (paced < 4)
                {
                    paced++;
                    pause.Wait(20);
                }
            }
        });
        reader.IsBackground = true;
        reader.Start();
        using var client = new SocketTransport("a", new RpcWireCodec());
        client.Bind();
        client.Start();
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port));
        byte[] command = new byte[64 * 1024];
        command[0] = 9;
        client.Send("a", "b", new AppendEntries(1, "a", 0, 0, [new LogEntry(1, 1, command)], 0));
        bool arrived = WaitFor(() => !frames.IsEmpty, TimeSpan.FromSeconds(3));
        stop.Set();
        Assert.True(arrived);
        Assert.True(frames.TryDequeue(out byte[]? payload));
        IMessageCodec.Decoded decoded = new RpcWireCodec().Decode(payload);
        var append = Assert.IsType<AppendEntries>(decoded.Payload);
        Assert.Equal(9, append.Entries[0].Command[0]);
        Assert.Equal(command.Length, append.Entries[0].Command.Length);
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
