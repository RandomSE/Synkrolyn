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
        SetPeerDead(client, TimeSpan.FromSeconds(30));
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
        SetPeerDead(client, TimeSpan.FromSeconds(30));
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
        SetPeerDead(client, TimeSpan.FromSeconds(30));
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
        SetPeerDead(client, TimeSpan.FromSeconds(30));
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
        SetPeerDead(client, TimeSpan.FromSeconds(30));
        client.Bind();
        client.Start();
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port));
        byte[] command = new byte[64 * 1024];
        command[0] = 9;
        var started = System.Diagnostics.Stopwatch.StartNew();
        client.Send("a", "b", new AppendEntries(1, "a", 0, 0, [new LogEntry(1, 1, command)], 0));
        int waitUs = (int)Remaining(started).TotalMilliseconds * 1000;
        Assert.True(listener.Server.Poll(Math.Max(1, waitUs), SelectMode.SelectRead));
        using TcpClient accepted = listener.AcceptTcpClient();
        Assert.False(listener.Pending());
        accepted.ReceiveBufferSize = 1024;
        accepted.ReceiveTimeout = Math.Max(1, (int)Remaining(started).TotalMilliseconds);
        var decoder = new FrameCodec.Decoder();
        byte[] buf = new byte[8192];
        byte[]? payload = null;
        NetworkStream stream = accepted.GetStream();
        using var pause = new ManualResetEventSlim(false);
        int paced = 0;
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

            // A few pauses keep the sender blocked longer than the old 75 ms
            // write cap without stretching the whole frame out to seconds.
            if (paced < 4)
            {
                paced++;
                pause.Wait(20);
            }
        }

        Assert.False(listener.Pending());
        Assert.NotNull(payload);
        IMessageCodec.Decoded decoded = new RpcWireCodec().Decode(payload);
        var append = Assert.IsType<AppendEntries>(decoded.Payload);
        Assert.Equal(9, append.Entries[0].Command[0]);
        Assert.Equal(command.Length, append.Entries[0].Command.Length);
    }

    [Fact]
    public void GracefulClose_countsAsDead_andTheFrameIsSentAgain()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var client = new SocketTransport("a", new RpcWireCodec());
        SetPeerDead(client, TimeSpan.FromSeconds(30));
        client.Bind();
        client.Start();
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port));
        client.Send("a", "b", new RequestVote(1, "a", 0, 0));
        using TcpClient accepted = AcceptFrame(listener, TimeSpan.FromSeconds(2), out byte[]? first);
        Assert.NotNull(first);
        // FIN only. The socket can still take our bytes, so a write failure is
        // not what marks it dead. A zero-byte peek with no socket error is a close.
        accepted.Client.Shutdown(SocketShutdown.Send);
        using TcpClient again = AcceptFrame(listener, TimeSpan.FromSeconds(2), out byte[]? resent);
        Assert.NotNull(resent);
        var vote = Assert.IsType<RequestVote>(new RpcWireCodec().Decode(resent).Payload);
        Assert.Equal(1, vote.Term);
    }

    [Fact]
    public void LivenessProbe_doesNotHoldTheTransportLock()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var client = new SocketTransport("a", new RpcWireCodec());
        SetPeerDead(client, TimeSpan.FromSeconds(30));
        client.Bind();
        client.Start();
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port));
        client.Send("a", "b", new RequestVote(1, "a", 0, 0));
        Assert.True(listener.Server.Poll(2_000_000, SelectMode.SelectRead));
        using TcpClient accepted = listener.AcceptTcpClient();
        Assert.NotNull(ReadOneFrame(accepted, TimeSpan.FromSeconds(2)).Frame);
        int entered = SocketTransport.ProbeEntered;
        using var stall = new ManualResetEventSlim(false);
        using var released = new ManualResetEventSlim(false);
        SocketTransport.ProbeStall = stall;
        var setter = new Thread(() =>
        {
            client.SetPeer("c", new IPEndPoint(IPAddress.Loopback, 1));
            released.Set();
        });
        setter.IsBackground = true;
        try
        {
            client.Send("a", "b", new RequestVote(2, "a", 1, 1));
            Assert.True(WaitFor(() => SocketTransport.ProbeEntered > entered));
            setter.Start();
            Assert.True(released.Wait(TimeSpan.FromMilliseconds(200)), "liveness probe held the transport lock");
        }
        finally
        {
            stall.Set();
            SocketTransport.ProbeStall = null;
            setter.Join(1000);
        }
    }

    [Fact]
    public void SilentPeer_doesNotHoldAFrameForever()
    {
        using var silentListener = new TcpListener(IPAddress.Loopback, 0);
        silentListener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer, 1024);
        silentListener.Start();
        var heard = new ConcurrentQueue<Envelope>();
        using var other = new SocketTransport("c", new RpcWireCodec());
        other.Bind();
        other.SetHandler(heard.Enqueue);
        other.Start();
        using var client = new SocketTransport("a", new RpcWireCodec());
        // Shorter than the old 1 s progress rule, and the release budget below is
        // this timeout plus a small margin. A writer that waits a full second, or
        // never unblocks, fails.
        var peerDead = TimeSpan.FromMilliseconds(400);
        SetPeerDead(client, peerDead);
        client.Bind();
        client.Start();
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)silentListener.LocalEndpoint).Port));
        client.SetPeer("c", new IPEndPoint(IPAddress.Loopback, other.LocalPort));
        client.Send("a", "b", new RequestVote(1, "a", 0, 0));
        Assert.True(silentListener.Server.Poll(2_000_000, SelectMode.SelectRead));
        using TcpClient accepted = silentListener.AcceptTcpClient();
        accepted.ReceiveBufferSize = 1024;
        Assert.True(WaitFor(() => Outbound(client) is not null));
        Outbound(client)!.Client.SendBufferSize = 1024;
        byte[] command = new byte[256 * 1024];
        command[0] = 4;
        var caller = System.Diagnostics.Stopwatch.StartNew();
        Assert.True(client.Send("a", "b", new AppendEntries(1, "a", 0, 0, [new LogEntry(1, 1, command)], 0)));
        Assert.True(caller.Elapsed < TimeSpan.FromMilliseconds(50), "Raft thread waited on the silent socket");
        caller.Restart();
        client.SetPeer("d", new IPEndPoint(IPAddress.Loopback, 1));
        Assert.True(caller.Elapsed < TimeSpan.FromMilliseconds(50), "Raft thread waited on the silent socket");
        caller.Restart();
        Assert.True(client.Send("a", "c", new RequestVote(2, "a", 1, 1)));
        Assert.True(caller.Elapsed < TimeSpan.FromMilliseconds(50), "a send to another peer waited on the silent socket");
        Assert.True(
            WaitFor(() => heard.Any(envelope => envelope.Payload is RequestVote vote && vote.Term == 2), TimeSpan.FromMilliseconds(200)),
            "a frame to another peer was delayed by the silent send");
        var started = System.Diagnostics.Stopwatch.StartNew();
        var budget = peerDead + TimeSpan.FromMilliseconds(250);
        bool released = false;
        while (!released && started.Elapsed < budget)
        {
            TimeSpan left = budget - started.Elapsed;
            int sliceUs = Math.Max(1, (int)Math.Min(Math.Max(left.TotalMilliseconds, 1), 20)) * 1000;
            if (!silentListener.Server.Poll(sliceUs, SelectMode.SelectRead))
            {
                continue;
            }

            using TcpClient retry = silentListener.AcceptTcpClient();
            released = true;
        }

        Assert.True(released, "silent peer was not released within the peer-dead timeout");
    }

    [Fact]
    public void SlowButLiveReader_pauseOverOneSecond_keepsTheFrameOnOneConnection()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer, 1024);
        listener.Start();
        using var client = new SocketTransport("a", new RpcWireCodec());
        // Longer than the pause, so a peer that is only slow is not dead.
        SetPeerDead(client, TimeSpan.FromSeconds(8));
        client.Bind();
        client.Start();
        client.SetPeer("b", new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port));
        client.Send("a", "b", new RequestVote(1, "a", 0, 0));
        Assert.True(listener.Server.Poll(2_000_000, SelectMode.SelectRead));
        using TcpClient accepted = listener.AcceptTcpClient();
        accepted.ReceiveBufferSize = 1024;
        Assert.False(listener.Pending());
        Assert.True(WaitFor(() => Outbound(client) is not null));
        Outbound(client)!.Client.SendBufferSize = 1024;
        Assert.NotNull(ReadOneFrame(accepted, TimeSpan.FromSeconds(2)).Frame);
        byte[] command = new byte[256 * 1024];
        command[0] = 6;
        client.Send("a", "b", new AppendEntries(1, "a", 0, 0, [new LogEntry(1, 1, command)], 0));
        // Longer than the old 1 s no-progress drop, shorter than the peer-dead timeout.
        new ManualResetEventSlim(false).Wait(1500);
        Assert.False(listener.Pending());
        byte[]? payload = ReadAppend(accepted, TimeSpan.FromSeconds(3), command);
        Assert.False(listener.Pending());
        Assert.NotNull(payload);
        var append = Assert.IsType<AppendEntries>(new RpcWireCodec().Decode(payload).Payload);
        Assert.Equal(6, append.Entries[0].Command[0]);
        Assert.Equal(command.Length, append.Entries[0].Command.Length);
    }

    /// <summary>
    /// Sets the peer-dead timeout when this build has one. Older builds keep the
    /// 1 s progress rule, which is what these tests reject.
    /// </summary>
    private static void SetPeerDead(SocketTransport transport, TimeSpan timeout)
    {
        PropertyInfo? property = typeof(SocketTransport).GetProperty("PeerDeadTimeout");
        property?.SetValue(transport, timeout);
    }

    /// <summary>
    /// Reads until the large append is found. One decoder stays for the whole
    /// socket: a vote and the append can arrive in the same read, and dropping
    /// the tail would decode the next bytes at the wrong offset.
    /// </summary>
    private static byte[]? ReadAppend(TcpClient client, TimeSpan budget, byte[] command)
    {
        var started = System.Diagnostics.Stopwatch.StartNew();
        client.ReceiveTimeout = Math.Max(1, (int)budget.TotalMilliseconds);
        var decoder = new FrameCodec.Decoder();
        var codec = new RpcWireCodec();
        byte[] buf = new byte[8192];
        NetworkStream stream = client.GetStream();
        while (started.Elapsed < budget)
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
                return null;
            }

            foreach (byte[] frame in decoder.Push(buf.AsSpan(0, n).ToArray()))
            {
                if (codec.Decode(frame).Payload is AppendEntries append
                    && append.Entries.Count == 1
                    && append.Entries[0].Command.Length == command.Length
                    && append.Entries[0].Command[0] == command[0])
                {
                    return frame;
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Accepts until a frame arrives. A connect the 75 ms budget aborted can sit
    /// in the backlog and close with no bytes; that socket is not the frame.
    /// </summary>
    private static TcpClient AcceptFrame(TcpListener listener, TimeSpan budget, out byte[]? frame)
    {
        var started = System.Diagnostics.Stopwatch.StartNew();
        while (started.Elapsed < budget)
        {
            int leftUs = Math.Max(1, (int)(budget - started.Elapsed).TotalMilliseconds) * 1000;
            if (!listener.Server.Poll(leftUs, SelectMode.SelectRead))
            {
                break;
            }

            TcpClient accepted = listener.AcceptTcpClient();
            TimeSpan left = budget - started.Elapsed;
            if (left <= TimeSpan.Zero)
            {
                accepted.Dispose();
                break;
            }

            FrameRead read = ReadOneFrame(accepted, left);
            if (read.Closed)
            {
                accepted.Dispose();
                continue;
            }

            frame = read.Frame;
            return accepted;
        }

        frame = null;
        throw new InvalidOperationException("no frame arrived");
    }

    private readonly struct FrameRead
    {
        public byte[]? Frame { get; init; }

        public bool Closed { get; init; }
    }

    private static FrameRead ReadOneFrame(TcpClient client, TimeSpan budget)
    {
        var started = System.Diagnostics.Stopwatch.StartNew();
        client.ReceiveTimeout = Math.Max(1, (int)budget.TotalMilliseconds);
        var decoder = new FrameCodec.Decoder();
        byte[] buf = new byte[8192];
        NetworkStream stream = client.GetStream();
        while (started.Elapsed < budget)
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
                return new FrameRead { Closed = true };
            }

            foreach (byte[] frame in decoder.Push(buf.AsSpan(0, n).ToArray()))
            {
                return new FrameRead { Frame = frame };
            }
        }

        return new FrameRead();
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
