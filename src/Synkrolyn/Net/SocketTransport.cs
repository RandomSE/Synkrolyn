using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;

namespace Synkrolyn.Net;

/// <summary>
/// Localhost TCP transport. <see cref="Send"/> enqueues a framed payload and does not
/// block the caller on a socket write. I/O threads only enqueue Raft work.
/// </summary>
public sealed class SocketTransport : ITransport, IDisposable
{
    private readonly string _nodeId;
    private readonly IMessageCodec _codec;
    private readonly Dictionary<string, IPEndPoint> _peers = [];
    private readonly HashSet<string> _disabled = [];
    private readonly Dictionary<string, TcpClient> _outbound = [];
    private readonly Dictionary<string, Channel<byte[]>> _peerQueues = [];
    private readonly Dictionary<string, Thread> _writers = [];
    private readonly List<TcpClient> _inbound = [];
    private readonly object _gate = new();

    /// <summary>
    /// A blocked connect or send fails inside this budget. The socket fixture's
    /// shortest election timeout is 120 ms, so one stall must end before CheckQuorum
    /// steps the leader down while the same frame is still being retried.
    /// </summary>
    private const int IoBudgetMillis = 75;

    /// <summary>
    /// How long an idle writer parks before it checks that the peer still holds
    /// the last frame. A queued frame wakes the wait early.
    /// </summary>
    private const int ProbeMillis = 5;

    /// <summary>
    /// A send that makes no progress for this long is a dead window, not a slow
    /// reader. The writer drops that socket and sends the same frame again.
    /// Loopback to a peer that is still reading finishes inside it. The 75 ms
    /// connect budget stays connect-only.
    /// </summary>
    private const int SendStallMillis = 1000;

    private Action<Envelope>? _handler;
    private Action _wakeup = static () => { };
    private volatile bool _running;
    private int _failConnects;
    private TcpListener? _listener;
    private Thread? _acceptor;

    /// <summary>Creates an unbound transport for <paramref name="nodeId"/>.</summary>
    public SocketTransport(string nodeId, IMessageCodec codec)
    {
        if (string.IsNullOrWhiteSpace(nodeId))
        {
            throw new ArgumentException("nodeId must be non-blank", nameof(nodeId));
        }

        _nodeId = nodeId;
        _codec = codec ?? throw new ArgumentNullException(nameof(codec));
    }

    /// <summary>Binds an ephemeral localhost port.</summary>
    public void Bind()
    {
        if (_listener is not null)
        {
            throw new InvalidOperationException("already bound");
        }

        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
    }

    /// <summary>Bound TCP port.</summary>
    public int LocalPort => (_listener?.LocalEndpoint as IPEndPoint)?.Port
        ?? throw new InvalidOperationException("not bound");

    /// <summary>Records the address of a peer.</summary>
    public void SetPeer(string peerId, IPEndPoint address)
    {
        if (string.IsNullOrWhiteSpace(peerId) || peerId == _nodeId)
        {
            throw new ArgumentException("peer id");
        }

        ArgumentNullException.ThrowIfNull(address);
        lock (_gate)
        {
            _peers[peerId] = address;
        }
    }

    /// <summary>Inbound envelope handler. Must only enqueue Raft work.</summary>
    public void SetHandler(Action<Envelope> handler) => _handler = handler ?? throw new ArgumentNullException(nameof(handler));

    /// <summary>Wakes the Raft thread after an inbound frame.</summary>
    public void SetWakeup(Action wakeup) => _wakeup = wakeup ?? throw new ArgumentNullException(nameof(wakeup));

    /// <summary>The next <paramref name="count"/> outbound connects fail with connection refused.</summary>
    internal void FailNextConnects(int count)
    {
        if (count < 0)
        {
            throw new ArgumentException("count must be >= 0", nameof(count));
        }

        Volatile.Write(ref _failConnects, count);
    }

    /// <summary>Closes accepted sockets. The listener stays up so peers can reconnect.</summary>
    internal void DropInbound()
    {
        TcpClient[] clients;
        lock (_gate)
        {
            clients = _inbound.ToArray();
            _inbound.Clear();
        }

        foreach (TcpClient client in clients)
        {
            client.Dispose();
        }
    }

    /// <summary>Starts accept and write threads.</summary>
    public void Start()
    {
        if (_listener is null)
        {
            throw new InvalidOperationException("not bound");
        }

        if (_running)
        {
            return;
        }

        _running = true;
        _acceptor = new Thread(AcceptLoop) { IsBackground = true, Name = "synkrolyn-accept-" + _nodeId };
        _acceptor.Start();
    }

    /// <summary>Closes sockets to <paramref name="peerId"/> and drops further sends.</summary>
    public void DisconnectPeer(string peerId)
    {
        lock (_gate)
        {
            _disabled.Add(peerId);
            if (_outbound.Remove(peerId, out TcpClient? client))
            {
                client.Dispose();
            }
        }
    }

    /// <summary>Allows sends to <paramref name="peerId"/> again.</summary>
    public void ReconnectPeer(string peerId)
    {
        lock (_gate)
        {
            _disabled.Remove(peerId);
        }
    }

    /// <summary>Drops every peer.</summary>
    public void DisconnectAll()
    {
        string[] peers;
        lock (_gate)
        {
            peers = _peers.Keys.ToArray();
        }

        foreach (string peer in peers)
        {
            DisconnectPeer(peer);
        }
    }

    /// <summary>Allows sends to every peer again.</summary>
    public void ReconnectAll()
    {
        lock (_gate)
        {
            _disabled.Clear();
        }
    }

    /// <inheritdoc />
    public bool HasPeer(string nodeId)
    {
        lock (_gate)
        {
            return _peers.ContainsKey(nodeId);
        }
    }

    /// <inheritdoc />
    public bool Send(string sender, string recipient, object payload)
    {
        if (sender != _nodeId)
        {
            throw new ArgumentException("from must be this node");
        }

        lock (_gate)
        {
            if (!_peers.ContainsKey(recipient))
            {
                Trace.WriteLine("drop send to unknown node id: " + recipient);
                return false;
            }

            if (_disabled.Contains(recipient))
            {
                return false;
            }
        }

        byte[] body = _codec.Encode(sender, payload);
        return PeerQueue(recipient).Writer.TryWrite(FrameCodec.Encode(body));
    }

    private Channel<byte[]> PeerQueue(string recipient)
    {
        lock (_gate)
        {
            if (_peerQueues.TryGetValue(recipient, out Channel<byte[]>? existing))
            {
                return existing;
            }

            var channel = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(64)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.DropOldest,
            });
            _peerQueues[recipient] = channel;
            var thread = new Thread(() => WriteLoop(recipient, channel))
            {
                IsBackground = true,
                Name = "synkrolyn-write-" + _nodeId + "-" + recipient,
            };
            _writers[recipient] = thread;
            thread.Start();
            return channel;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _running = false;
        try
        {
            _listener?.Stop();
        }
        catch (Exception ex) when (ex is SocketException or ObjectDisposedException or InvalidOperationException)
        {
            // Stopping the listener unblocks Accept.
        }

        _acceptor?.Join(1000);

        Channel<byte[]>[] channels;
        Thread[] writers;
        TcpClient[] inbound;
        lock (_gate)
        {
            channels = _peerQueues.Values.ToArray();
            writers = _writers.Values.ToArray();
            inbound = _inbound.ToArray();
            _inbound.Clear();
            foreach (TcpClient client in _outbound.Values)
            {
                client.Dispose();
            }

            _outbound.Clear();
        }

        foreach (Channel<byte[]> channel in channels)
        {
            channel.Writer.TryComplete();
        }

        foreach (TcpClient client in inbound)
        {
            client.Dispose();
        }

        foreach (Thread writer in writers)
        {
            writer.Join(1000);
        }

        GC.SuppressFinalize(this);
    }

    private void AcceptLoop()
    {
        using var pause = new ManualResetEventSlim(false);
        while (_running)
        {
            TcpClient client;
            try
            {
                client = _listener!.AcceptTcpClient();
            }
            catch (Exception)
            {
                if (!_running)
                {
                    return;
                }

                // One accept error must not exit the listener or spin a core.
                pause.Wait(5);
                continue;
            }

            client.NoDelay = true;
            if (!_running)
            {
                client.Dispose();
                return;
            }

            lock (_gate)
            {
                if (!_running)
                {
                    client.Dispose();
                    return;
                }

                _inbound.Add(client);
            }

            var reader = new Thread(() => ReadLoop(client)) { IsBackground = true, Name = "synkrolyn-read-" + _nodeId };
            reader.Start();
        }
    }

    private void WriteLoop(string peer, Channel<byte[]> channel)
    {
        using var pause = new ManualResetEventSlim(false);
        byte[]? inflight = null;
        Task<bool>? queued = null;
        while (_running)
        {
            // Write can return after the kernel has copied the bytes and before the
            // peer's Read. DropInbound then discards them, IsLive still passed, and
            // nothing else will resend a frame Raft already handed off. Hold the
            // last frame and resend it when that socket dies, before any newer one.
            if (inflight is not null && !OutboundLive(peer))
            {
                DropOutbound(peer);
                pause.Wait(5);
                inflight = SendUntil(peer, inflight, pause) ? inflight : null;
                continue;
            }

            if (channel.Reader.TryRead(out byte[]? frame))
            {
                queued = null;
                inflight = SendUntil(peer, frame, pause) ? frame : null;
                continue;
            }

            if (inflight is null)
            {
                try
                {
                    frame = channel.Reader.ReadAsync().AsTask().GetAwaiter().GetResult();
                }
                catch (Exception ex) when (ex is ChannelClosedException || ex.GetBaseException() is ChannelClosedException)
                {
                    return;
                }

                inflight = SendUntil(peer, frame, pause) ? frame : null;
                continue;
            }

            queued ??= channel.Reader.WaitToReadAsync().AsTask();
            if (queued.IsCompleted)
            {
                if (!queued.IsCompletedSuccessfully || !queued.Result)
                {
                    return;
                }

                queued = null;
                continue;
            }

            // Park until the next frame or a short probe. A frame that arrives
            // during the wait is taken immediately; an idle writer does not spin.
            queued.Wait(ProbeMillis);
        }
    }

    private bool SendUntil(string peer, byte[] frame, ManualResetEventSlim pause)
    {
        while (_running && !TrySend(peer, frame))
        {
            if (IsDisabled(peer))
            {
                return false;
            }

            DropOutbound(peer);
            pause.Wait(5);
        }

        return _running && !IsDisabled(peer);
    }

    private bool TrySend(string peer, byte[] frame)
    {
        try
        {
            if (IsDisabled(peer))
            {
                return true;
            }

            TcpClient client = EnsureOutbound(peer);
            NetworkStream stream = client.GetStream();
            stream.Write(frame);
            stream.Flush();
            if (!OutboundLive(peer))
            {
                // The peer reset while the bytes were only in the socket buffer.
                DropOutbound(peer);
                return false;
            }

            return true;
        }
        catch (Exception)
        {
            // Any send failure, including a connect abort, leaves the writer alive.
            DropOutbound(peer);
            return false;
        }
    }

    private bool OutboundLive(string peer)
    {
        TcpClient? client;
        lock (_gate)
        {
            if (!_outbound.TryGetValue(peer, out client))
            {
                return false;
            }
        }

        // The probe can block. It must not run while this lock is held, or one
        // peer stalls SetPeer and every other writer.
        return IsLive(client);
    }

    private bool IsDisabled(string peer)
    {
        lock (_gate)
        {
            return _disabled.Contains(peer);
        }
    }

    private void DropOutbound(string peer)
    {
        lock (_gate)
        {
            if (_outbound.Remove(peer, out TcpClient? client))
            {
                client.Dispose();
            }
        }
    }

    private TcpClient EnsureOutbound(string peerId)
    {
        TcpClient? existing;
        IPEndPoint address;
        lock (_gate)
        {
            _outbound.TryGetValue(peerId, out existing);
            if (!_peers.TryGetValue(peerId, out IPEndPoint? found))
            {
                throw new IOException("no address for " + peerId);
            }

            address = found;
        }

        if (existing is not null && IsLive(existing))
        {
            return existing;
        }

        if (existing is not null)
        {
            lock (_gate)
            {
                if (_outbound.TryGetValue(peerId, out TcpClient? current) && ReferenceEquals(current, existing))
                {
                    _outbound.Remove(peerId);
                }
            }

            existing.Dispose();
        }

        if (Volatile.Read(ref _failConnects) > 0 && Interlocked.Decrement(ref _failConnects) >= 0)
        {
            throw new SocketException(10061);
        }

        TcpClient socket = ConnectWithin(address, TimeSpan.FromMilliseconds(IoBudgetMillis));
        lock (_gate)
        {
            if (_disabled.Contains(peerId))
            {
                socket.Dispose();
                throw new IOException("peer disabled: " + peerId);
            }

            if (_outbound.TryGetValue(peerId, out TcpClient? raced) && raced.Connected)
            {
                socket.Dispose();
                return raced;
            }

            if (_outbound.Remove(peerId, out TcpClient? stale))
            {
                stale.Dispose();
            }

            _outbound[peerId] = socket;
            return socket;
        }
    }

    private static TcpClient ConnectWithin(IPEndPoint address, TimeSpan budget)
    {
        var client = new TcpClient();
        var done = new ManualResetEventSlim(false);
        try
        {
            Socket raw = client.Client;
            raw.NoDelay = true;
            var args = new SocketAsyncEventArgs { RemoteEndPoint = address };
            try
            {
                args.Completed += (_, _) =>
                {
                    try
                    {
                        done.Set();
                    }
                    catch (ObjectDisposedException)
                    {
                        // The budget already elapsed and the event was released.
                    }
                };
                bool pending = raw.ConnectAsync(args);
                if (!pending)
                {
                    done.Set();
                }

                if (!done.Wait(budget))
                {
                    client.Dispose();
                    done.Wait(50);
                    throw new TimeoutException("connect exceeded " + (int)budget.TotalMilliseconds + " ms");
                }

                if (args.SocketError != SocketError.Success)
                {
                    throw new SocketException((int)args.SocketError);
                }

                // ConnectAsync leaves the native socket non-blocking, and the managed
                // Blocking flag can already read true, so assigning true is a no-op.
                // A full send buffer then fails the write and the writer drops a live
                // slow reader mid-frame. Toggle so the native mode follows. A send
                // that makes no progress still ends, so a silent peer cannot pin the
                // frame; a peer that is reading drains the buffer and the write returns.
                raw.Blocking = false;
                raw.Blocking = true;
                raw.SendTimeout = SendStallMillis;
                ArmDeadPeerTimeout(raw);
                return client;
            }
            finally
            {
                args.Dispose();
            }
        }
        catch
        {
            client.Dispose();
            throw;
        }
        finally
        {
            done.Dispose();
        }
    }

    /// <summary>
    /// Test hook. While set, a liveness probe waits here. Production leaves it null.
    /// </summary>
    internal static ManualResetEventSlim? ProbeStall;

    /// <summary>How many liveness probes have started. Tests use this to see the probe is inside.</summary>
    internal static int ProbeEntered;

    private static bool IsLive(TcpClient client)
    {
        Interlocked.Increment(ref ProbeEntered);
        ProbeStall?.Wait();
        try
        {
            if (!client.Connected)
            {
                return false;
            }

            Socket socket = client.Client;
            if (!socket.Poll(0, SelectMode.SelectRead) || socket.Available > 0)
            {
                return true;
            }

            // Poll said readable with nothing queued. On Windows that is also true
            // for a peer that has not sent. A blocking peek would wait forever, and
            // a receive timeout stores WSAETIMEDOUT and leaves the socket
            // indeterminate, so this path never sets one. A non-blocking peek that
            // would block is a live socket. A zero-byte peek is a graceful FIN and
            // is dead: the next write can still succeed on a half-closed socket and
            // the frame would never be sent again.
            try
            {
                socket.Blocking = true;
                socket.Blocking = false;
                int peeked = socket.Receive(new byte[1], SocketFlags.Peek);
                return peeked > 0;
            }
            catch (SocketException ex) when (ex.SocketErrorCode is SocketError.WouldBlock or SocketError.TimedOut or SocketError.IOPending or SocketError.TryAgain)
            {
                return true;
            }
            finally
            {
                try
                {
                    socket.Blocking = false;
                    socket.Blocking = true;
                }
                catch (ObjectDisposedException)
                {
                    // The peer socket was dropped while the probe ran.
                }
            }
        }
        catch (Exception)
        {
            return false;
        }
    }

    private static void ArmDeadPeerTimeout(Socket raw)
    {
        raw.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        try
        {
            raw.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 1);
            raw.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 1);
            raw.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 3);
        }
        catch (SocketException)
        {
            // This OS has no per-socket keepalive timers. A blocked write still ends.
        }

        try
        {
            if (OperatingSystem.IsLinux())
            {
                // TCP_USER_TIMEOUT (18), milliseconds. Unacked data aborts a half-open peer.
                raw.SetSocketOption(SocketOptionLevel.Tcp, (SocketOptionName)18, SendStallMillis);
            }
            else if (OperatingSystem.IsWindows())
            {
                // TCP_MAXRT (5), seconds.
                raw.SetSocketOption(SocketOptionLevel.Tcp, (SocketOptionName)5, Math.Max(1, SendStallMillis / 1000));
            }
        }
        catch (SocketException)
        {
            // Keepalive and the send stall still bound a peer that stops answering.
        }
    }

    private void ReadLoop(TcpClient client)
    {
        var decoder = new FrameCodec.Decoder();
        try
        {
            NetworkStream stream = client.GetStream();
            byte[] buf = new byte[8192];
            int read;
            while (_running && (read = stream.Read(buf, 0, buf.Length)) > 0)
            {
                List<byte[]> frames;
                try
                {
                    frames = decoder.Push(buf.AsSpan(0, read).ToArray());
                }
                catch (ArgumentException)
                {
                    return;
                }

                foreach (byte[] payload in frames)
                {
                    IMessageCodec.Decoded decoded;
                    try
                    {
                        decoded = _codec.Decode(payload);
                    }
                    catch (Exception)
                    {
                        return;
                    }

                    _handler?.Invoke(new Envelope(decoded.From, _nodeId, decoded.Payload));
                    _wakeup();
                }
            }
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException or InvalidOperationException)
        {
            // Connection close stays on the I/O thread.
        }
        finally
        {
            lock (_gate)
            {
                _inbound.Remove(client);
            }

            client.Dispose();
        }
    }
}
