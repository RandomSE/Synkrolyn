using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Synkrolyn.Raft;

namespace Synkrolyn.Net;

/// <summary>
/// Localhost TCP transport. <see cref="Send"/> enqueues a framed payload and does not
/// block the caller on a socket write. Each peer has its own writer. A peer is closed
/// only when no frame has arrived from it for <see cref="PeerDeadTimeout"/>, or when
/// its socket is already shut. I/O threads only enqueue Raft work.
/// </summary>
public sealed class SocketTransport : ITransport, IDisposable
{
    private readonly string _nodeId;
    private readonly IMessageCodec _codec;
    private readonly Dictionary<string, IPEndPoint> _peers = [];
    private readonly HashSet<string> _disabled = [];
    private readonly Dictionary<string, TcpClient> _outbound = [];
    private readonly Dictionary<string, PeerLink> _links = [];
    private readonly Dictionary<string, long> _lastInbound = [];
    private readonly Dictionary<string, Thread> _writers = [];
    private readonly List<TcpClient> _inbound = [];
    private readonly object _gate = new();

    /// <summary>
    /// A blocked connect fails inside this budget. The socket fixture's shortest
    /// election timeout is 120 ms, so one refused connect ends before CheckQuorum
    /// steps the leader down while the same frame is still being retried.
    /// </summary>
    private const int IoBudgetMillis = 75;

    /// <summary>
    /// How long an idle writer parks before it checks that the peer still holds
    /// the last frame. A queued frame wakes the wait early.
    /// </summary>
    private const int ProbeMillis = 5;

    /// <summary>How often the per-peer checker looks for a silent socket.</summary>
    private const int PeerCheckMillis = 25;

    /// <summary>Matches the host election timeout when the caller does not set one.</summary>
    private long _peerDeadMillis = 150;

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

    /// <summary>
    /// How long a peer may send nothing before its socket is closed. An ack or any
    /// other inbound frame restarts the wait. Send progress does not. Callers set
    /// this to the node's election timeout, the same window CheckQuorum already
    /// uses, so closing the socket does not move failover earlier.
    /// </summary>
    public TimeSpan PeerDeadTimeout
    {
        get => TimeSpan.FromMilliseconds(Volatile.Read(ref _peerDeadMillis));
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);

            Volatile.Write(ref _peerDeadMillis, (long)value.TotalMilliseconds);
        }
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
            CloseSocket(client);
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
        TcpClient? client;
        lock (_gate)
        {
            _disabled.Add(peerId);
            _outbound.Remove(peerId, out client);
        }

        if (client is not null)
        {
            CloseSocket(client);
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

        byte[] frame = FrameCodec.Encode(_codec.Encode(sender, payload));
        PeerLink link = LinkFor(recipient);
        if (payload is AppendEntries)
        {
            // The leader rebuilds AppendEntries from nextIndex. An older heartbeat
            // that has not been handed to the socket is replaced, so a stuck peer
            // cannot queue an unbounded run of the same replication.
            lock (link.Slot)
            {
                link.PendingAppend = frame;
                link.AppendGeneration++;
            }

            link.Poke.Set();
            return true;
        }

        bool queued = link.Others.Writer.TryWrite(frame);
        if (queued)
        {
            link.Poke.Set();
        }

        return queued;
    }

    private PeerLink LinkFor(string recipient)
    {
        Thread? spawned = null;
        PeerLink link;
        lock (_gate)
        {
            if (_links.TryGetValue(recipient, out PeerLink? existing))
            {
                return existing;
            }

            link = new PeerLink();
            if (_lastInbound.TryGetValue(recipient, out long heard))
            {
                link.LastInboundTicks = heard;
            }

            _links[recipient] = link;
            spawned = new Thread(() => WriteLoop(recipient, link))
            {
                IsBackground = true,
                Name = "synkrolyn-write-" + _nodeId + "-" + recipient,
            };
            _writers[recipient] = spawned;
        }

        spawned.Start();
        return link;
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

        PeerLink[] links;
        Thread[] writers;
        TcpClient[] inbound;
        TcpClient[] outbound;
        lock (_gate)
        {
            links = _links.Values.ToArray();
            writers = _writers.Values.ToArray();
            inbound = _inbound.ToArray();
            outbound = _outbound.Values.ToArray();
            _inbound.Clear();
            _outbound.Clear();
        }

        foreach (TcpClient client in outbound)
        {
            CloseSocket(client);
        }

        foreach (PeerLink link in links)
        {
            link.Others.Writer.TryComplete();
            link.Poke.Set();
            link.Stop.Set();
        }

        foreach (TcpClient client in inbound)
        {
            CloseSocket(client);
        }

        foreach (Thread writer in writers)
        {
            writer.Join(1000);
        }

        foreach (PeerLink link in links)
        {
            link.Dispose();
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

    private void WriteLoop(string peer, PeerLink link)
    {
        var checker = new Thread(() => WatchPeer(peer, link))
        {
            IsBackground = true,
            Name = "synkrolyn-dead-" + _nodeId + "-" + peer,
        };
        checker.Start();
        using var pause = new ManualResetEventSlim(false);
        try
        {
            byte[]? inflight = null;
            bool inflightAppend = false;
            long inflightGeneration = 0;
            while (_running)
            {
                // The last frame stays here until a newer one replaces it or the
                // socket dies. A reset discards bytes the kernel had accepted, and
                // nothing else will resend a frame Raft already handed off.
                if (inflight is not null && !OutboundLive(peer))
                {
                    DropOutbound(peer);
                    if (inflightAppend && Superseded(link, inflightGeneration))
                    {
                        inflight = null;
                    }
                    else if (!SendUntil(peer, link, inflight, inflightAppend, inflightGeneration, pause))
                    {
                        inflight = null;
                    }

                    continue;
                }

                if (link.Others.Reader.TryRead(out byte[]? other))
                {
                    inflight = other;
                    inflightAppend = false;
                    inflightGeneration = 0;
                    if (!SendUntil(peer, link, other, false, 0, pause))
                    {
                        inflight = null;
                    }

                    continue;
                }

                byte[]? append = TakeAppend(link, out long generation);
                if (append is not null)
                {
                    inflight = append;
                    inflightAppend = true;
                    inflightGeneration = generation;
                    if (!SendUntil(peer, link, append, true, generation, pause))
                    {
                        inflight = null;
                    }

                    continue;
                }

                link.Poke.Reset();
                if (HasQueued(link))
                {
                    continue;
                }

                // Park until the next frame or a short probe. A frame that arrives
                // during the wait is taken immediately; an idle writer does not spin.
                link.Poke.Wait(ProbeMillis);
            }
        }
        finally
        {
            link.Stop.Set();
            checker.Join(1000);
        }
    }

    /// <summary>
    /// Closes a socket that has not delivered an inbound frame for
    /// <see cref="PeerDeadTimeout"/>. The close runs on this thread so a
    /// <see cref="Socket.SendAsync(SocketAsyncEventArgs)"/> parked on the writer
    /// completes with an error. Send progress is not a reason to close.
    /// </summary>
    private void WatchPeer(string peer, PeerLink link)
    {
        while (_running && !link.Stop.Wait(PeerCheckMillis))
        {
            if (!PeerSilent(link))
            {
                continue;
            }

            TcpClient? client;
            lock (_gate)
            {
                _outbound.TryGetValue(peer, out client);
            }

            if (client is null || !PeerSilent(link))
            {
                continue;
            }

            DropOutbound(peer, client);
            link.Poke.Set();
        }
    }

    private bool SendUntil(
        string peer,
        PeerLink link,
        byte[] frame,
        bool append,
        long generation,
        ManualResetEventSlim pause)
    {
        while (_running)
        {
            if (IsDisabled(peer) || (append && Superseded(link, generation)))
            {
                return false;
            }

            if (TrySend(peer, link, frame))
            {
                return true;
            }

            if (!_running || IsDisabled(peer) || (append && Superseded(link, generation)))
            {
                return false;
            }

            pause.Wait(5);
        }

        return false;
    }

    private bool TrySend(string peer, PeerLink link, byte[] frame)
    {
        try
        {
            if (IsDisabled(peer))
            {
                return false;
            }

            EnsureWindow(link);
            TcpClient client = EnsureOutbound(peer);
            SendAll(client.Client, frame);
            if (!OutboundLive(peer))
            {
                // The peer reset while the bytes were only in the socket buffer.
                DropOutbound(peer, client);
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

    private static void EnsureWindow(PeerLink link)
    {
        lock (link.Slot)
        {
            if (link.WindowStartTicks == 0)
            {
                link.WindowStartTicks = Stopwatch.GetTimestamp();
            }
        }
    }

    private static void RestartWindow(PeerLink link)
    {
        lock (link.Slot)
        {
            link.WindowStartTicks = Stopwatch.GetTimestamp();
        }
    }

    private bool PeerSilent(PeerLink link)
    {
        long timeout = Volatile.Read(ref _peerDeadMillis);
        long lastIn;
        long window;
        lock (link.Slot)
        {
            lastIn = link.LastInboundTicks;
            window = link.WindowStartTicks;
        }

        long basis = lastIn > window ? lastIn : window;
        if (basis == 0 || timeout <= 0)
        {
            return false;
        }

        return Stopwatch.GetElapsedTime(basis).TotalMilliseconds >= timeout;
    }

    private static byte[]? TakeAppend(PeerLink link, out long generation)
    {
        lock (link.Slot)
        {
            if (link.PendingAppend is null)
            {
                generation = 0;
                return null;
            }

            byte[] frame = link.PendingAppend;
            generation = link.AppendGeneration;
            link.PendingAppend = null;
            return frame;
        }
    }

    private static bool Superseded(PeerLink link, long generation)
    {
        lock (link.Slot)
        {
            return link.AppendGeneration != generation;
        }
    }

    private static bool HasQueued(PeerLink link)
    {
        if (link.Others.Reader.TryPeek(out _))
        {
            return true;
        }

        lock (link.Slot)
        {
            return link.PendingAppend is not null;
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

    private void DropOutbound(string peer, TcpClient? expected = null)
    {
        TcpClient? client;
        lock (_gate)
        {
            if (!_outbound.TryGetValue(peer, out client))
            {
                client = null;
            }
            else if (expected is not null && !ReferenceEquals(client, expected))
            {
                client = null;
            }
            else
            {
                _outbound.Remove(peer);
            }

            if (client is not null && _links.TryGetValue(peer, out PeerLink? link))
            {
                RestartWindow(link);
            }
        }

        if (client is not null)
        {
            CloseSocket(client);
        }
    }

    private static void CloseSocket(TcpClient client)
    {
        try
        {
            // Closing from this thread completes a SendAsync parked on the writer.
            // A blocking Socket.Send can hold the same lock Close waits on; the
            // async send does not.
            client.Client.Close();
        }
        catch (Exception ex) when (ex is ObjectDisposedException or SocketException or InvalidOperationException)
        {
            // The writer already dropped this socket.
        }

        try
        {
            client.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // Already closed.
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

            CloseSocket(existing);
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
                CloseSocket(socket);
                throw new IOException("peer disabled: " + peerId);
            }

            if (_outbound.TryGetValue(peerId, out TcpClient? raced) && raced.Connected)
            {
                CloseSocket(socket);
                return raced;
            }

            if (_outbound.Remove(peerId, out TcpClient? stale))
            {
                CloseSocket(stale);
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
                // slow reader mid-frame. Toggle so the native mode follows.
                raw.Blocking = false;
                raw.Blocking = true;
                ArmKeepAlive(raw);
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

    /// <summary>
    /// Writes <paramref name="frame"/> or throws. The wait is only this peer's
    /// writer. Closing the socket from the checker unblocks it. A partial copy
    /// into the kernel is not a reason to reconnect: the peer is still reading.
    /// </summary>
    private static void SendAll(Socket socket, byte[] frame)
    {
        using var done = new ManualResetEventSlim(false);
        using var args = new SocketAsyncEventArgs();
        args.Completed += (_, _) =>
        {
            try
            {
                done.Set();
            }
            catch (ObjectDisposedException)
            {
                // The writer is already leaving this send.
            }
        };

        int offset = 0;
        while (offset < frame.Length)
        {
            done.Reset();
            args.SetBuffer(frame, offset, frame.Length - offset);
            bool pending = socket.SendAsync(args);
            if (pending)
            {
                done.Wait();
            }

            if (args.SocketError != SocketError.Success || args.BytesTransferred <= 0)
            {
                throw new IOException("send failed: " + args.SocketError);
            }

            offset += args.BytesTransferred;
        }
    }

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

    /// <summary>
    /// Enables keepalive at the OS default intervals. <c>TCP_USER_TIMEOUT</c> and
    /// <c>TCP_MAXRT</c> stay at the OS default: a one-second cap aborts a live peer
    /// that has only stopped draining.
    /// </summary>
    private static void ArmKeepAlive(Socket raw)
    {
        try
        {
            raw.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        }
        catch (SocketException)
        {
            // The OS keeps its default. The peer-dead close is what unblocks a send.
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

                    NoteInbound(decoded.From);
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

    private void NoteInbound(string peer)
    {
        if (string.IsNullOrWhiteSpace(peer))
        {
            return;
        }

        long now = Stopwatch.GetTimestamp();
        lock (_gate)
        {
            _lastInbound[peer] = now;
            if (_links.TryGetValue(peer, out PeerLink? link))
            {
                lock (link.Slot)
                {
                    link.LastInboundTicks = now;
                }
            }
        }
    }

    /// <summary>One peer's outbound queue. AppendEntries collapse to the latest unsent frame.</summary>
    private sealed class PeerLink : IDisposable
    {
        public readonly Channel<byte[]> Others = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(64)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.DropOldest,
        });

        public readonly object Slot = new();

        public readonly ManualResetEventSlim Poke = new(false);

        public readonly ManualResetEventSlim Stop = new(false);

        public byte[]? PendingAppend;

        public long AppendGeneration;

        public long LastInboundTicks;

        public long WindowStartTicks;

        public void Dispose()
        {
            Poke.Dispose();
            Stop.Dispose();
        }
    }
}
