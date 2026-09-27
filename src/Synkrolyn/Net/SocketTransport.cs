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
    public void Send(string sender, string recipient, object payload)
    {
        if (sender != _nodeId)
        {
            throw new ArgumentException("from must be this node");
        }

        lock (_gate)
        {
            if (!_peers.ContainsKey(recipient))
            {
                throw new ArgumentException("unknown node id: " + recipient);
            }

            if (_disabled.Contains(recipient))
            {
                return;
            }
        }

        byte[] body = _codec.Encode(sender, payload);
        PeerQueue(recipient).Writer.TryWrite(FrameCodec.Encode(body));
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
        while (_running)
        {
            byte[] frame;
            try
            {
                frame = channel.Reader.ReadAsync().AsTask().GetAwaiter().GetResult();
            }
            catch (Exception ex) when (ex is ChannelClosedException || ex.GetBaseException() is ChannelClosedException)
            {
                return;
            }

            // The frame stays here until it is written or the peer is disabled.
            // A connect refusal, a reset, or a timed-out handshake must not drop it:
            // nothing else will resend a frame that Raft already handed off.
            while (_running && !TrySend(peer, frame))
            {
                if (IsDisabled(peer))
                {
                    break;
                }

                DropOutbound(peer);
                pause.Wait(5);
            }
        }
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
            return true;
        }
        catch (Exception)
        {
            // Any send failure, including a connect abort, leaves the writer alive.
            DropOutbound(peer);
            return false;
        }
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
        IPEndPoint address;
        lock (_gate)
        {
            if (_outbound.TryGetValue(peerId, out TcpClient? existing) && IsLive(existing))
            {
                return existing;
            }

            if (_outbound.Remove(peerId, out TcpClient? dead))
            {
                dead.Dispose();
            }

            if (!_peers.TryGetValue(peerId, out IPEndPoint? found))
            {
                throw new IOException("no address for " + peerId);
            }

            address = found;
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
        try
        {
            Socket raw = client.Client;
            raw.NoDelay = true;
            raw.SendTimeout = IoBudgetMillis;
            using var timeout = new CancellationTokenSource(budget);
            client.ConnectAsync(address, timeout.Token).AsTask().GetAwaiter().GetResult();
            raw.NoDelay = true;
            raw.SendTimeout = IoBudgetMillis;
            return client;
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    private static bool IsLive(TcpClient client)
    {
        try
        {
            if (!client.Connected)
            {
                return false;
            }

            Socket socket = client.Client;
            return !(socket.Poll(0, SelectMode.SelectRead) && socket.Available == 0);
        }
        catch (Exception)
        {
            return false;
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
