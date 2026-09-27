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

    private Action<Envelope>? _handler;
    private Action _wakeup = static () => { };
    private volatile bool _running;
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

        _listener?.Stop();
        _acceptor?.Join(1000);
        foreach (Thread writer in writers)
        {
            writer.Join(1000);
        }

        GC.SuppressFinalize(this);
    }

    private void AcceptLoop()
    {
        while (_running)
        {
            try
            {
                TcpClient client = _listener!.AcceptTcpClient();
                lock (_gate)
                {
                    _inbound.Add(client);
                }

                var reader = new Thread(() => ReadLoop(client)) { IsBackground = true, Name = "synkrolyn-read-" + _nodeId };
                reader.Start();
            }
            catch (SocketException)
            {
                if (!_running)
                {
                    return;
                }
            }
            catch (InvalidOperationException)
            {
                return;
            }
        }
    }

    private void WriteLoop(string peer, Channel<byte[]> channel)
    {
        while (_running)
        {
            byte[] frame;
            try
            {
                frame = channel.Reader.ReadAsync().AsTask().GetAwaiter().GetResult();
            }
            catch (ChannelClosedException)
            {
                return;
            }

            lock (_gate)
            {
                if (_disabled.Contains(peer))
                {
                    continue;
                }
            }

            try
            {
                TcpClient client = EnsureOutbound(peer);
                NetworkStream stream = client.GetStream();
                stream.Write(frame);
                stream.Flush();
            }
            catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException or InvalidOperationException)
            {
                DropOutbound(peer);
            }
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
            if (_outbound.TryGetValue(peerId, out TcpClient? existing) && existing.Connected)
            {
                return existing;
            }

            if (!_peers.TryGetValue(peerId, out IPEndPoint? found))
            {
                throw new IOException("no address for " + peerId);
            }

            address = found;
        }

        var socket = new TcpClient();
        socket.Connect(address);
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
