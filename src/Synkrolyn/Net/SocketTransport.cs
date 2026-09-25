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
    private readonly Channel<Outbound> _queue = Channel.CreateUnbounded<Outbound>(new UnboundedChannelOptions { SingleReader = true });
    private readonly object _gate = new();

    private Action<Envelope>? _handler;
    private Action _wakeup = static () => { };
    private volatile bool _running;
    private TcpListener? _listener;
    private Thread? _acceptor;
    private Thread? _writer;

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
        _writer = new Thread(WriteLoop) { IsBackground = true, Name = "synkrolyn-write-" + _nodeId };
        _acceptor.Start();
        _writer.Start();
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
        _queue.Writer.TryWrite(new Outbound(recipient, FrameCodec.Encode(body)));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _running = false;
        _queue.Writer.TryComplete();
        _listener?.Stop();
        lock (_gate)
        {
            foreach (TcpClient client in _outbound.Values)
            {
                client.Dispose();
            }

            _outbound.Clear();
        }

        _acceptor?.Join(1000);
        _writer?.Join(1000);
        GC.SuppressFinalize(this);
    }

    private void AcceptLoop()
    {
        while (_running)
        {
            try
            {
                TcpClient client = _listener!.AcceptTcpClient();
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

    private void WriteLoop()
    {
        while (_running)
        {
            if (!_queue.Reader.TryRead(out Outbound next))
            {
                if (!_queue.Reader.WaitToReadAsync().AsTask().Wait(50))
                {
                    continue;
                }

                continue;
            }

            lock (_gate)
            {
                if (_disabled.Contains(next.To))
                {
                    continue;
                }
            }

            try
            {
                TcpClient client = EnsureOutbound(next.To);
                NetworkStream stream = client.GetStream();
                stream.Write(next.Frame);
                stream.Flush();
            }
            catch (IOException)
            {
                lock (_gate)
                {
                    if (_outbound.Remove(next.To, out TcpClient? client))
                    {
                        client.Dispose();
                    }
                }
            }
            catch (SocketException)
            {
                lock (_gate)
                {
                    if (_outbound.Remove(next.To, out TcpClient? client))
                    {
                        client.Dispose();
                    }
                }
            }
        }
    }

    private TcpClient EnsureOutbound(string peerId)
    {
        lock (_gate)
        {
            if (_outbound.TryGetValue(peerId, out TcpClient? existing) && existing.Connected)
            {
                return existing;
            }

            if (!_peers.TryGetValue(peerId, out IPEndPoint? address))
            {
                throw new IOException("no address for " + peerId);
            }

            var socket = new TcpClient();
            socket.Connect(address);
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
        catch (IOException)
        {
            // Connection close stays on the I/O thread.
        }
        finally
        {
            client.Dispose();
        }
    }

    private readonly record struct Outbound(string To, byte[] Frame);
}
