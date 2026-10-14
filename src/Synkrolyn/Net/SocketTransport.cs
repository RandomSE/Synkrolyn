using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
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

        if (payload is RequestVote or RequestVoteResponse or InstallSnapshotResponse)
        {
            // Votes and snapshot acks must survive a full replication queue.
            // AppendEntries already collapse separately. The 64-slot channel
            // still drops older heartbeats and append replies.
            bool kept = link.Critical.Writer.TryWrite(frame);
            if (kept)
            {
                link.Poke.Set();
            }

            return kept;
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
            link.Critical.Writer.TryComplete();
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

                if (link.Critical.Reader.TryRead(out byte[]? critical))
                {
                    inflight = critical;
                    inflightAppend = false;
                    inflightGeneration = 0;
                    if (!SendUntil(peer, link, critical, false, 0, pause))
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

            NoteDrop("silent " + peer);
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
            SendAll(client.Client, frame, () => PeerSilent(link) || !_running, link);
            if (!OutboundLive(peer))
            {
                // The peer reset while the bytes were only in the socket buffer.
                DropOutbound(peer, client);
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            // Any send failure, including a connect abort, leaves the writer alive.
            NoteDrop(ex is SocketException sock ? "send " + sock.SocketErrorCode : "send " + ex.GetType().Name);
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
        if (link.Critical.Reader.TryPeek(out _) || link.Others.Reader.TryPeek(out _))
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

            // The silence window stays as it is until the replacement socket is
            // stored. Restarting it here would hide a peer that is already dead
            // while this send is still stuck.
        }

        if (client is not null)
        {
            CloseSocket(client);
        }
    }

    private void CloseSocket(TcpClient client)
    {
        // AbortTicks is the test's t0. CancelIoEx completes a Windows overlapped
        // send that Close leaves pending. Shutdown does the same on Linux.
        Volatile.Write(ref AbortTicks, Stopwatch.GetTimestamp());
        Socket? socket = null;
        try
        {
            socket = client.Client;
        }
        catch (Exception ex) when (ex is ObjectDisposedException or NullReferenceException or InvalidOperationException)
        {
            // Dispose already cleared the handle.
        }

        if (socket is not null)
        {
            NativeCancel.Cancel(socket);
        }

        try
        {
            client.Dispose();
        }
        catch (Exception ex) when (ex is ObjectDisposedException or SocketException or InvalidOperationException or NullReferenceException)
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

            // The silence window starts when this connection exists. Time spent
            // refusing the connect is not a quiet peer, and the new socket gets
            // one full peer-dead interval.
            if (_links.TryGetValue(peerId, out PeerLink? link))
            {
                RestartWindow(link);
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
            // A non-blocking connect finishes on this thread. ConnectAsync's
            // callback needs the thread pool, and under a loaded suite that
            // callback misses the budget and the handshake is aborted before
            // the listener can accept it.
            raw.Blocking = false;
            try
            {
                raw.Connect(address);
            }
            catch (SocketException ex) when (ex.SocketErrorCode is SocketError.WouldBlock or SocketError.InProgress or SocketError.IOPending)
            {
                // The handshake is in progress.
            }

            int micros = Math.Max(1, (int)(budget.TotalMilliseconds * 1000));
            if (!raw.Poll(micros, SelectMode.SelectWrite))
            {
                throw new TimeoutException("connect exceeded " + (int)budget.TotalMilliseconds + " ms");
            }

            int error = (int)raw.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Error)!;
            if (error != 0)
            {
                throw new SocketException(error);
            }

            // Leave the socket non-blocking. SendAsync on a blocking socket can
            // sit inside the call while the window is full, where another thread's
            // close waits on the same lock. A pending SendAsync does not.
            raw.Blocking = true;
            raw.Blocking = false;
            ArmKeepAlive(raw);
            return client;
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Test hook. While set, a liveness probe waits here. Production leaves it null.
    /// </summary>
    internal static ManualResetEventSlim? ProbeStall;

    /// <summary>How many liveness probes have started. Tests use this to see the probe is inside.</summary>
    internal static int ProbeEntered;

    /// <summary>Pending send waits that have started on this transport. Tests use this to see a writer blocked in send.</summary>
    internal int SendWaits;

    /// <summary>Pending send waits that have returned on this transport. Tests compare this with <see cref="SendWaits"/>.</summary>
    internal int SendDones;

    /// <summary>Timestamp of this transport's latest socket abort, or 0. Tests measure send completion against it.</summary>
    internal long AbortTicks;

    /// <summary>
    /// When set, the writer blocks after a pending send has completed and before it
    /// can start another frame itself. Tests use this to see whether the completion
    /// callback already flushed the next append.
    /// </summary>
    internal ManualResetEventSlim? HoldWriterAfterPending;

    private static readonly Queue<string> Notes = new();

    /// <summary>Failure-path notes for the connect-reset test. Not on the success path.</summary>
    internal static string RecentNotes()
    {
        lock (Notes)
        {
            return string.Join("; ", Notes);
        }
    }

    /// <summary>Drops notes recorded by earlier tests.</summary>
    internal static void ClearNotes()
    {
        lock (Notes)
        {
            Notes.Clear();
        }
    }

    private static void NoteDrop(string reason)
    {
        lock (Notes)
        {
            if (Notes.Count == 40)
            {
                Notes.Dequeue();
            }

            Notes.Enqueue(reason);
        }
    }

    /// <summary>
    /// Writes <paramref name="frame"/> or throws. The wait is only this peer's
    /// writer. <paramref name="abort"/> is polled while the send is pending.
    /// A silent peer cancels the overlapped send and completes this wait with
    /// <see cref="OperationCanceledException"/>, so the writer does not depend
    /// on the OS finishing the callback. A partial copy into the kernel is not
    /// a reason to reconnect: the peer is still reading.
    /// </summary>
    private void SendAll(Socket socket, byte[] frame, Func<bool> abort, PeerLink link)
    {
        int offset = 0;
        while (offset < frame.Length)
        {
            if (abort())
            {
                throw new OperationCanceledException();
            }

            var args = new SocketAsyncEventArgs();
            var done = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
            int chunk = offset;
            args.Completed += (_, e) =>
            {
                try
                {
                    if (e.SocketError != SocketError.Success || e.BytesTransferred <= 0)
                    {
                        done.TrySetException(new IOException("send failed: " + e.SocketError));
                    }
                    else
                    {
                        if (chunk + e.BytesTransferred >= frame.Length)
                        {
                            // The next append must leave before the writer notices
                            // this completion. The 20 ms wait below is only the abort poll.
                            ChainNext(socket, link, abort);
                        }

                        done.TrySetResult(e.BytesTransferred);
                    }
                }
                finally
                {
                    e.Dispose();
                }
            };

            bool pending;
            try
            {
                args.SetBuffer(frame, offset, frame.Length - offset);
                pending = socket.SendAsync(args);
            }
            catch
            {
                args.Dispose();
                throw;
            }

            if (!pending)
            {
                try
                {
                    if (args.SocketError is SocketError.WouldBlock or SocketError.IOPending or SocketError.TryAgain)
                    {
                        // The window is full. The peer is still reading. Wait for
                        // space instead of dropping the socket.
                        if (!WaitWritable(socket, abort))
                        {
                            throw new OperationCanceledException();
                        }

                        continue;
                    }

                    if (args.SocketError != SocketError.Success || args.BytesTransferred <= 0)
                    {
                        throw new IOException("send failed: " + args.SocketError);
                    }

                    offset += args.BytesTransferred;
                    if (offset >= frame.Length)
                    {
                        ChainNext(socket, link, abort);
                    }
                }
                finally
                {
                    args.Dispose();
                }

                continue;
            }

            offset += WaitPending(done, socket, abort);
        }

        if (Volatile.Read(ref link.ChainRunning) != 0)
        {
            WaitChain(socket, link, abort);
        }
    }

    /// <summary>
    /// Starts <see cref="PeerLink.PendingAppend"/> from the send that just finished.
    /// The writer is still inside <see cref="WaitPending"/>, so this is the only
    /// path that does not wait for that thread to loop.
    /// </summary>
    private void ChainNext(Socket socket, PeerLink link, Func<bool> abort)
    {
        byte[]? next = TakeAppend(link, out _);
        if (next is null)
        {
            TaskCompletionSource<int>? done = link.ChainDone;
            Volatile.Write(ref link.ChainRunning, 0);
            done?.TrySetResult(0);
            return;
        }

        if (Volatile.Read(ref link.ChainRunning) == 0)
        {
            link.ChainDone = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref link.ChainRunning, 1);
        }

        PostFrame(socket, next, 0, link.ChainDone!, link, abort);
    }

    private void PostFrame(
        Socket socket,
        byte[] frame,
        int offset,
        TaskCompletionSource<int> done,
        PeerLink link,
        Func<bool> abort)
    {
        if (abort())
        {
            done.TrySetException(new OperationCanceledException());
            return;
        }

        var args = new SocketAsyncEventArgs();
        args.Completed += (_, e) =>
        {
            try
            {
                FinishChunk(socket, frame, offset, e, done, link, abort);
            }
            finally
            {
                e.Dispose();
            }
        };

        bool pending;
        try
        {
            args.SetBuffer(frame, offset, frame.Length - offset);
            pending = socket.SendAsync(args);
        }
        catch (Exception ex)
        {
            args.Dispose();
            done.TrySetException(ex);
            return;
        }

        if (!pending)
        {
            try
            {
                FinishChunk(socket, frame, offset, args, done, link, abort);
            }
            finally
            {
                args.Dispose();
            }
        }
    }

    private void FinishChunk(
        Socket socket,
        byte[] frame,
        int offset,
        SocketAsyncEventArgs args,
        TaskCompletionSource<int> done,
        PeerLink link,
        Func<bool> abort)
    {
        if (args.SocketError is SocketError.WouldBlock or SocketError.IOPending or SocketError.TryAgain)
        {
            link.RetryFrame = frame;
            link.RetryOffset = offset;
            done.TrySetResult(int.MinValue);
            return;
        }

        if (args.SocketError != SocketError.Success || args.BytesTransferred <= 0)
        {
            done.TrySetException(new IOException("send failed: " + args.SocketError));
            return;
        }

        int next = offset + args.BytesTransferred;
        if (next < frame.Length)
        {
            PostFrame(socket, frame, next, done, link, abort);
            return;
        }

        ChainNext(socket, link, abort);
    }

    private void WaitChain(Socket socket, PeerLink link, Func<bool> abort)
    {
        TaskCompletionSource<int> done = link.ChainDone ?? throw new InvalidOperationException("missing chain");
        while (Volatile.Read(ref link.ChainRunning) != 0)
        {
            int result = WaitPending(done, socket, abort);
            if (result != int.MinValue)
            {
                return;
            }

            if (!WaitWritable(socket, abort))
            {
                throw new OperationCanceledException();
            }

            byte[]? frame = link.RetryFrame;
            int offset = link.RetryOffset;
            link.RetryFrame = null;
            if (frame is null)
            {
                throw new IOException("send failed: " + SocketError.WouldBlock);
            }

            done = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
            link.ChainDone = done;
            PostFrame(socket, frame, offset, done, link, abort);
        }
    }

    /// <summary>
    /// Waits for a pending send. A silent peer cancels it from this thread and
    /// completes the wait even when the native callback does not run.
    /// </summary>
    private int WaitPending(TaskCompletionSource<int> done, Socket socket, Func<bool> abort)
    {
        Interlocked.Increment(ref SendWaits);
        try
        {
            while (!done.Task.IsCompleted)
            {
                if (done.Task.Wait(20))
                {
                    break;
                }

                if (!abort())
                {
                    continue;
                }

                Volatile.Write(ref AbortTicks, Stopwatch.GetTimestamp());
                NativeCancel.Cancel(socket);
                done.TrySetException(new OperationCanceledException());
                break;
            }

            HoldWriterAfterPending?.Wait();
            return done.Task.GetAwaiter().GetResult();
        }
        catch (OperationCanceledException)
        {
            HoldWriterAfterPending?.Wait();
            throw;
        }
        finally
        {
            Interlocked.Increment(ref SendDones);
        }
    }

    /// <summary>Waits until <paramref name="socket"/> can take more bytes, or the peer is dead.</summary>
    private bool WaitWritable(Socket socket, Func<bool> abort)
    {
        Interlocked.Increment(ref SendWaits);
        try
        {
            while (!abort())
            {
                try
                {
                    if (socket.Poll(20_000, SelectMode.SelectWrite))
                    {
                        return true;
                    }
                }
                catch (SocketException ex) when (IsBenign(ex))
                {
                    return true;
                }
                catch (Exception ex) when (ex is ObjectDisposedException or SocketException)
                {
                    return false;
                }
            }

            Volatile.Write(ref AbortTicks, Stopwatch.GetTimestamp());
            NativeCancel.Cancel(socket);
            return false;
        }
        finally
        {
            Interlocked.Increment(ref SendDones);
        }
    }

    private static bool IsLive(TcpClient client)
    {
        Interlocked.Increment(ref ProbeEntered);
        ProbeStall?.Wait();
        try
        {
            Socket socket = client.Client;
            if (!socket.Connected)
            {
                NoteDrop("not-connected");
                return false;
            }

            bool readable;
            try
            {
                readable = socket.Poll(0, SelectMode.SelectRead);
            }
            catch (SocketException ex) when (IsBenign(ex))
            {
                return true;
            }

            int available;
            try
            {
                available = readable ? socket.Available : 1;
            }
            catch (SocketException ex) when (IsBenign(ex))
            {
                return true;
            }

            if (!readable || available > 0)
            {
                return true;
            }

            // A zero-byte poll is not death. WouldBlock is not death. A socket is
            // dead here only for a reset or a FIN that peek reports twice. One
            // empty poll is what dropped a live slow reader.
            if (HasDisconnectError(socket))
            {
                NoteDrop("reset");
                return false;
            }

            if (ConfirmedFin(socket))
            {
                NoteDrop("fin");
                return false;
            }

            return true;
        }
        catch (SocketException ex) when (IsBenign(ex))
        {
            return true;
        }
        catch (SocketException ex) when (IsDisconnect(ex))
        {
            NoteDrop(ex.SocketErrorCode.ToString());
            return false;
        }
        catch (ObjectDisposedException)
        {
            NoteDrop("disposed");
            return false;
        }
        catch (Exception)
        {
            // An unknown probe error is not a dead peer. Silence still closes it.
            return true;
        }
    }

    private static bool IsBenign(SocketException ex) =>
        ex.SocketErrorCode is SocketError.WouldBlock or SocketError.TimedOut or SocketError.IOPending or SocketError.TryAgain or SocketError.Interrupted;

    private static bool IsDisconnect(SocketException ex) =>
        ex.SocketErrorCode is SocketError.ConnectionReset
            or SocketError.ConnectionAborted
            or SocketError.NotConnected
            or SocketError.Shutdown
            or SocketError.Disconnecting
            or SocketError.NetworkReset
            or SocketError.NetworkDown;

    private static bool HasDisconnectError(Socket socket)
    {
        try
        {
            int code = (int)socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Error)!;
            if (code == 0)
            {
                return false;
            }

            var error = (SocketError)code;
            return error is SocketError.ConnectionReset
                or SocketError.ConnectionAborted
                or SocketError.NotConnected
                or SocketError.Shutdown
                or SocketError.Disconnecting
                or SocketError.NetworkReset
                or SocketError.NetworkDown;
        }
        catch (SocketException ex) when (IsBenign(ex))
        {
            return false;
        }
        catch (SocketException ex) when (IsDisconnect(ex))
        {
            return true;
        }
        catch (ObjectDisposedException)
        {
            return true;
        }
    }

    /// <summary>
    /// True only when a non-blocking peek returns 0 twice. A single zero-byte
    /// read is not a FIN: Windows reports that for a peer that has not sent.
    /// </summary>
    private static bool ConfirmedFin(Socket socket)
    {
        try
        {
            socket.Blocking = true;
            socket.Blocking = false;
            var probe = new byte[1];
            int peeked = socket.Receive(probe, SocketFlags.Peek);
            if (peeked > 0)
            {
                return false;
            }

            int again = socket.Receive(probe, SocketFlags.Peek);
            return again == 0;
        }
        catch (SocketException ex) when (IsBenign(ex))
        {
            return false;
        }
        catch (SocketException ex) when (IsDisconnect(ex))
        {
            return true;
        }
        finally
        {
            try
            {
                // Leave the socket non-blocking so the next SendAsync stays pending
                // when the window is full instead of blocking inside the call.
                socket.Blocking = true;
                socket.Blocking = false;
            }
            catch (Exception ex) when (ex is ObjectDisposedException or SocketException)
            {
                // The peer socket was dropped while the probe ran.
            }
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
        public readonly Channel<byte[]> Critical = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
        {
            SingleReader = true,
        });

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

        /// <summary>1 while a completion callback is still sending a newer append.</summary>
        public int ChainRunning;

        /// <summary>Completed when <see cref="ChainRunning"/> drops to 0.</summary>
        public TaskCompletionSource<int>? ChainDone;

        /// <summary>Frame a chained send could not accept because the window was full.</summary>
        public byte[]? RetryFrame;

        /// <summary>Offset into <see cref="RetryFrame"/>.</summary>
        public int RetryOffset;

        public long LastInboundTicks;

        public long WindowStartTicks;

        public void Dispose()
        {
            Poke.Dispose();
            Stop.Dispose();
        }
    }

    /// <summary>
    /// Completes an overlapped send that <see cref="Socket.Close()"/> leaves pending
    /// on Windows, then shuts the socket down so Linux epoll wakes too.
    /// </summary>
    private static class NativeCancel
    {
        public static void Cancel(Socket socket)
        {
            try
            {
                // CancelIoEx(handle, NULL) cancels every overlapped operation on the
                // socket from this thread. Linux has no kernel32; shutdown covers it.
                bool cancelled = CancelIoEx(socket.Handle, IntPtr.Zero);
                if (!cancelled)
                {
                    // 1168 is ERROR_NOT_FOUND (nothing pending). 6 is a stale handle.
                    // Any other code still falls through to shutdown.
                    _ = Marshal.GetLastWin32Error();
                }
            }
            catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException or SocketException or ObjectDisposedException or InvalidOperationException)
            {
                // Not Windows, or the handle is already gone.
            }

            try
            {
                socket.Shutdown(SocketShutdown.Both);
            }
            catch (Exception ex) when (ex is SocketException or ObjectDisposedException or InvalidOperationException or PlatformNotSupportedException)
            {
                // Already shut down.
            }

            try
            {
                socket.Close();
            }
            catch (Exception ex) when (ex is SocketException or ObjectDisposedException or InvalidOperationException)
            {
                // Already closed.
            }
        }

#pragma warning disable SYSLIB1054 // LibraryImport needs an unsafe partial type. DllImport stays.
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CancelIoEx(IntPtr hFile, IntPtr lpOverlapped);
#pragma warning restore SYSLIB1054
    }
}
