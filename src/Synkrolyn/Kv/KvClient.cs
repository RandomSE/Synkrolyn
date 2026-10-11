using Synkrolyn.Raft;

namespace Synkrolyn.Kv;

/// <summary>
/// Leader-facing put and delete. Success means committed and applied on this leader,
/// or null on step-down. Get is a local applied read.
/// A registered session retries one sequence until the caller observes the result.
/// </summary>
public sealed class KvClient
{
    private RaftNode _raft;
    private IKvStore _store;
    private readonly string _clientId;
    private long _nextSerial;
    private long? _pendingIndex;
    private RaftNode.ApplyWaiter? _applyWaiter;
    private long _readTicket;
    private bool _readTicketOpen;
    private long _sessionId;
    private long _seq;
    private long _acked;
    private byte[]? _inflight;
    private byte[]? _registerCommand;
    private long? _registerIndex;
    private RaftNode.ApplyWaiter? _registerWaiter;
    private TaskCompletionSource<long?>? _putAsync;

    /// <summary>Creates a client without exactly-once serials.</summary>
    public KvClient(RaftNode raft, IKvStore store)
        : this(raft, store, "")
    {
    }

    /// <summary>Creates a client. A non-empty <paramref name="clientId"/> enables serial exactly-once.</summary>
    public KvClient(RaftNode raft, IKvStore store, string? clientId)
    {
        _raft = raft ?? throw new ArgumentNullException(nameof(raft));
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _clientId = clientId ?? "";
    }

    /// <summary>Client id assigned by the registration entry, or 0 before that entry is applied.</summary>
    public long ClientId
    {
        get
        {
            ObserveRegister();
            return _sessionId;
        }
    }

    /// <summary>Proposes a session registration. The applied log index is the client id.</summary>
    public long? Register(TimeSpan sessionTimeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(sessionTimeout, TimeSpan.Zero);

        if (ObserveRegister())
        {
            return _sessionId;
        }

        if (_registerCommand is null)
        {
            long? now = _raft.LeaderMillis();
            if (now is null)
            {
                return null;
            }

            _registerCommand = KvCommandCodec.EncodeRegister(now.Value, (long)sessionTimeout.TotalMilliseconds);
        }

        long? index = _raft.Propose(_registerCommand);
        if (index is null)
        {
            return null;
        }

        _registerIndex = index;
        _registerWaiter = _raft.RegisterApplyWaiter(index.Value);
        return ObserveRegister() ? _sessionId : null;
    }

    /// <summary>Points this client at another replica. The session id and the unacked command stay.</summary>
    public void Follow(RaftNode raft, IKvStore store)
    {
        ArgumentNullException.ThrowIfNull(raft);
        ArgumentNullException.ThrowIfNull(store);
        _raft = raft;
        _store = store;
        _applyWaiter = null;
        _pendingIndex = null;
        _readTicketOpen = false;
    }

    /// <summary>Appends the leader clock so session expiry is decided from the log. Null when not leader.</summary>
    public long? RecordTime()
    {
        long? now = _raft.LeaderMillis();
        if (now is null)
        {
            return null;
        }

        return _raft.Propose(KvCommandCodec.EncodeTick(now.Value));
    }

    /// <summary>Proposes a put. Null until committed and applied, or when this node is not leader.</summary>
    public long? Put(string key, string value)
    {
        if (_sessionId != 0)
        {
            return SessionMutate(put: true, key, value);
        }

        byte[] command = _clientId.Length == 0
            ? KvCommandCodec.EncodePut(key, value)
            : KvCommandCodec.EncodePut(key, value, _clientId, ++_nextSerial);
        return ProposeNew(command, retrySame: false);
    }

    /// <summary>Proposes a put and completes when it is applied. The task does not start a thread.</summary>
    public Task<long?> PutAsync(string key, string value)
    {
        var tcs = new TaskCompletionSource<long?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _putAsync = tcs;
        long? index = Put(key, value);
        if (tcs.Task.IsCompleted)
        {
            return tcs.Task;
        }

        if (index is not null)
        {
            tcs.TrySetResult(index);
            _putAsync = null;
            return tcs.Task;
        }

        if (_applyWaiter is null || _applyWaiter.Failed || _applyWaiter.Indeterminate)
        {
            tcs.TrySetResult(null);
            _putAsync = null;
            return tcs.Task;
        }

        return tcs.Task;
    }

    /// <summary>Proposes a delete.</summary>
    public long? Delete(string key)
    {
        if (_sessionId != 0)
        {
            return SessionMutate(put: false, key, value: "");
        }

        byte[] command = _clientId.Length == 0
            ? KvCommandCodec.EncodeDelete(key)
            : KvCommandCodec.EncodeDelete(key, _clientId, ++_nextSerial);
        return ProposeNew(command, retrySame: false);
    }

    /// <summary>Present when the last put or delete is committed and applied on this leader.</summary>
    public long? AwaitCommitted()
    {
        long? done = Completed();
        if (done is not null)
        {
            Deliver();
        }

        return done;
    }

    /// <summary>Local applied read. A registered session that has expired throws.</summary>
    public string? Get(string key)
    {
        if (_sessionId != 0)
        {
            return _store.GetForSession(_sessionId, key);
        }

        return _store.Get(key);
    }

    /// <summary>Local applied read as a completed task. No thread is started.</summary>
    public Task<string?> GetAsync(string key)
    {
        try
        {
            return Task.FromResult(Get(key));
        }
        catch (Exception ex)
        {
            return Task.FromException<string?>(ex);
        }
    }

    /// <summary>Last known leader id.</summary>
    public string? LeaderHint() => _raft.LeaderId;

    /// <summary>Leader ReadIndex or send-time lease. Followers return null.</summary>
    public string? LinearizableGet(string key)
    {
        KvCommandCodec.RequireKey(key);
        if (_raft.Role != Role.Leader)
        {
            return null;
        }

        if (_raft.ReadIndexSatisfied())
        {
            return Get(key);
        }

        if (_readTicketOpen)
        {
            if (_raft.ReadIndexSatisfied(_readTicket))
            {
                _readTicketOpen = false;
                return Get(key);
            }

            if (_raft.ReadTicketPending(_readTicket) && _raft.Role == Role.Leader)
            {
                return null;
            }

            _readTicketOpen = false;
        }

        if (_raft.Role != Role.Leader || _raft.BeginReadIndex() is not ReadIndexStart started)
        {
            return null;
        }

        _readTicket = started.Ticket;
        _readTicketOpen = true;
        if (_raft.ReadIndexSatisfied(_readTicket))
        {
            _readTicketOpen = false;
            return Get(key);
        }

        return null;
    }

    /// <summary>Bounded-stale lease read. Not linearizable.</summary>
    public string? BoundedStaleGet(string key)
    {
        KvCommandCodec.RequireKey(key);
        if (_raft.Role == Role.Leader)
        {
            return _raft.QuorumLeaseValid ? Get(key) : null;
        }

        return _raft.FollowerReadLeaseValid ? Get(key) : null;
    }

    private long? SessionMutate(bool put, string key, string value)
    {
        if (_inflight is null)
        {
            long? now = _raft.LeaderMillis();
            if (now is null)
            {
                return null;
            }

            long seq = _seq + 1;
            _inflight = put
                ? KvCommandCodec.EncodeSessionPut(key, value, _sessionId, seq, _acked, now.Value)
                : KvCommandCodec.EncodeSessionDelete(key, _sessionId, seq, _acked, now.Value);
            _seq = seq;
            _pendingIndex = null;
            _applyWaiter = null;
        }

        long? index = ProposeNew(_inflight, retrySame: true);
        if (index is not null)
        {
            Deliver();
        }

        return index;
    }

    private bool ObserveRegister()
    {
        if (_sessionId != 0)
        {
            return true;
        }

        if (_registerWaiter is { Applied: true } && _registerIndex is long index)
        {
            _sessionId = index;
            _registerCommand = null;
            return true;
        }

        return false;
    }

    private void Deliver()
    {
        if (_inflight is null)
        {
            return;
        }

        _acked = _seq;
        _inflight = null;
    }

    private void OnPutSettled(RaftNode.ApplyWaiter waiter)
    {
        TaskCompletionSource<long?>? tcs = _putAsync;
        _putAsync = null;
        if (waiter.Applied)
        {
            Deliver();
            tcs?.TrySetResult(waiter.Index);
            return;
        }

        tcs?.TrySetResult(null);
    }

    private long? ProposeNew(byte[] command, bool retrySame)
    {
        if (_applyWaiter is { Indeterminate: true })
        {
            if (!retrySame)
            {
                return null;
            }

            _applyWaiter = null;
            _pendingIndex = null;
            return null;
        }

        if (retrySame && _inflight is not null && _pendingIndex is not null && _applyWaiter is not null)
        {
            if (_applyWaiter.Failed)
            {
                _applyWaiter = null;
                _pendingIndex = null;
                return null;
            }

            return Completed();
        }

        if (_applyWaiter is { Failed: true })
        {
            _pendingIndex = null;
            _applyWaiter = null;
        }

        long? index = _raft.Propose(command);
        if (index is null)
        {
            _pendingIndex = null;
            _applyWaiter = null;
            return null;
        }

        _pendingIndex = index;
        _applyWaiter = _raft.RegisterApplyWaiter(index.Value, _putAsync is null ? null : OnPutSettled);
        if (retrySame && _applyWaiter is { Indeterminate: true })
        {
            _applyWaiter = null;
            _pendingIndex = null;
            return null;
        }

        return Completed();
    }

    private long? Completed()
    {
        if (_pendingIndex is null)
        {
            return null;
        }

        if (_applyWaiter is { Indeterminate: true })
        {
            return null;
        }

        if (_applyWaiter is { Failed: true })
        {
            _pendingIndex = null;
            _applyWaiter = null;
            return null;
        }

        if (_applyWaiter is { Applied: true })
        {
            return _pendingIndex;
        }

        return null;
    }
}
