using Synkrolyn.Raft;

namespace Synkrolyn.Kv;

/// <summary>
/// Leader-facing put and delete. Success means committed and applied on this leader,
/// or null on step-down. Get is a local applied read.
/// </summary>
public sealed class KvClient
{
    private readonly RaftNode _raft;
    private readonly IKvStore _store;
    private readonly string _clientId;
    private long _nextSerial;
    private long? _pendingIndex;
    private RaftNode.ApplyWaiter? _applyWaiter;

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

    /// <summary>Proposes a put. Null until committed and applied, or when this node is not leader.</summary>
    public long? Put(string key, string value)
    {
        byte[] command = _clientId.Length == 0
            ? KvCommandCodec.EncodePut(key, value)
            : KvCommandCodec.EncodePut(key, value, _clientId, ++_nextSerial);
        return ProposeWait(command);
    }

    /// <summary>Proposes a delete.</summary>
    public long? Delete(string key)
    {
        byte[] command = _clientId.Length == 0
            ? KvCommandCodec.EncodeDelete(key)
            : KvCommandCodec.EncodeDelete(key, _clientId, ++_nextSerial);
        return ProposeWait(command);
    }

    /// <summary>Present when the last put or delete is committed and applied on this leader.</summary>
    public long? AwaitCommitted() => Completed();

    /// <summary>Local applied read.</summary>
    public string? Get(string key) => _store.Get(key);

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
            return _store.Get(key);
        }

        _raft.BeginReadIndex();
        return _raft.ReadIndexSatisfied() ? _store.Get(key) : null;
    }

    /// <summary>Bounded-stale lease read. Not linearizable.</summary>
    public string? BoundedStaleGet(string key)
    {
        KvCommandCodec.RequireKey(key);
        if (_raft.Role == Role.Leader)
        {
            return _raft.QuorumLeaseValid ? _store.Get(key) : null;
        }

        return _raft.FollowerReadLeaseValid ? _store.Get(key) : null;
    }

    private long? ProposeWait(byte[] command)
    {
        long? index = _raft.Propose(command);
        if (index is null)
        {
            _pendingIndex = null;
            _applyWaiter = null;
            return null;
        }

        _pendingIndex = index;
        _applyWaiter = _raft.RegisterApplyWaiter(index.Value);
        return Completed();
    }

    private long? Completed()
    {
        if (_pendingIndex is null)
        {
            return null;
        }

        if (_raft.Role != Role.Leader || _applyWaiter is { Failed: true })
        {
            _pendingIndex = null;
            _applyWaiter = null;
            return null;
        }

        if (_applyWaiter is { Applied: true } || _raft.AwaitCommitted(_pendingIndex.Value))
        {
            return _pendingIndex;
        }

        return null;
    }
}
