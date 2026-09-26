using Synkrolyn.Raft;

namespace Synkrolyn.Kv;

/// <summary>Follower write retry once on the hinted leader.</summary>
public sealed class RedirectingKvClient
{
    private readonly Dictionary<string, KvClient> _byId;

    /// <summary>Creates a redirecting client over the given node clients.</summary>
    public RedirectingKvClient(IReadOnlyDictionary<string, KvClient> byId)
    {
        ArgumentNullException.ThrowIfNull(byId);
        _byId = new Dictionary<string, KvClient>(byId);
    }

    /// <summary>Puts locally, then once on the hinted leader.</summary>
    public long? Put(string fromId, string key, string value)
    {
        KvClient local = Require(fromId);
        return local.Put(key, value) ?? Forward(fromId, local, leader => leader.Put(key, value));
    }

    /// <summary>Deletes locally, then once on the hinted leader.</summary>
    public long? Delete(string fromId, string key)
    {
        KvClient local = Require(fromId);
        return local.Delete(key) ?? Forward(fromId, local, leader => leader.Delete(key));
    }

    /// <summary>Local get on <paramref name="fromId"/>.</summary>
    public string? Get(string fromId, string key) => Require(fromId).Get(key);

    /// <summary>Leader linearizable get, forwarded from a follower.</summary>
    public string? LinearizableGet(string fromId, string key)
    {
        KvClient local = Require(fromId);
        return local.LinearizableGet(key) ?? ForwardGet(fromId, local, leader => leader.LinearizableGet(key));
    }

    /// <summary>Local bounded-stale read, then the hinted leader.</summary>
    public string? BoundedStaleGet(string fromId, string key)
    {
        KvClient local = Require(fromId);
        return local.BoundedStaleGet(key) ?? ForwardGet(fromId, local, leader => leader.BoundedStaleGet(key));
    }

    private string? ForwardGet(string fromId, KvClient local, Func<KvClient, string?> op)
    {
        string? hint = local.LeaderHint();
        if (hint is null || hint == fromId || !_byId.TryGetValue(hint, out KvClient? leader))
        {
            return null;
        }

        return op(leader);
    }

    private long? Forward(string fromId, KvClient local, Func<KvClient, long?> op)
    {
        string? hint = local.LeaderHint();
        if (hint is null || hint == fromId || !_byId.TryGetValue(hint, out KvClient? leader))
        {
            return null;
        }

        return op(leader);
    }

    private KvClient Require(string fromId) =>
        _byId.TryGetValue(fromId, out KvClient? client)
            ? client
            : throw new ArgumentException("unknown client id: " + fromId);
}

/// <summary>Half-open key range <c>[startInclusive, endExclusive)</c>. An empty end means no upper bound.</summary>
public sealed class RangeBinding
{
    /// <summary>Creates a binding.</summary>
    public RangeBinding(string startInclusive, string endExclusive, KvClient client)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        ArgumentNullException.ThrowIfNull(client);
        StartInclusive = startInclusive;
        EndExclusive = endExclusive;
        Client = client;
    }

    /// <summary>Inclusive start.</summary>
    public string StartInclusive { get; }

    /// <summary>Exclusive end. Empty means unbounded.</summary>
    public string EndExclusive { get; }

    /// <summary>Client for this range.</summary>
    public KvClient Client { get; }

    /// <summary>True when <paramref name="key"/> falls in this range.</summary>
    public bool Contains(string key)
    {
        if (string.CompareOrdinal(StartInclusive, key) > 0)
        {
            return false;
        }

        if (EndExclusive.Length == 0)
        {
            return true;
        }

        return string.CompareOrdinal(key, EndExclusive) < 0;
    }
}

/// <summary>Key-routed multi-Raft client. Each range is an independent group.</summary>
public sealed class RangeKvClient
{
    private readonly IReadOnlyList<RangeBinding> _ranges;

    /// <summary>Creates a router. The range list must not be empty.</summary>
    public RangeKvClient(IReadOnlyList<RangeBinding> ranges)
    {
        ArgumentNullException.ThrowIfNull(ranges);
        if (ranges.Count == 0)
        {
            throw new ArgumentException("ranges must not be empty");
        }

        _ranges = ranges;
    }

    /// <summary>Puts on the range that contains <paramref name="key"/>.</summary>
    public long? Put(string key, string value) => ClientFor(key).Put(key, value);

    /// <summary>Deletes on the matching range.</summary>
    public long? Delete(string key) => ClientFor(key).Delete(key);

    /// <summary>Local get on the matching range.</summary>
    public string? Get(string key) => ClientFor(key).Get(key);

    private KvClient ClientFor(string key)
    {
        KvCommandCodec.RequireKey(key);
        foreach (RangeBinding range in _ranges)
        {
            if (range.Contains(key))
            {
                return range.Client;
            }
        }

        throw new ArgumentException("no range for key: " + key);
    }
}
