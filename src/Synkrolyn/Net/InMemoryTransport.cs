using Synkrolyn.Raft;

namespace Synkrolyn.Net;

/// <summary>
/// Deterministic in-memory transport. Supports drop, bidirectional partition,
/// per-message delay, and link delay. Not a substitute for TCP.
/// Not thread-safe: one dispatcher thread.
/// </summary>
public sealed class InMemoryTransport : ITransport
{
    private readonly IRaftClock _clock;
    private readonly Dictionary<string, Action<Envelope>> _handlers = [];
    private readonly Dictionary<Link, int> _dropCounts = [];
    private readonly Dictionary<Link, Queue<TimeSpan>> _delayNext = [];
    private readonly Dictionary<Link, TimeSpan> _linkDelay = [];
    private readonly HashSet<Partition> _partitions = [];
    private readonly List<Delayed> _delayed = [];
    private long _nextSeq;

    /// <summary>Binds delayed delivery to <paramref name="clock"/> advances.</summary>
    public InMemoryTransport(IRaftClock clock)
    {
        ArgumentNullException.ThrowIfNull(clock);
        _clock = clock;
        _clock.OnAdvance(DeliverDue);
    }

    /// <summary>Registers the first handler for <paramref name="nodeId"/>.</summary>
    public void Register(string nodeId, Action<Envelope> handler)
    {
        RequireNodeId(nodeId);
        ArgumentNullException.ThrowIfNull(handler);
        if (!_handlers.TryAdd(nodeId, handler))
        {
            throw new InvalidOperationException("already registered: " + nodeId);
        }
    }

    /// <summary>Replaces the handler for an already registered node.</summary>
    public void Reregister(string nodeId, Action<Envelope> handler)
    {
        RequireNodeId(nodeId);
        ArgumentNullException.ThrowIfNull(handler);
        if (!_handlers.ContainsKey(nodeId))
        {
            throw new ArgumentException("unknown node id: " + nodeId);
        }

        _handlers[nodeId] = handler;
    }

    /// <inheritdoc />
    public void Send(string sender, string recipient, object payload) => Send(sender, recipient, payload, TimeSpan.Zero);

    /// <summary>Sends after <paramref name="delay"/> on this transport's clock. Zero delivers immediately unless a link delay applies.</summary>
    public void Send(string sender, string recipient, object payload, TimeSpan delay)
    {
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentException("delay must be >= 0");
        }

        RequireRegistered(sender);
        RequireRegistered(recipient);
        ArgumentNullException.ThrowIfNull(payload);
        if (IsPartitioned(sender, recipient) || ConsumeDrop(sender, recipient))
        {
            return;
        }

        var envelope = new Envelope(sender, recipient, payload);
        TimeSpan extra = ConsumeDelayNext(sender, recipient);
        if (_linkDelay.TryGetValue(new Link(sender, recipient), out TimeSpan link))
        {
            extra += link;
        }

        TimeSpan total = delay + extra;
        if (total == TimeSpan.Zero)
        {
            Deliver(envelope);
            return;
        }

        long due = checked(_clock.Millis + (long)total.TotalMilliseconds);
        _delayed.Add(new Delayed(due, _nextSeq++, envelope));
    }

    /// <summary>Holds the next send on this directed link until the clock reaches the delay.</summary>
    public void DelayNext(string from, string to, TimeSpan delay)
    {
        RequireRegistered(from);
        RequireRegistered(to);
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentException("delay must be >= 0");
        }

        if (!_delayNext.TryGetValue(new Link(from, to), out Queue<TimeSpan>? queued))
        {
            queued = new Queue<TimeSpan>();
            _delayNext[new Link(from, to)] = queued;
        }

        queued.Enqueue(delay);
    }

    /// <summary>Adds <paramref name="delay"/> to every send on this directed link. Zero clears it.</summary>
    public void SetLinkDelay(string from, string to, TimeSpan delay)
    {
        RequireRegistered(from);
        RequireRegistered(to);
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentException("delay must be >= 0");
        }

        var link = new Link(from, to);
        if (delay == TimeSpan.Zero)
        {
            _linkDelay.Remove(link);
        }
        else
        {
            _linkDelay[link] = delay;
        }
    }

    /// <summary>Drops the next send on this directed link.</summary>
    public void DropNext(string from, string to)
    {
        RequireRegistered(from);
        RequireRegistered(to);
        var link = new Link(from, to);
        _dropCounts[link] = _dropCounts.GetValueOrDefault(link) + 1;
    }

    /// <summary>Drops messages in both directions until <see cref="HealBidirectional"/>.</summary>
    public void PartitionBidirectional(string a, string b)
    {
        RequireRegistered(a);
        RequireRegistered(b);
        if (a == b)
        {
            throw new ArgumentException("cannot partition a node from itself");
        }

        _partitions.Add(Partition.Of(a, b));
    }

    /// <summary>Removes a bidirectional partition.</summary>
    public void HealBidirectional(string a, string b)
    {
        RequireRegistered(a);
        RequireRegistered(b);
        _partitions.Remove(Partition.Of(a, b));
    }

    /// <summary>Delivers every delayed message whose due time is at or before now.</summary>
    public void DeliverDue()
    {
        long now = _clock.Millis;
        _delayed.Sort(static (x, y) =>
        {
            int byDue = x.Due.CompareTo(y.Due);
            return byDue != 0 ? byDue : x.Seq.CompareTo(y.Seq);
        });
        var ready = new List<Envelope>();
        var kept = new List<Delayed>();
        foreach (Delayed item in _delayed)
        {
            if (item.Due <= now)
            {
                if (!IsPartitioned(item.Envelope.From, item.Envelope.To))
                {
                    ready.Add(item.Envelope);
                }
            }
            else
            {
                kept.Add(item);
            }
        }

        _delayed.Clear();
        _delayed.AddRange(kept);
        foreach (Envelope envelope in ready)
        {
            Deliver(envelope);
        }
    }

    private void Deliver(Envelope envelope) => _handlers[envelope.To](envelope);

    private bool ConsumeDrop(string from, string to)
    {
        var link = new Link(from, to);
        if (!_dropCounts.TryGetValue(link, out int remaining) || remaining <= 0)
        {
            return false;
        }

        if (remaining == 1)
        {
            _dropCounts.Remove(link);
        }
        else
        {
            _dropCounts[link] = remaining - 1;
        }

        return true;
    }

    private TimeSpan ConsumeDelayNext(string from, string to)
    {
        var link = new Link(from, to);
        if (!_delayNext.TryGetValue(link, out Queue<TimeSpan>? queued) || queued.Count == 0)
        {
            return TimeSpan.Zero;
        }

        TimeSpan extra = queued.Dequeue();
        if (queued.Count == 0)
        {
            _delayNext.Remove(link);
        }

        return extra;
    }

    private bool IsPartitioned(string a, string b) => _partitions.Contains(Partition.Of(a, b));

    private void RequireRegistered(string nodeId)
    {
        RequireNodeId(nodeId);
        if (!_handlers.ContainsKey(nodeId))
        {
            throw new ArgumentException("unknown node id: " + nodeId);
        }
    }

    private static void RequireNodeId(string nodeId)
    {
        if (string.IsNullOrWhiteSpace(nodeId))
        {
            throw new ArgumentException("node id must be non-blank");
        }
    }

    private readonly record struct Link(string From, string To);

    private readonly record struct Partition(string Left, string Right)
    {
        public static Partition Of(string a, string b) =>
            string.CompareOrdinal(a, b) <= 0 ? new Partition(a, b) : new Partition(b, a);
    }

    private readonly record struct Delayed(long Due, long Seq, Envelope Envelope);
}
