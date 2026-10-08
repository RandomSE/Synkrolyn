namespace Synkrolyn.Raft;

/// <summary>In-memory 1-indexed Raft log. <see cref="Force"/> marks the visible suffix durable without disk I/O.</summary>
public sealed class InMemoryRaftLog : IRaftLog
{
    private readonly object _gate = new();
    private readonly List<LogEntry> _entries = [];
    private long _lastIncludedIndex;
    private long _lastIncludedTerm;
    private byte[] _snapshot = [];
    private long _durableIndex;

    /// <inheritdoc />
    public void Append(LogEntry entry)
    {
        lock (_gate)
        {
            ArgumentNullException.ThrowIfNull(entry);
            long expected = LastIndexUnlocked() + 1;
            if (entry.Index != expected)
            {
                throw new ArgumentException(
                    "Raft log does not allow gaps: expected index " + expected + " but was " + entry.Index);
            }

            _entries.Add(entry);
        }
    }

    /// <inheritdoc />
    public LogEntry Read(long index)
    {
        lock (_gate)
        {
            return ReadUnlocked(index);
        }
    }

    /// <inheritdoc />
    public ReplicationBatch ReadForReplication(long nextIndex, int maxEntries, int maxBytes)
    {
        lock (_gate)
        {
            return ReadBatchUnlocked(nextIndex, maxEntries, maxBytes);
        }
    }

    /// <inheritdoc />
    public long LastIndex
    {
        get
        {
            lock (_gate)
            {
                return LastIndexUnlocked();
            }
        }
    }

    /// <inheritdoc />
    public long LastTerm
    {
        get
        {
            lock (_gate)
            {
                return _entries.Count == 0 ? _lastIncludedTerm : _entries[^1].Term;
            }
        }
    }

    /// <inheritdoc />
    public long DurableIndex
    {
        get
        {
            lock (_gate)
            {
                return _durableIndex;
            }
        }
    }

    /// <inheritdoc />
    public void TruncateFrom(long index)
    {
        lock (_gate)
        {
            RequirePositive(index);
            if (index <= _lastIncludedIndex)
            {
                throw new ArgumentException(
                    "truncateFrom index " + index + " is inside compacted prefix (lastIncludedIndex=" + _lastIncludedIndex + ")");
            }

            long endExclusive = LastIndexUnlocked() + 1;
            if (index > endExclusive)
            {
                throw new ArgumentException("truncateFrom index " + index + " is past lastIndex+1 (" + endExclusive + ")");
            }

            if (index == endExclusive)
            {
                return;
            }

            _entries.RemoveRange(Offset(index), _entries.Count - Offset(index));
            if (_durableIndex >= index)
            {
                _durableIndex = LastIndexUnlocked();
            }
        }
    }

    /// <inheritdoc />
    public long LastIncludedIndex
    {
        get
        {
            lock (_gate)
            {
                return _lastIncludedIndex;
            }
        }
    }

    /// <inheritdoc />
    public long LastIncludedTerm
    {
        get
        {
            lock (_gate)
            {
                return _lastIncludedTerm;
            }
        }
    }

    /// <inheritdoc />
    public byte[] SnapshotBytes()
    {
        lock (_gate)
        {
            return (byte[])_snapshot.Clone();
        }
    }

    /// <inheritdoc />
    public void CompactThrough(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot)
    {
        lock (_gate)
        {
            CompactUnlocked(lastIncludedIndex, lastIncludedTerm, snapshot);
        }
    }

    /// <inheritdoc />
    public void Force()
    {
        lock (_gate)
        {
            _durableIndex = LastIndexUnlocked();
        }
    }

    private ReplicationBatch ReadBatchUnlocked(long nextIndex, int maxEntries, int maxBytes)
    {
        if (_lastIncludedIndex > 0 && nextIndex <= _lastIncludedIndex)
        {
            return new ReplicationBatch(true, 0, 0, []);
        }

        long prevIndex = nextIndex - 1;
        long prevTerm = prevIndex == 0
            ? 0
            : prevIndex == _lastIncludedIndex
                ? _lastIncludedTerm
                : ReadUnlocked(prevIndex).Term;
        var entries = new List<LogEntry>();
        long last = Math.Min(LastIndexUnlocked(), nextIndex + maxEntries - 1);
        int bytes = 0;
        for (long i = nextIndex; i <= last; i++)
        {
            LogEntry entry = ReadUnlocked(i);
            if (entries.Count > 0 && bytes + entry.Command.Length > maxBytes)
            {
                break;
            }

            entries.Add(entry);
            bytes += entry.Command.Length;
        }

        return new ReplicationBatch(false, prevIndex, prevTerm, entries);
    }

    private void CompactUnlocked(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot)
    {
        LogCompaction.Apply(_entries, _lastIncludedIndex, lastIncludedIndex, lastIncludedTerm, snapshot);
        _lastIncludedIndex = lastIncludedIndex;
        _lastIncludedTerm = lastIncludedTerm;
        _snapshot = (byte[])snapshot.Clone();
        if (_durableIndex < lastIncludedIndex)
        {
            _durableIndex = lastIncludedIndex;
        }
    }

    private LogEntry ReadUnlocked(long index)
    {
        RequirePositive(index);
        if (index <= _lastIncludedIndex)
        {
            throw new InvalidOperationException("log entry at index " + index + " was compacted");
        }

        if (index > LastIndexUnlocked())
        {
            throw new KeyNotFoundException("no log entry at index " + index);
        }

        return _entries[Offset(index)];
    }

    private long LastIndexUnlocked() => _entries.Count == 0 ? _lastIncludedIndex : _entries[^1].Index;

    private int Offset(long index) => checked((int)(index - _lastIncludedIndex - 1));

    private static void RequirePositive(long index)
    {
        if (index < 1)
        {
            throw new ArgumentException(LogEntry.IndexZeroMessage + " (got " + index + ")");
        }
    }
}
