namespace Synkrolyn.Raft;

/// <summary>In-memory 1-indexed Raft log. <see cref="Force"/> marks the visible suffix durable without disk I/O.</summary>
public sealed class InMemoryRaftLog : IRaftLog
{
    private readonly List<LogEntry> _entries = [];
    private long _lastIncludedIndex;
    private long _lastIncludedTerm;
    private byte[] _snapshot = [];
    private long _durableIndex;

    /// <inheritdoc />
    public void Append(LogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        long expected = LastIndex + 1;
        if (entry.Index != expected)
        {
            throw new ArgumentException(
                "Raft log does not allow gaps: expected index " + expected + " but was " + entry.Index);
        }

        _entries.Add(entry);
    }

    /// <inheritdoc />
    public LogEntry Read(long index)
    {
        RequirePositive(index);
        if (index <= _lastIncludedIndex)
        {
            throw new InvalidOperationException("log entry at index " + index + " was compacted");
        }

        if (index > LastIndex)
        {
            throw new KeyNotFoundException("no log entry at index " + index);
        }

        return _entries[Offset(index)];
    }

    /// <inheritdoc />
    public long LastIndex => _entries.Count == 0 ? _lastIncludedIndex : _entries[^1].Index;

    /// <inheritdoc />
    public long LastTerm => _entries.Count == 0 ? _lastIncludedTerm : _entries[^1].Term;

    /// <inheritdoc />
    public long DurableIndex => _durableIndex;

    /// <inheritdoc />
    public void TruncateFrom(long index)
    {
        RequirePositive(index);
        if (index <= _lastIncludedIndex)
        {
            throw new ArgumentException(
                "truncateFrom index " + index + " is inside compacted prefix (lastIncludedIndex=" + _lastIncludedIndex + ")");
        }

        long endExclusive = LastIndex + 1;
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
            _durableIndex = LastIndex;
        }
    }

    /// <inheritdoc />
    public long LastIncludedIndex => _lastIncludedIndex;

    /// <inheritdoc />
    public long LastIncludedTerm => _lastIncludedTerm;

    /// <inheritdoc />
    public byte[] SnapshotBytes() => (byte[])_snapshot.Clone();

    /// <inheritdoc />
    public void CompactThrough(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot)
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

    /// <inheritdoc />
    public void Force()
    {
        _durableIndex = LastIndex;
    }

    private int Offset(long index) => checked((int)(index - _lastIncludedIndex - 1));

    private static void RequirePositive(long index)
    {
        if (index < 1)
        {
            throw new ArgumentException(LogEntry.IndexZeroMessage + " (got " + index + ")");
        }
    }
}
