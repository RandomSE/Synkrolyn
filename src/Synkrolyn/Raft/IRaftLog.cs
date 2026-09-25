namespace Synkrolyn.Raft;

/// <summary>
/// 1-indexed Raft log. Index 0 is never a valid entry. Gaps are forbidden.
/// An empty log reports <see cref="LastIndex"/> 0 and <see cref="LastTerm"/> 0.
/// </summary>
public interface IRaftLog
{
    /// <summary>Appends <paramref name="entry"/> at <see cref="LastIndex"/> + 1.</summary>
    void Append(LogEntry entry);

    /// <summary>Returns the entry at <paramref name="index"/>.</summary>
    LogEntry Read(long index);

    /// <summary>Highest assigned index, or 0 when the log is empty.</summary>
    long LastIndex { get; }

    /// <summary>Term of <see cref="LastIndex"/>, or 0 when the log is empty.</summary>
    long LastTerm { get; }

    /// <summary>
    /// Highest index known to be durable. In-memory logs advance this in <see cref="Force"/>.
    /// File logs advance it only after fsync. Unforced appends do not move it.
    /// </summary>
    long DurableIndex { get; }

    /// <summary>Deletes <paramref name="index"/> and every entry after it. <c>lastIndex + 1</c> is a no-op.</summary>
    void TruncateFrom(long index);

    /// <summary>Highest index included in the snapshot prefix, or 0 when never compacted.</summary>
    long LastIncludedIndex { get; }

    /// <summary>Term of <see cref="LastIncludedIndex"/>, or 0 when there is no snapshot.</summary>
    long LastIncludedTerm { get; }

    /// <summary>Lowest index still stored as a log entry.</summary>
    long FirstIndex => LastIncludedIndex + 1;

    /// <summary>Snapshot bytes installed at <see cref="LastIncludedIndex"/>, or empty.</summary>
    byte[] SnapshotBytes();

    /// <summary>Compacts through <paramref name="lastIncludedIndex"/>, keeping a contiguous suffix.</summary>
    void CompactThrough(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot);

    /// <summary>Durability barrier. File logs share one fsync across the pending batch.</summary>
    void Force();
}
