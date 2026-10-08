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

    /// <summary>
    /// O(1) capture of the suffix through <paramref name="lastIncludedIndex"/>. The background
    /// writer then reads only that capture. The default has nothing to freeze.
    /// </summary>
    void ArmCompaction(long lastIncludedIndex, long lastIncludedTerm)
    {
    }

    /// <summary>
    /// One locked view of the suffix an AppendEntries send needs. A concurrent
    /// compaction cannot slip between the prefix check and the entry reads.
    /// </summary>
    ReplicationBatch ReadForReplication(long nextIndex, int maxEntries, int maxBytes)
    {
        long included = LastIncludedIndex;
        if (included > 0 && nextIndex <= included)
        {
            return new ReplicationBatch(true, 0, 0, []);
        }

        long prevIndex = nextIndex - 1;
        long prevTerm = prevIndex == 0 ? 0 : prevIndex == included ? LastIncludedTerm : Read(prevIndex).Term;
        var entries = new List<LogEntry>();
        long last = Math.Min(LastIndex, nextIndex + maxEntries - 1);
        int bytes = 0;
        for (long i = nextIndex; i <= last; i++)
        {
            LogEntry entry = Read(i);
            if (entries.Count > 0 && bytes + entry.Command.Length > maxBytes)
            {
                break;
            }

            entries.Add(entry);
            bytes += entry.Command.Length;
        }

        return new ReplicationBatch(false, prevIndex, prevTerm, entries);
    }

    /// <summary>
    /// Copies the suffix at <paramref name="lastIncludedIndex"/>. <see cref="StagedCompaction.Write"/>
    /// runs off the Raft thread and reads only an immutable capture. <see cref="StagedCompaction.Commit"/>
    /// publishes the result on the Raft thread.
    /// </summary>
    StagedCompaction StageCompaction(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot)
    {
        long index = lastIncludedIndex;
        long term = lastIncludedTerm;
        byte[] bytes = snapshot;
        return new StagedCompaction(
            static () => { },
            () => CompactThrough(index, term, bytes));
    }

    /// <summary>Durability barrier. File logs share one fsync across the pending batch.</summary>
    void Force();
}

/// <summary>Prev-log term and entries for one AppendEntries, read under one lock.</summary>
public readonly record struct ReplicationBatch(
    bool NeedsSnapshot,
    long PrevIndex,
    long PrevTerm,
    IReadOnlyList<LogEntry> Entries);

/// <summary>Compaction whose file write is separate from the in-memory swap.</summary>
public sealed class StagedCompaction
{
    private readonly Action _write;
    private readonly Action _commit;

    /// <summary>Creates a compaction. <paramref name="write"/> performs the slow IO.</summary>
    public StagedCompaction(Action write, Action commit)
    {
        _write = write ?? throw new ArgumentNullException(nameof(write));
        _commit = commit ?? throw new ArgumentNullException(nameof(commit));
    }

    /// <summary>Writes the staged bytes. Safe to call off the Raft thread.</summary>
    public void Write() => _write();

    /// <summary>Publishes the staged compaction. Called on the Raft thread.</summary>
    public void Commit() => _commit();
}
