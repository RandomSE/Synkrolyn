namespace Synkrolyn.Raft;

/// <summary>Shared prefix-compaction rules for in-memory and file logs.</summary>
internal static class LogCompaction
{
    public static void Apply(List<LogEntry> entries, long currentLastIncluded, long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (lastIncludedIndex < 1)
        {
            throw new ArgumentException("lastIncludedIndex must be >= 1");
        }

        if (lastIncludedTerm < 1)
        {
            throw new ArgumentException("lastIncludedTerm must be >= 1");
        }

        if (lastIncludedIndex < currentLastIncluded)
        {
            throw new ArgumentException(
                "lastIncludedIndex " + lastIncludedIndex + " is before current " + currentLastIncluded);
        }

        entries.RemoveAll(entry => entry.Index <= lastIncludedIndex);
        if (entries.Count > 0 && entries[0].Index != lastIncludedIndex + 1)
        {
            entries.Clear();
        }
    }
}
