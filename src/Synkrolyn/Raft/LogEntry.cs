namespace Synkrolyn.Raft;

/// <summary>
/// One Raft log entry. Indexes are 1-based. The command buffer is copied on
/// construction and again when <see cref="Command"/> is read.
/// </summary>
public sealed class LogEntry
{
    /// <summary>Message used when an index is not positive.</summary>
    public const string IndexZeroMessage = "Raft log is 1-indexed; index 0 is invalid";

    private readonly byte[] _command;

    /// <summary>Creates an entry. <paramref name="command"/> may be empty and is copied.</summary>
    public LogEntry(long index, long term, byte[] command)
    {
        if (index < 1)
        {
            throw new ArgumentException(IndexZeroMessage + " (got " + index + ")", nameof(index));
        }

        if (term < 1)
        {
            throw new ArgumentException("log entry term must be >= 1 (got " + term + ")", nameof(term));
        }

        ArgumentNullException.ThrowIfNull(command);
        Index = index;
        Term = term;
        _command = (byte[])command.Clone();
    }

    /// <summary>1-based log index.</summary>
    public long Index { get; }

    /// <summary>Term in which the entry was created.</summary>
    public long Term { get; }

    /// <summary>A copy of the opaque command bytes.</summary>
    public byte[] Command => (byte[])_command.Clone();

    /// <summary>Command bytes without an extra copy. Callers must not mutate the array.</summary>
    internal byte[] CommandUnsafe => _command;
}
