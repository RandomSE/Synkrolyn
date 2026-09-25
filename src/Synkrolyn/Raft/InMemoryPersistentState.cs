namespace Synkrolyn.Raft;

/// <summary>In-memory current term and vote. Durable tests use <see cref="FilePersistentState"/>.</summary>
public sealed class InMemoryPersistentState : IPersistentState
{
    private long _currentTerm;
    private string? _votedFor;
    private byte[] _membership = [];

    /// <inheritdoc />
    public long CurrentTerm => _currentTerm;

    /// <inheritdoc />
    public string? VotedFor => _votedFor;

    /// <inheritdoc />
    public void SetCurrentTerm(long term)
    {
        if (term < 0)
        {
            throw new ArgumentException("term must be >= 0 (got " + term + ")");
        }

        if (term < _currentTerm)
        {
            throw new ArgumentException("term must not decrease: current=" + _currentTerm + " proposed=" + term);
        }

        if (term > _currentTerm)
        {
            _currentTerm = term;
            _votedFor = null;
        }
    }

    /// <inheritdoc />
    public void RecordVote(string candidateId)
    {
        if (string.IsNullOrWhiteSpace(candidateId))
        {
            throw new ArgumentException("candidateId must be non-blank");
        }

        if (_votedFor is not null && _votedFor != candidateId)
        {
            throw new InvalidOperationException("already voted for " + _votedFor + " in term " + _currentTerm);
        }

        _votedFor = candidateId;
    }

    /// <inheritdoc />
    public void PersistMembership(byte[] blob)
    {
        ArgumentNullException.ThrowIfNull(blob);
        _membership = (byte[])blob.Clone();
    }

    /// <inheritdoc />
    public byte[] MembershipBlob() => (byte[])_membership.Clone();
}
