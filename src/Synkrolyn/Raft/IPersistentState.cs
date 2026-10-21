namespace Synkrolyn.Raft;

/// <summary>
/// Raft persistent state: current term and vote. Terms are monotonic.
/// Initial term is 0 and the vote is empty.
/// </summary>
public interface IPersistentState
{
    /// <summary>Current term. Starts at 0.</summary>
    long CurrentTerm { get; }

    /// <summary>Candidate voted for in <see cref="CurrentTerm"/>, or null.</summary>
    string? VotedFor { get; }

    /// <summary>Sets the term. A strictly greater term clears the vote. The same term is a no-op.</summary>
    void SetCurrentTerm(long term);

    /// <summary>Records a vote in the current term. The same candidate is idempotent.</summary>
    void RecordVote(string candidateId);

    /// <summary>
    /// Largest AppendEntries lease bound stored with the term and vote. Zero when
    /// no bound has been advertised. A restart gate uses this with the node's own timeout.
    /// </summary>
    long MaxAdvertisedLeaseMillis { get; }

    /// <summary>
    /// Stores <paramref name="millis"/> when it is larger than the bound already stored.
    /// A smaller or non-positive value does not shrink the bound and is not written.
    /// </summary>
    void NoteAdvertisedLease(long millis);

    /// <summary>Persists the latest applied membership blob. In-memory state keeps it in process.</summary>
    void PersistMembership(byte[] blob);

    /// <summary>Applied membership blob, or empty when none was stored.</summary>
    byte[] MembershipBlob();
}
