namespace Synkrolyn.Raft;

/// <summary>RequestVote RPC, including the PreVote probe flag.</summary>
public sealed record RequestVote(long Term, string CandidateId, long LastLogIndex, long LastLogTerm, bool PreVote)
{
    /// <summary>A real vote request. <see cref="PreVote"/> is false.</summary>
    public RequestVote(long term, string candidateId, long lastLogIndex, long lastLogTerm)
        : this(term, candidateId, lastLogIndex, lastLogTerm, false)
    {
    }

    /// <summary>Validates fields.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(CandidateId))
        {
            throw new ArgumentException("candidateId");
        }

        if (Term < 0 || LastLogIndex < 0 || LastLogTerm < 0)
        {
            throw new ArgumentException("term");
        }
    }
}

/// <summary>RequestVote result. <see cref="PreVote"/> answers must not count as real votes.</summary>
public sealed record RequestVoteResponse(long Term, bool VoteGranted, bool PreVote)
{
    /// <summary>A real vote response.</summary>
    public RequestVoteResponse(long term, bool voteGranted)
        : this(term, voteGranted, false)
    {
    }
}

/// <summary>AppendEntries RPC. An empty entry list is a heartbeat.</summary>
public sealed record AppendEntries(
    long Term,
    string LeaderId,
    long PrevLogIndex,
    long PrevLogTerm,
    IReadOnlyList<LogEntry> Entries,
    long LeaderCommit,
    long Stamp)
{
    /// <summary>AppendEntries with stamp 0.</summary>
    public AppendEntries(
        long term,
        string leaderId,
        long prevLogIndex,
        long prevLogTerm,
        IReadOnlyList<LogEntry> entries,
        long leaderCommit)
        : this(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit, 0)
    {
    }

    /// <summary>True when this RPC carries no entries.</summary>
    public bool Heartbeat => Entries.Count == 0;
}

/// <summary>
/// AppendEntries result. <see cref="MatchIndex"/> is the index stored by this RPC.
/// Rejections carry fast-backup hints.
/// </summary>
public sealed record AppendEntriesResponse(
    long Term,
    bool Success,
    long MatchIndex,
    long XLen,
    long XTerm,
    long XIndex,
    long Stamp)
{
    /// <summary>Success or rejection with no fast-backup hint and stamp 0.</summary>
    public AppendEntriesResponse(long term, bool success, long matchIndex)
        : this(term, success, matchIndex, 0, 0, 0, 0)
    {
    }

    /// <summary>Success or rejection that echoes <paramref name="stamp"/> and carries no hint.</summary>
    public AppendEntriesResponse(long term, bool success, long matchIndex, long stamp)
        : this(term, success, matchIndex, 0, 0, 0, stamp)
    {
    }
}

/// <summary>InstallSnapshot RPC chunk.</summary>
public sealed class InstallSnapshot
{
    private readonly byte[] _data;

    /// <summary>Creates a snapshot chunk. <paramref name="data"/> is copied.</summary>
    public InstallSnapshot(
        long term,
        string leaderId,
        long lastIncludedIndex,
        long lastIncludedTerm,
        long offset,
        byte[] data,
        bool done)
    {
        if (string.IsNullOrWhiteSpace(leaderId))
        {
            throw new ArgumentException("leaderId must be non-blank", nameof(leaderId));
        }

        if (term < 0 || lastIncludedIndex < 1 || lastIncludedTerm < 1 || offset < 0)
        {
            throw new ArgumentException("snapshot fields must be non-negative and indexes at least 1");
        }

        ArgumentNullException.ThrowIfNull(data);
        Term = term;
        LeaderId = leaderId;
        LastIncludedIndex = lastIncludedIndex;
        LastIncludedTerm = lastIncludedTerm;
        Offset = offset;
        _data = (byte[])data.Clone();
        Done = done;
    }

    /// <summary>Leader term.</summary>
    public long Term { get; }

    /// <summary>Leader id.</summary>
    public string LeaderId { get; }

    /// <summary>Snapshot index.</summary>
    public long LastIncludedIndex { get; }

    /// <summary>Snapshot term.</summary>
    public long LastIncludedTerm { get; }

    /// <summary>Byte offset of <see cref="Data"/>.</summary>
    public long Offset { get; }

    /// <summary>A copy of the chunk bytes.</summary>
    public byte[] Data => (byte[])_data.Clone();

    /// <summary>Chunk bytes without a copy. Do not mutate.</summary>
    internal byte[] DataUnsafe => _data;

    /// <summary>True when this is the last chunk.</summary>
    public bool Done { get; }
}

/// <summary>InstallSnapshot reply. Installed index and term echo the follower's snapshot generation.</summary>
public sealed record InstallSnapshotResponse(long Term, bool Success, bool Done, long InstalledIndex, long InstalledTerm)
{
    /// <summary>In-progress or failed reply. Installed index and term are zero.</summary>
    public InstallSnapshotResponse(long term, bool success, bool done)
        : this(term, success, done, 0, 0)
    {
    }
}

/// <summary>Leadership transfer. The leader sends this, then steps down.</summary>
public sealed record TimeoutNow(long Term, string LeaderId);
