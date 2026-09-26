namespace Synkrolyn.Raft;

/// <summary>Raft server role.</summary>
public enum Role
{
    /// <summary>Follower.</summary>
    Follower,

    /// <summary>Candidate.</summary>
    Candidate,

    /// <summary>Leader.</summary>
    Leader,
}

/// <summary>
/// Leadership transfer progress. Success is reported only after the target is observed as leader.
/// </summary>
public enum LeadershipTransferStatus
{
    /// <summary>No transfer is in progress or recorded.</summary>
    None,

    /// <summary>Waiting for the target's match index to reach the leader's last index.</summary>
    CatchingUp,

    /// <summary>TimeoutNow was sent and this node is a follower waiting to see the target win.</summary>
    AwaitingWinner,

    /// <summary>An AppendEntries from the target was observed.</summary>
    Succeeded,

    /// <summary>The wait expired or a different leader appeared. Pending state is cleared.</summary>
    Aborted,
}
