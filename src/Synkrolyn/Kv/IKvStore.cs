namespace Synkrolyn.Kv;

/// <summary>Applied KV map. <see cref="Get"/> is local and is not linearizable.</summary>
public interface IKvStore : Raft.IStateMachine
{
    /// <summary>Local applied value, or null when missing.</summary>
#pragma warning disable CA1716 // Get is the KV read name used by clients; the VB keyword collision is accepted.
    string? Get(string key);

    /// <summary>Local read for a live session. An unknown or expired session throws.</summary>
    string? GetForSession(long clientId, string key);
#pragma warning restore CA1716
}
