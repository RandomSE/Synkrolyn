namespace Synkrolyn.Kv;

/// <summary>Outcome of one session command. <see cref="None"/> means it applied or a duplicate hit the cache.</summary>
public enum KvCommandError
{
    /// <summary>The command applied, or a duplicate returned the cached response.</summary>
    None = 0,

    /// <summary>The session is missing or past its timeout. The command did not apply.</summary>
    SessionExpired = 1,

    /// <summary>The sequence is above the next expected value. The command did not apply.</summary>
    OutOfOrder = 2,
}

/// <summary>Last session command that produced a result. Empty log entries leave the previous value in place.</summary>
internal readonly record struct KvCommandResult(long Index, KvCommandError Error, string? CachedResponse);
