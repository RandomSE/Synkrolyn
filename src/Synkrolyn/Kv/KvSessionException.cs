namespace Synkrolyn.Kv;

/// <summary>A client session is missing or has expired from the replicated log time.</summary>
public sealed class KvSessionException : Exception
{
    /// <summary>Creates an exception whose message names the session failure.</summary>
    public KvSessionException(string message)
        : base(message)
    {
    }
}
