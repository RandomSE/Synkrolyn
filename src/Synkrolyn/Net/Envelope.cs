namespace Synkrolyn.Net;

/// <summary>One addressed message. Payload is the RPC object, not a serialized frame.</summary>
public sealed class Envelope
{
    /// <summary>Validates sender, recipient, and payload.</summary>
    public Envelope(string from, string to, object payload)
    {
        if (string.IsNullOrWhiteSpace(from))
        {
            throw new ArgumentException("from must be non-blank", nameof(from));
        }

        if (string.IsNullOrWhiteSpace(to))
        {
            throw new ArgumentException("to must be non-blank", nameof(to));
        }

        ArgumentNullException.ThrowIfNull(payload);
        From = from;
        To = to;
        Payload = payload;
    }

    /// <summary>Sender node id.</summary>
    public string From { get; }

    /// <summary>Recipient node id.</summary>
    public string To { get; }

    /// <summary>RPC payload.</summary>
    public object Payload { get; }
}
