namespace Synkrolyn.Net;

/// <summary>
/// Addressable send path. Implementations must not start a thread per RPC.
/// Delivery is driven by the caller, and for delayed in-memory messages by a clock advance.
/// </summary>
public interface ITransport
{
    /// <summary>Sends <paramref name="payload"/> from <paramref name="sender"/> to <paramref name="recipient"/>.</summary>
    void Send(string sender, string recipient, object payload);

    /// <summary>True when <paramref name="nodeId"/> is a known peer. The default accepts every id.</summary>
    bool HasPeer(string nodeId) => true;
}
