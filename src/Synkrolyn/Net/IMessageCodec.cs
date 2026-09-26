namespace Synkrolyn.Net;

/// <summary>Binary encoding of Raft RPC records. Not a language serializer.</summary>
public interface IMessageCodec
{
    /// <summary>Decoded sender and payload.</summary>
    public readonly record struct Decoded(string From, object Payload);

    /// <summary>Encodes <paramref name="payload"/> with the sender id.</summary>
    byte[] Encode(string from, object payload);

    /// <summary>Decodes one frame payload.</summary>
    Decoded Decode(byte[] payload);
}
