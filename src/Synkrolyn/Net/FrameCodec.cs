namespace Synkrolyn.Net;

/// <summary>4-byte big-endian length prefix plus payload.</summary>
public static class FrameCodec
{
    /// <summary>Default maximum payload size.</summary>
    public const int DefaultMaxPayloadBytes = 1_048_576;

    /// <summary>Frames <paramref name="payload"/>.</summary>
    public static byte[] Encode(byte[] payload) => Encode(payload, DefaultMaxPayloadBytes);

    /// <summary>Frames <paramref name="payload"/> when it is within <paramref name="maxPayloadBytes"/>.</summary>
    public static byte[] Encode(byte[] payload, int maxPayloadBytes)
    {
        ArgumentNullException.ThrowIfNull(payload);
        if (payload.Length > maxPayloadBytes)
        {
            throw new ArgumentException("payload larger than cap: " + payload.Length);
        }

        byte[] buf = new byte[4 + payload.Length];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buf, payload.Length);
        payload.CopyTo(buf.AsSpan(4));
        return buf;
    }

    /// <summary>Incremental frame decoder.</summary>
    public sealed class Decoder
    {
        private readonly int _maxPayloadBytes;
        private readonly List<byte> _pending = [];

        /// <summary>Uses <see cref="DefaultMaxPayloadBytes"/>.</summary>
        public Decoder()
            : this(DefaultMaxPayloadBytes)
        {
        }

        /// <summary>Caps each payload at <paramref name="maxPayloadBytes"/>.</summary>
        public Decoder(int maxPayloadBytes)
        {
            if (maxPayloadBytes < 0)
            {
                throw new ArgumentException("maxPayloadBytes must be non-negative.", nameof(maxPayloadBytes));
            }

            _maxPayloadBytes = maxPayloadBytes;
        }

        /// <summary>Pushes a socket read and returns every complete payload.</summary>
        public List<byte[]> Push(byte[] chunk)
        {
            ArgumentNullException.ThrowIfNull(chunk);
            _pending.AddRange(chunk);
            byte[] all = [.. _pending];
            int pos = 0;
            var frames = new List<byte[]>();
            while (all.Length - pos >= 4)
            {
                int length = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(all.AsSpan(pos));
                if (length < 0 || length > _maxPayloadBytes)
                {
                    throw new ArgumentException("invalid frame length: " + length);
                }

                if (all.Length - pos - 4 < length)
                {
                    break;
                }

                frames.Add(all.AsSpan(pos + 4, length).ToArray());
                pos += 4 + length;
            }

            _pending.Clear();
            if (pos < all.Length)
            {
                _pending.AddRange(all.AsSpan(pos));
            }

            return frames;
        }
    }
}
