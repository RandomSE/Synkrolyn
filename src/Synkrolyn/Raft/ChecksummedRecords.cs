using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;

namespace Synkrolyn.Raft;

/// <summary>Length-prefixed payload plus CRC32. Shared by the file log and hard state.</summary>
internal static class ChecksummedRecords
{
    public const int MaxPayloadBytes = 1_048_576;

    public static byte[] Frame(byte[] payload)
    {
        if (payload.Length > MaxPayloadBytes)
        {
            throw new ArgumentException("payload larger than cap: " + payload.Length);
        }

        byte[] buf = new byte[4 + payload.Length + 4];
        BinaryPrimitives.WriteInt32BigEndian(buf, payload.Length);
        payload.CopyTo(buf.AsSpan(4));
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(4 + payload.Length), Crc32(payload));
        return buf;
    }

    public static int Crc32(byte[] payload)
    {
        uint value = System.IO.Hashing.Crc32.HashToUInt32(payload);
        return unchecked((int)value);
    }

    public static byte[] Unframe(byte[] framed)
    {
        if (framed.Length < 8)
        {
            throw new InvalidOperationException("incomplete checksummed record");
        }

        int length = BinaryPrimitives.ReadInt32BigEndian(framed);
        if (length < 0 || length > MaxPayloadBytes)
        {
            throw new InvalidOperationException("invalid record length: " + length);
        }

        if (framed.Length < 4 + length + 4)
        {
            throw new InvalidOperationException("incomplete checksummed record");
        }

        if (framed.Length != 4 + length + 4)
        {
            throw new InvalidOperationException("trailing bytes after checksummed record");
        }

        byte[] payload = framed.AsSpan(4, length).ToArray();
        int crc = BinaryPrimitives.ReadInt32BigEndian(framed.AsSpan(4 + length));
        if (crc != Crc32(payload))
        {
            throw new InvalidOperationException("checksum mismatch");
        }

        return payload;
    }
}

/// <summary>Big-endian writers and readers used by durable records and RPC frames.</summary>
internal static class Be
{
    public static void WriteInt(Stream stream, int value)
    {
        Span<byte> buf = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(buf, value);
        stream.Write(buf);
    }

    public static void WriteLong(Stream stream, long value)
    {
        Span<byte> buf = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(buf, value);
        stream.Write(buf);
    }

    public static int ReadInt(ReadOnlySpan<byte> span) => BinaryPrimitives.ReadInt32BigEndian(span);

    public static long ReadLong(ReadOnlySpan<byte> span) => BinaryPrimitives.ReadInt64BigEndian(span);

    public static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    public static string Utf8(ReadOnlySpan<byte> bytes) => Encoding.UTF8.GetString(bytes);
}
