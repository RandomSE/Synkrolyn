using System.Buffers.Binary;
using System.Text;

namespace Synkrolyn.Kv;

/// <summary>Length-prefixed UTF-8 put and delete commands. Get is never logged.</summary>
public static class KvCommandCodec
{
    private const byte PutTag = 0x01;
    private const byte DeleteTag = 0x02;

    /// <summary>Command operation.</summary>
    public enum Op
    {
        /// <summary>Put.</summary>
        Put,

        /// <summary>Delete.</summary>
        Delete,
    }

    /// <summary>Decoded command.</summary>
    public sealed record Command(Op Operation, string Key, string Value, string ClientId, long Serial);

    /// <summary>Encodes a put without a client serial.</summary>
    public static byte[] EncodePut(string key, string value) => EncodePut(key, value, "", 0);

    /// <summary>Encodes a put with an optional client serial.</summary>
    public static byte[] EncodePut(string key, string value, string clientId, long serial)
    {
        RequireKey(key);
        ArgumentNullException.ThrowIfNull(value);
        return Encode(PutTag, key, value, clientId, serial, includeValue: true);
    }

    /// <summary>Encodes a delete without a client serial.</summary>
    public static byte[] EncodeDelete(string key) => EncodeDelete(key, "", 0);

    /// <summary>Encodes a delete with an optional client serial.</summary>
    public static byte[] EncodeDelete(string key, string clientId, long serial)
    {
        RequireKey(key);
        return Encode(DeleteTag, key, "", clientId, serial, includeValue: false);
    }

    /// <summary>Decodes a put or delete.</summary>
    public static Command Decode(byte[] command)
    {
        ArgumentNullException.ThrowIfNull(command);
        if (command.Length < 1)
        {
            throw new ArgumentException("empty command");
        }

        int pos = 1;
        byte tag = command[0];
        if (tag == PutTag)
        {
            string key = ReadUtf8(command, ref pos);
            RequireKey(key);
            string value = ReadUtf8(command, ref pos);
            string clientId = "";
            long serial = 0;
            if (pos < command.Length)
            {
                clientId = ReadUtf8(command, ref pos);
                serial = ReadLong(command, ref pos);
            }

            RequireDone(command, pos);
            return new Command(Op.Put, key, value, clientId, serial);
        }

        if (tag == DeleteTag)
        {
            string key = ReadUtf8(command, ref pos);
            RequireKey(key);
            string clientId = "";
            long serial = 0;
            if (pos < command.Length)
            {
                clientId = ReadUtf8(command, ref pos);
                serial = ReadLong(command, ref pos);
            }

            RequireDone(command, pos);
            return new Command(Op.Delete, key, "", clientId, serial);
        }

        throw new ArgumentException("unknown command tag: " + tag);
    }

    /// <summary>Rejects a null or blank key.</summary>
    public static void RequireKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException("key must be non-blank");
        }
    }

    private static byte[] Encode(byte tag, string key, string value, string? clientId, long serial, bool includeValue)
    {
        byte[] keyBytes = Encoding.UTF8.GetBytes(key);
        byte[] valueBytes = includeValue ? Encoding.UTF8.GetBytes(value) : [];
        byte[] clientBytes = string.IsNullOrEmpty(clientId) ? [] : Encoding.UTF8.GetBytes(clientId);
        bool withSerial = serial != 0 || clientBytes.Length > 0;
        int size = 1 + 4 + keyBytes.Length + (includeValue ? 4 + valueBytes.Length : 0);
        if (withSerial)
        {
            size += 4 + clientBytes.Length + 8;
        }

        byte[] buf = new byte[size];
        int pos = 0;
        buf[pos++] = tag;
        WriteBytes(buf, ref pos, keyBytes);
        if (includeValue)
        {
            WriteBytes(buf, ref pos, valueBytes);
        }

        if (withSerial)
        {
            WriteBytes(buf, ref pos, clientBytes);
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), serial);
        }

        return buf;
    }

    private static void WriteBytes(byte[] buf, ref int pos, byte[] bytes)
    {
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(pos), bytes.Length);
        pos += 4;
        bytes.CopyTo(buf.AsSpan(pos));
        pos += bytes.Length;
    }

    private static string ReadUtf8(byte[] buf, ref int pos)
    {
        if (buf.Length - pos < 4)
        {
            throw new ArgumentException("truncated length prefix");
        }

        int len = BinaryPrimitives.ReadInt32BigEndian(buf.AsSpan(pos));
        pos += 4;
        if (len < 0 || buf.Length - pos < len)
        {
            throw new ArgumentException("truncated string payload");
        }

        string text = Encoding.UTF8.GetString(buf, pos, len);
        pos += len;
        return text;
    }

    private static long ReadLong(byte[] buf, ref int pos)
    {
        if (buf.Length - pos < 8)
        {
            throw new ArgumentException("truncated serial");
        }

        long value = BinaryPrimitives.ReadInt64BigEndian(buf.AsSpan(pos));
        pos += 8;
        return value;
    }

    private static void RequireDone(byte[] buf, int pos)
    {
        if (pos != buf.Length)
        {
            throw new ArgumentException("trailing bytes after command");
        }
    }
}
