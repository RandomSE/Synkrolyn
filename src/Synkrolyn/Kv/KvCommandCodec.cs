using System.Buffers.Binary;
using System.Text;

namespace Synkrolyn.Kv;

/// <summary>Length-prefixed UTF-8 put and delete commands. Get is never logged.</summary>
public static class KvCommandCodec
{
    private const byte PutTag = 0x01;
    private const byte DeleteTag = 0x02;
    private const byte RegisterTag = 0x03;
    private const byte SessionPutTag = 0x04;
    private const byte SessionDeleteTag = 0x05;
    private const byte TickTag = 0x06;

    /// <summary>Command operation.</summary>
    public enum Op
    {
        /// <summary>Put.</summary>
        Put,

        /// <summary>Delete.</summary>
        Delete,

        /// <summary>Open a client session. The log index of this command is the client id.</summary>
        Register,

        /// <summary>Leader timestamp with no map change. Session expiry reads this, not a local clock.</summary>
        Tick,
    }

    /// <summary>Decoded command. Session fields are zero on a put or delete that has no session.</summary>
    public sealed record Command(
        Op Operation,
        string Key,
        string Value,
        string ClientId,
        long Serial,
        long SessionId = 0,
        long AckFloor = 0,
        long LeaderTimestamp = 0,
        long TimeoutMillis = 0);

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

    /// <summary>Encodes a session registration. The applying index becomes the client id.</summary>
    public static byte[] EncodeRegister(long leaderTimestampMillis, long timeoutMillis)
    {
        byte[] buf = new byte[1 + 16];
        buf[0] = RegisterTag;
        BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(1), leaderTimestampMillis);
        BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(9), timeoutMillis);
        return buf;
    }

    /// <summary>Encodes a session put. A duplicate sequence returns the cached response.</summary>
    public static byte[] EncodeSessionPut(
        string key,
        string value,
        long clientId,
        long seq,
        long ackFloor,
        long leaderTimestampMillis)
    {
        RequireKey(key);
        ArgumentNullException.ThrowIfNull(value);
        return EncodeSession(SessionPutTag, key, value, clientId, seq, ackFloor, leaderTimestampMillis, includeValue: true);
    }

    /// <summary>Encodes a session delete.</summary>
    public static byte[] EncodeSessionDelete(string key, long clientId, long seq, long ackFloor, long leaderTimestampMillis)
    {
        RequireKey(key);
        return EncodeSession(SessionDeleteTag, key, "", clientId, seq, ackFloor, leaderTimestampMillis, includeValue: false);
    }

    /// <summary>Encodes a leader timestamp so every replica expires sessions from the same entry.</summary>
    public static byte[] EncodeTick(long leaderTimestampMillis)
    {
        byte[] buf = new byte[1 + 8];
        buf[0] = TickTag;
        BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(1), leaderTimestampMillis);
        return buf;
    }

    /// <summary>Decodes a put, delete, registration, session command, or tick.</summary>
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

        if (tag == RegisterTag)
        {
            long timestamp = ReadLong(command, ref pos);
            long timeout = ReadLong(command, ref pos);
            RequireDone(command, pos);
            return new Command(Op.Register, "", "", "", 0, 0, 0, timestamp, timeout);
        }

        if (tag == TickTag)
        {
            long timestamp = ReadLong(command, ref pos);
            RequireDone(command, pos);
            return new Command(Op.Tick, "", "", "", 0, 0, 0, timestamp);
        }

        if (tag is SessionPutTag or SessionDeleteTag)
        {
            string key = ReadUtf8(command, ref pos);
            RequireKey(key);
            string value = "";
            if (tag == SessionPutTag)
            {
                value = ReadUtf8(command, ref pos);
            }

            long sessionId = ReadLong(command, ref pos);
            long seq = ReadLong(command, ref pos);
            long ackFloor = ReadLong(command, ref pos);
            long timestamp = ReadLong(command, ref pos);
            RequireDone(command, pos);
            Op operation = tag == SessionPutTag ? Op.Put : Op.Delete;
            return new Command(operation, key, value, "", seq, sessionId, ackFloor, timestamp);
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

    private static byte[] EncodeSession(
        byte tag,
        string key,
        string value,
        long clientId,
        long seq,
        long ackFloor,
        long leaderTimestampMillis,
        bool includeValue)
    {
        byte[] keyBytes = Encoding.UTF8.GetBytes(key);
        byte[] valueBytes = includeValue ? Encoding.UTF8.GetBytes(value) : [];
        int size = 1 + 4 + keyBytes.Length + (includeValue ? 4 + valueBytes.Length : 0) + 32;
        byte[] buf = new byte[size];
        int pos = 0;
        buf[pos++] = tag;
        WriteBytes(buf, ref pos, keyBytes);
        if (includeValue)
        {
            WriteBytes(buf, ref pos, valueBytes);
        }

        BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), clientId);
        pos += 8;
        BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), seq);
        pos += 8;
        BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), ackFloor);
        pos += 8;
        BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), leaderTimestampMillis);
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
