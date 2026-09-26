using System.Buffers.Binary;
using System.Text;

namespace Synkrolyn.Kv;

/// <summary>
/// Key-diff snapshot. Full snapshots are PTCS. New deltas append a full serial table.
/// A delta without a trailer keeps the base serials. A present trailer replaces them.
/// </summary>
public static class KvSnapshotDelta
{
    private static readonly byte[] Magic = "PTKV"u8.ToArray();
    private const byte TypeDelta = 2;

    /// <summary>True when the bytes are a PTKV delta.</summary>
    public static bool IsDelta(byte[]? snapshot) =>
        snapshot is not null && snapshot.Length >= Magic.Length + 1 && Starts(snapshot);

    /// <summary>Base index stored in a delta.</summary>
    public static long BaseIndex(byte[] snapshot)
    {
        if (!IsDelta(snapshot))
        {
            throw new ArgumentException("not a delta snapshot");
        }

        return BinaryPrimitives.ReadInt64BigEndian(snapshot.AsSpan(Magic.Length + 1));
    }

    /// <summary>Encodes a delta. <paramref name="serials"/> is written in full.</summary>
    public static byte[] Encode(long baseIndex, IReadOnlyDictionary<string, string> puts, IReadOnlyList<string> deletes, IReadOnlyDictionary<string, long> serials)
    {
        ArgumentNullException.ThrowIfNull(serials);
        byte[] mapOnly = EncodeMapOnly(baseIndex, puts, deletes);
        var clients = serials.Keys.OrderBy(static id => id, StringComparer.Ordinal).ToList();
        int serialSize = 4;
        var ids = new List<byte[]>(clients.Count);
        foreach (string id in clients)
        {
            byte[] idBytes = Encoding.UTF8.GetBytes(id);
            ids.Add(idBytes);
            serialSize += 4 + idBytes.Length + 8;
        }

        byte[] buf = new byte[mapOnly.Length + serialSize];
        mapOnly.CopyTo(buf);
        int pos = mapOnly.Length;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(pos), clients.Count);
        pos += 4;
        for (int i = 0; i < clients.Count; i++)
        {
            WriteBytes(buf, ref pos, ids[i]);
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), serials[clients[i]]);
            pos += 8;
        }

        return buf;
    }

    /// <summary>Applies puts, deletes, and an optional serial trailer onto the base maps.</summary>
    public static void ApplyTo(Dictionary<string, string> map, Dictionary<string, long> serials, byte[] snapshot)
    {
        ArgumentNullException.ThrowIfNull(serials);
        int pos = Magic.Length + 1 + 8;
        int putCount = BinaryPrimitives.ReadInt32BigEndian(snapshot.AsSpan(pos));
        pos += 4;
        if (putCount < 0)
        {
            throw new ArgumentException("negative delta put count");
        }

        for (int i = 0; i < putCount; i++)
        {
            string key = ReadUtf8(snapshot, ref pos);
            string value = ReadUtf8(snapshot, ref pos);
            map[key] = value;
        }

        int delCount = BinaryPrimitives.ReadInt32BigEndian(snapshot.AsSpan(pos));
        pos += 4;
        if (delCount < 0)
        {
            throw new ArgumentException("negative delta delete count");
        }

        for (int i = 0; i < delCount; i++)
        {
            map.Remove(ReadUtf8(snapshot, ref pos));
        }

        if (pos == snapshot.Length)
        {
            return;
        }

        if (snapshot.Length - pos < 4)
        {
            throw new ArgumentException("truncated delta serial table");
        }

        int serialCount = BinaryPrimitives.ReadInt32BigEndian(snapshot.AsSpan(pos));
        pos += 4;
        if (serialCount < 0)
        {
            throw new ArgumentException("negative delta serial count");
        }

        serials.Clear();
        for (int i = 0; i < serialCount; i++)
        {
            string clientId = ReadUtf8(snapshot, ref pos);
            if (snapshot.Length - pos < 8)
            {
                throw new ArgumentException("truncated delta serial");
            }

            serials[clientId] = BinaryPrimitives.ReadInt64BigEndian(snapshot.AsSpan(pos));
            pos += 8;
        }

        if (pos != snapshot.Length)
        {
            throw new ArgumentException("trailing bytes after delta serial table");
        }
    }

    private static byte[] EncodeMapOnly(long baseIndex, IReadOnlyDictionary<string, string> puts, IReadOnlyList<string> deletes)
    {
        ArgumentNullException.ThrowIfNull(puts);
        ArgumentNullException.ThrowIfNull(deletes);
        int size = Magic.Length + 1 + 8 + 4 + 4;
        var encoded = new List<byte[]>();
        foreach (KeyValuePair<string, string> pair in puts)
        {
            byte[] k = Encoding.UTF8.GetBytes(pair.Key);
            byte[] v = Encoding.UTF8.GetBytes(pair.Value);
            encoded.Add(k);
            encoded.Add(v);
            size += 8 + k.Length + v.Length;
        }

        var del = new List<byte[]>();
        foreach (string key in deletes)
        {
            byte[] k = Encoding.UTF8.GetBytes(key);
            del.Add(k);
            size += 4 + k.Length;
        }

        byte[] buf = new byte[size];
        Magic.CopyTo(buf);
        int pos = Magic.Length;
        buf[pos++] = TypeDelta;
        BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), baseIndex);
        pos += 8;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(pos), puts.Count);
        pos += 4;
        foreach (byte[] bytes in encoded)
        {
            WriteBytes(buf, ref pos, bytes);
        }

        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(pos), del.Count);
        pos += 4;
        foreach (byte[] bytes in del)
        {
            WriteBytes(buf, ref pos, bytes);
        }

        return buf;
    }

    private static bool Starts(byte[] snapshot)
    {
        for (int i = 0; i < Magic.Length; i++)
        {
            if (snapshot[i] != Magic[i])
            {
                return false;
            }
        }

        return snapshot[Magic.Length] == TypeDelta;
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
            throw new ArgumentException("truncated delta string");
        }

        int len = BinaryPrimitives.ReadInt32BigEndian(buf.AsSpan(pos));
        pos += 4;
        if (len < 0 || buf.Length - pos < len)
        {
            throw new ArgumentException("truncated delta payload");
        }

        string text = Encoding.UTF8.GetString(buf, pos, len);
        pos += len;
        return text;
    }
}
