using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Text;
using Synkrolyn.Raft;

namespace Synkrolyn.Kv;

/// <summary>
/// Applied-local map. <see cref="Get"/> reads this replica and is not linearizable.
/// Client serials survive PTCS snapshots.
/// </summary>
public sealed class InMemoryKvStore : IKvStore
{
    private static readonly byte[] SerialMagic = "PTCS"u8.ToArray();
    private const int SerialSnapshotVersion = 1;
    private readonly object _gate = new();
    private ImmutableDictionary<string, string> _map = ImmutableDictionary<string, string>.Empty;
    private ImmutableDictionary<string, long> _clientSerials = ImmutableDictionary<string, long>.Empty;
    private ImmutableDictionary<string, string>? _lastSnapMap;
    private int _epoch;
    private long _appliedThrough = -1;

    /// <summary>Runs while snapshot bytes are encoded. Tests block here. Production leaves it null.</summary>
    internal Action? WhileEncoding { get; set; }

    /// <summary>
    /// Runs once in the gap after the epoch is sampled and before the mutation is
    /// published. Tests restore or snapshot there. Production leaves it null.
    /// </summary>
    internal Action? AfterEpochSample { get; set; }

    /// <inheritdoc />
    public void Apply(long index, byte[] command)
    {
        ArgumentNullException.ThrowIfNull(command);
        if (command.Length == 0)
        {
            return;
        }

        KvCommandCodec.Command decoded = KvCommandCodec.Decode(command);
        while (true)
        {
            int seen;
            lock (_gate)
            {
                seen = _epoch;
            }

            Action? overlap = AfterEpochSample;
            AfterEpochSample = null;
            overlap?.Invoke();
            lock (_gate)
            {
                if (seen != _epoch)
                {
                    continue;
                }

                if (index <= _appliedThrough)
                {
                    return;
                }

                if (decoded.Serial > 0 && decoded.ClientId.Length > 0)
                {
                    if (_clientSerials.TryGetValue(decoded.ClientId, out long last) && decoded.Serial <= last)
                    {
                        return;
                    }

                    _clientSerials = _clientSerials.SetItem(decoded.ClientId, decoded.Serial);
                }

                if (decoded.Operation == KvCommandCodec.Op.Put)
                {
                    _map = _map.SetItem(decoded.Key, decoded.Value);
                }
                else
                {
                    _map = _map.Remove(decoded.Key);
                }

                return;
            }
        }
    }

    /// <inheritdoc />
    public byte[] Snapshot()
    {
        lock (_gate)
        {
            WhileEncoding?.Invoke();
            byte[] bytes = Encode(_map, _clientSerials);
            _lastSnapMap = _map;
            return bytes;
        }
    }

    /// <inheritdoc />
    public StateCapture CaptureState()
    {
        lock (_gate)
        {
            ImmutableDictionary<string, string> map = _map;
            ImmutableDictionary<string, long> serials = _clientSerials;
            ImmutableDictionary<string, string>? baseline = _lastSnapMap;
            return StateCapture.Frozen(
                () =>
                {
                    WhileEncoding?.Invoke();
                    return Encode(map, serials);
                },
                baseIndex => EncodeDelta(map, serials, baseline, baseIndex),
                () =>
                {
                    lock (_gate)
                    {
                        _lastSnapMap = map;
                    }
                });
        }
    }

    /// <summary>Bytes for disk persist. Does not move the delta base.</summary>
    public byte[] EncodeMapForPersist()
    {
        lock (_gate)
        {
            return Encode(_map, _clientSerials);
        }
    }

    /// <inheritdoc />
    public byte[] SnapshotDelta(long baseIndex)
    {
        lock (_gate)
        {
            return EncodeDelta(_map, _clientSerials, _lastSnapMap, baseIndex);
        }
    }

    private static byte[] EncodeDelta(
        ImmutableDictionary<string, string> map,
        ImmutableDictionary<string, long> serials,
        ImmutableDictionary<string, string>? baseline,
        long baseIndex)
    {
        if (baseline is null)
        {
            return Encode(map, serials);
        }

        var puts = new Dictionary<string, string>();
        var deletes = new List<string>();
        foreach ((string key, string value) in map)
        {
            if (!baseline.TryGetValue(key, out string? prev) || prev != value)
            {
                puts[key] = value;
            }
        }

        foreach (string key in baseline.Keys)
        {
            if (!map.ContainsKey(key))
            {
                deletes.Add(key);
            }
        }

        return KvSnapshotDelta.Encode(baseIndex, puts, deletes, serials);
    }

    /// <inheritdoc />
    public void Restore(byte[] snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        lock (_gate)
        {
            _epoch++;
            if (KvSnapshotDelta.IsDelta(snapshot))
            {
                throw new ArgumentException("delta snapshot requires a base");
            }

            RestoreFull(snapshot);
        }
    }

    /// <inheritdoc />
    public void RestoreChecked(long lastIncludedIndex, long lastApplied, byte[] snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        lock (_gate)
        {
            _epoch++;
            _appliedThrough = Math.Max(_appliedThrough, lastIncludedIndex);
            RestoreCheckedLocked(lastIncludedIndex, lastApplied, snapshot);
        }
    }

    private void RestoreCheckedLocked(long lastIncludedIndex, long lastApplied, byte[] snapshot)
    {
        if (KvSnapshotDelta.IsDelta(snapshot))
        {
            if (_lastSnapMap is null
                || KvSnapshotDelta.BaseIndex(snapshot) != lastIncludedIndex
                || lastApplied != lastIncludedIndex
                || !MapsEqual(_map, _lastSnapMap))
            {
                throw new ArgumentException("delta snapshot requires a matching base");
            }

            var map = _map.ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);
            var serials = _clientSerials.ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);
            KvSnapshotDelta.ApplyTo(map, serials, snapshot);
            _map = map.ToImmutableDictionary();
            _clientSerials = serials.ToImmutableDictionary();
            _lastSnapMap = _map;
            return;
        }

        RestoreFull(snapshot);
    }

    /// <inheritdoc />
    public string? Get(string key)
    {
        KvCommandCodec.RequireKey(key);
        lock (_gate)
        {
            return _map.GetValueOrDefault(key);
        }
    }

    private static byte[] Encode(ImmutableDictionary<string, string> map, ImmutableDictionary<string, long> serials)
    {
        byte[] mapBytes = EncodeMapOnly(map);
        var clients = serials.Keys.OrderBy(static id => id, StringComparer.Ordinal).ToList();
        int serialSize = 4;
        var ids = new List<byte[]>(clients.Count);
        foreach (string id in clients)
        {
            byte[] idBytes = Encoding.UTF8.GetBytes(id);
            ids.Add(idBytes);
            serialSize += 4 + idBytes.Length + 8;
        }

        byte[] buf = new byte[SerialMagic.Length + 4 + mapBytes.Length + serialSize];
        SerialMagic.CopyTo(buf);
        int pos = SerialMagic.Length;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(pos), SerialSnapshotVersion);
        pos += 4;
        mapBytes.CopyTo(buf.AsSpan(pos));
        pos += mapBytes.Length;
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

    private static byte[] EncodeMapOnly(ImmutableDictionary<string, string> map)
    {
        var keys = map.Keys.OrderBy(static key => key, StringComparer.Ordinal).ToList();
        int size = 4;
        var encoded = new List<byte[]>(keys.Count * 2);
        foreach (string key in keys)
        {
            byte[] keyBytes = Encoding.UTF8.GetBytes(key);
            byte[] valueBytes = Encoding.UTF8.GetBytes(map[key]);
            encoded.Add(keyBytes);
            encoded.Add(valueBytes);
            size += 8 + keyBytes.Length + valueBytes.Length;
        }

        byte[] buf = new byte[size];
        BinaryPrimitives.WriteInt32BigEndian(buf, keys.Count);
        int pos = 4;
        foreach (byte[] bytes in encoded)
        {
            WriteBytes(buf, ref pos, bytes);
        }

        return buf;
    }

    private void RestoreFull(byte[] snapshot)
    {
        int pos = 0;
        if (IsPtcs(snapshot))
        {
            pos = SerialMagic.Length;
            int version = BinaryPrimitives.ReadInt32BigEndian(snapshot.AsSpan(pos));
            pos += 4;
            if (version != SerialSnapshotVersion)
            {
                throw new ArgumentException("unsupported kv snapshot version " + version);
            }

            RestoreMap(snapshot, ref pos);
            RestoreSerials(snapshot, ref pos);
        }
        else
        {
            RestoreMap(snapshot, ref pos);
            _clientSerials = ImmutableDictionary<string, long>.Empty;
            if (pos != snapshot.Length)
            {
                throw new ArgumentException("trailing bytes after kv snapshot");
            }
        }

        _lastSnapMap = _map;
    }

    private void RestoreMap(byte[] snapshot, ref int pos)
    {
        if (snapshot.Length - pos < 4)
        {
            throw new ArgumentException("truncated kv snapshot");
        }

        int count = BinaryPrimitives.ReadInt32BigEndian(snapshot.AsSpan(pos));
        pos += 4;
        if (count < 0)
        {
            throw new ArgumentException("negative kv snapshot count");
        }

        var restored = ImmutableDictionary.CreateBuilder<string, string>();
        for (int i = 0; i < count; i++)
        {
            string key = ReadUtf8(snapshot, ref pos);
            string value = ReadUtf8(snapshot, ref pos);
            restored[key] = value;
        }

        _map = restored.ToImmutable();
    }

    private void RestoreSerials(byte[] snapshot, ref int pos)
    {
        if (snapshot.Length - pos < 4)
        {
            throw new ArgumentException("truncated kv serial table");
        }

        int count = BinaryPrimitives.ReadInt32BigEndian(snapshot.AsSpan(pos));
        pos += 4;
        if (count < 0)
        {
            throw new ArgumentException("negative kv serial count");
        }

        var restored = ImmutableDictionary.CreateBuilder<string, long>();
        for (int i = 0; i < count; i++)
        {
            string clientId = ReadUtf8(snapshot, ref pos);
            if (snapshot.Length - pos < 8)
            {
                throw new ArgumentException("truncated kv serial");
            }

            restored[clientId] = BinaryPrimitives.ReadInt64BigEndian(snapshot.AsSpan(pos));
            pos += 8;
        }

        _clientSerials = restored.ToImmutable();

        if (pos != snapshot.Length)
        {
            throw new ArgumentException("trailing bytes after kv serial table");
        }
    }

    private static bool IsPtcs(byte[] snapshot)
    {
        if (snapshot.Length < SerialMagic.Length + 4)
        {
            return false;
        }

        for (int i = 0; i < SerialMagic.Length; i++)
        {
            if (snapshot[i] != SerialMagic[i])
            {
                return false;
            }
        }

        return true;
    }

    private static bool MapsEqual(ImmutableDictionary<string, string> left, ImmutableDictionary<string, string> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        foreach ((string key, string value) in left)
        {
            if (!right.TryGetValue(key, out string? other) || other != value)
            {
                return false;
            }
        }

        return true;
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
            throw new ArgumentException("truncated kv snapshot string");
        }

        int len = BinaryPrimitives.ReadInt32BigEndian(buf.AsSpan(pos));
        pos += 4;
        if (len < 0 || buf.Length - pos < len)
        {
            throw new ArgumentException("truncated kv snapshot payload");
        }

        string text = Encoding.UTF8.GetString(buf, pos, len);
        pos += len;
        return text;
    }
}
