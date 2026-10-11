using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Text;
using Synkrolyn.Raft;

namespace Synkrolyn.Kv;

/// <summary>
/// Applied-local map. <see cref="Get"/> reads this replica and is not linearizable.
/// Client serials and sessions survive PTCS snapshots. Session expiry uses the
/// leader timestamp stored in the log, never a local clock.
/// </summary>
public sealed class InMemoryKvStore : IKvStore
{
    private static readonly byte[] SerialMagic = "PTCS"u8.ToArray();
    private const int SerialSnapshotVersion = 1;
    private readonly object _gate = new();
    private ImmutableDictionary<string, string> _map = ImmutableDictionary<string, string>.Empty;
    private ImmutableDictionary<string, long> _clientSerials = ImmutableDictionary<string, long>.Empty;
    private ImmutableDictionary<long, Session> _sessions = ImmutableDictionary<long, Session>.Empty;
    private long _logTime;
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
            lock (_gate)
            {
                if (index > _appliedThrough)
                {
                    _appliedThrough = index;
                }
            }

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

                if (ApplySession(index, decoded))
                {
                    return;
                }

                if (decoded.Serial > 0 && decoded.ClientId.Length > 0)
                {
                    if (_clientSerials.TryGetValue(decoded.ClientId, out long last) && decoded.Serial <= last)
                    {
                        _appliedThrough = Math.Max(_appliedThrough, index);
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

                _appliedThrough = Math.Max(_appliedThrough, index);
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
            byte[] bytes = Encode(_map, _clientSerials, _sessions, _logTime);
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
            ImmutableDictionary<long, Session> sessions = _sessions;
            long logTime = _logTime;
            ImmutableDictionary<string, string>? baseline = _lastSnapMap;
            return StateCapture.Frozen(
                () =>
                {
                    WhileEncoding?.Invoke();
                    return Encode(map, serials, sessions, logTime);
                },
                baseIndex => EncodeDelta(map, serials, sessions, logTime, baseline, baseIndex),
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
            return Encode(_map, _clientSerials, _sessions, _logTime);
        }
    }

    /// <inheritdoc />
    public byte[] SnapshotDelta(long baseIndex)
    {
        lock (_gate)
        {
            return EncodeDelta(_map, _clientSerials, _sessions, _logTime, _lastSnapMap, baseIndex);
        }
    }

    private static byte[] EncodeDelta(
        ImmutableDictionary<string, string> map,
        ImmutableDictionary<string, long> serials,
        ImmutableDictionary<long, Session> sessions,
        long logTime,
        ImmutableDictionary<string, string>? baseline,
        long baseIndex)
    {
        if (baseline is null)
        {
            return Encode(map, serials, sessions, logTime);
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

        byte[] delta = KvSnapshotDelta.Encode(baseIndex, puts, deletes, serials);
        if (sessions.IsEmpty && logTime == 0)
        {
            return delta;
        }

        byte[] tail = EncodeSessionTail(sessions, logTime);
        byte[] buf = new byte[delta.Length + tail.Length];
        delta.CopyTo(buf);
        tail.CopyTo(buf.AsSpan(delta.Length));
        return buf;
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
            int pos = KvSnapshotDelta.ApplyTo(map, serials, snapshot);
            _map = map.ToImmutableDictionary();
            _clientSerials = serials.ToImmutableDictionary();
            if (pos < snapshot.Length)
            {
                RestoreSessionTail(snapshot, ref pos);
            }

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

    /// <inheritdoc />
    public string? GetForSession(long clientId, string key)
    {
        KvCommandCodec.RequireKey(key);
        lock (_gate)
        {
            if (!_sessions.TryGetValue(clientId, out Session? session))
            {
                throw new KvSessionException("unknown session " + clientId);
            }

            if (IsExpired(session))
            {
                throw new KvSessionException("session expired");
            }

            return _map.GetValueOrDefault(key);
        }
    }

    /// <summary>Session fields replicated through the log. Expired is derived from log time.</summary>
    internal readonly record struct SessionView(
        long ClientId,
        long LastSeq,
        long AckFloor,
        long LastActiveMillis,
        long TimeoutMillis,
        bool Expired,
        long LogTimeMillis);

    /// <summary>Reads one session. Throws when the id was never registered in this store.</summary>
    internal SessionView ReadSession(long clientId)
    {
        lock (_gate)
        {
            if (!_sessions.TryGetValue(clientId, out Session? session))
            {
                throw new KvSessionException("unknown session " + clientId);
            }

            return new SessionView(
                session.Id,
                session.LastSeq,
                session.AckFloor,
                session.LastActive,
                session.TimeoutMillis,
                IsExpired(session),
                _logTime);
        }
    }

    private static byte[] Encode(
        ImmutableDictionary<string, string> map,
        ImmutableDictionary<string, long> serials,
        ImmutableDictionary<long, Session> sessions,
        long logTime)
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

        if (sessions.IsEmpty && logTime == 0)
        {
            return buf;
        }

        byte[] tail = EncodeSessionTail(sessions, logTime);
        byte[] withSessions = new byte[buf.Length + tail.Length];
        buf.CopyTo(withSessions);
        BinaryPrimitives.WriteInt32BigEndian(withSessions.AsSpan(SerialMagic.Length), 2);
        tail.CopyTo(withSessions.AsSpan(buf.Length));
        return withSessions;
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
            if (version is not SerialSnapshotVersion and not 2)
            {
                throw new ArgumentException("unsupported kv snapshot version " + version);
            }

            RestoreMap(snapshot, ref pos);
            RestoreSerials(snapshot, ref pos);
            if (version == SerialSnapshotVersion)
            {
                if (pos != snapshot.Length)
                {
                    throw new ArgumentException("trailing bytes after kv serial table");
                }

                _sessions = ImmutableDictionary<long, Session>.Empty;
                _logTime = 0;
            }
            else
            {
                RestoreSessionTail(snapshot, ref pos);
            }
        }
        else
        {
            RestoreMap(snapshot, ref pos);
            _clientSerials = ImmutableDictionary<string, long>.Empty;
            _sessions = ImmutableDictionary<long, Session>.Empty;
            _logTime = 0;
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
    }

    /// <summary>
    /// Session register, tick, and sequenced commands. One dictionary lookup on the
    /// steady-state put and delete path. Returns false for an ordinary command.
    /// </summary>
    private bool ApplySession(long index, KvCommandCodec.Command decoded)
    {
        if (decoded.Operation == KvCommandCodec.Op.Tick)
        {
            NoteTime(decoded.LeaderTimestamp);
            _appliedThrough = Math.Max(_appliedThrough, index);
            return true;
        }

        if (decoded.Operation == KvCommandCodec.Op.Register)
        {
            NoteTime(decoded.LeaderTimestamp);
            if (!_sessions.ContainsKey(index))
            {
                _sessions = _sessions.SetItem(
                    index,
                    new Session(index, 0, 0, decoded.LeaderTimestamp, decoded.TimeoutMillis, false, "", ""));
            }

            _appliedThrough = Math.Max(_appliedThrough, index);
            return true;
        }

        if (decoded.SessionId <= 0)
        {
            return false;
        }

        NoteTime(decoded.LeaderTimestamp);
        if (!_sessions.TryGetValue(decoded.SessionId, out Session? session))
        {
            _appliedThrough = Math.Max(_appliedThrough, index);
            return true;
        }

        if (IsExpired(session))
        {
            _appliedThrough = Math.Max(_appliedThrough, index);
            return true;
        }

        if (decoded.AckFloor > session.AckFloor)
        {
            bool keepCache = session.HasCache && session.LastSeq > decoded.AckFloor;
            session = session with { AckFloor = decoded.AckFloor, HasCache = keepCache };
        }

        if (decoded.Serial <= session.AckFloor)
        {
            _sessions = _sessions.SetItem(session.Id, session);
            _appliedThrough = Math.Max(_appliedThrough, index);
            return true;
        }

        if (decoded.Serial == session.LastSeq && session.LastSeq > 0)
        {
            long active = Math.Max(session.LastActive, decoded.LeaderTimestamp);
            _sessions = _sessions.SetItem(session.Id, session with { LastActive = active });
            _appliedThrough = Math.Max(_appliedThrough, index);
            return true;
        }

        if (decoded.Serial != session.LastSeq + 1)
        {
            _sessions = _sessions.SetItem(session.Id, session);
            _appliedThrough = Math.Max(_appliedThrough, index);
            return true;
        }

        if (decoded.Operation == KvCommandCodec.Op.Put)
        {
            _map = _map.SetItem(decoded.Key, decoded.Value);
        }
        else if (decoded.Operation == KvCommandCodec.Op.Delete)
        {
            _map = _map.Remove(decoded.Key);
        }

        session = session with
        {
            LastSeq = decoded.Serial,
            LastActive = Math.Max(session.LastActive, decoded.LeaderTimestamp),
            HasCache = true,
            CachedKey = decoded.Key,
            CachedValue = decoded.Operation == KvCommandCodec.Op.Put ? decoded.Value : "",
        };
        _sessions = _sessions.SetItem(session.Id, session);
        _appliedThrough = Math.Max(_appliedThrough, index);
        return true;
    }

    private void NoteTime(long timestamp)
    {
        if (timestamp > _logTime)
        {
            _logTime = timestamp;
        }
    }

    private bool IsExpired(Session session) =>
        session.TimeoutMillis > 0 && _logTime - session.LastActive > session.TimeoutMillis;

    private static byte[] EncodeSessionTail(ImmutableDictionary<long, Session> sessions, long logTime)
    {
        var ids = sessions.Keys.OrderBy(static id => id).ToList();
        var cachedKeys = new List<byte[]>(ids.Count);
        var cachedValues = new List<byte[]>(ids.Count);
        int size = 8 + 4;
        foreach (long id in ids)
        {
            Session session = sessions[id];
            size += (8 * 5) + 1;
            if (session.HasCache)
            {
                byte[] key = Encoding.UTF8.GetBytes(session.CachedKey);
                byte[] value = Encoding.UTF8.GetBytes(session.CachedValue);
                cachedKeys.Add(key);
                cachedValues.Add(value);
                size += 8 + key.Length + value.Length;
            }
            else
            {
                cachedKeys.Add([]);
                cachedValues.Add([]);
            }
        }

        byte[] buf = new byte[size];
        int pos = 0;
        BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), logTime);
        pos += 8;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(pos), ids.Count);
        pos += 4;
        for (int i = 0; i < ids.Count; i++)
        {
            Session session = sessions[ids[i]];
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), session.Id);
            pos += 8;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), session.LastSeq);
            pos += 8;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), session.AckFloor);
            pos += 8;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), session.LastActive);
            pos += 8;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), session.TimeoutMillis);
            pos += 8;
            buf[pos++] = session.HasCache ? (byte)1 : (byte)0;
            if (session.HasCache)
            {
                WriteBytes(buf, ref pos, cachedKeys[i]);
                WriteBytes(buf, ref pos, cachedValues[i]);
            }
        }

        return buf;
    }

    private void RestoreSessionTail(byte[] snapshot, ref int pos)
    {
        if (snapshot.Length - pos < 12)
        {
            throw new ArgumentException("truncated kv session table");
        }

        _logTime = BinaryPrimitives.ReadInt64BigEndian(snapshot.AsSpan(pos));
        pos += 8;
        int count = BinaryPrimitives.ReadInt32BigEndian(snapshot.AsSpan(pos));
        pos += 4;
        if (count < 0)
        {
            throw new ArgumentException("negative kv session count");
        }

        var restored = ImmutableDictionary.CreateBuilder<long, Session>();
        for (int i = 0; i < count; i++)
        {
            if (snapshot.Length - pos < (8 * 5) + 1)
            {
                throw new ArgumentException("truncated kv session");
            }

            long id = BinaryPrimitives.ReadInt64BigEndian(snapshot.AsSpan(pos));
            pos += 8;
            long lastSeq = BinaryPrimitives.ReadInt64BigEndian(snapshot.AsSpan(pos));
            pos += 8;
            long ackFloor = BinaryPrimitives.ReadInt64BigEndian(snapshot.AsSpan(pos));
            pos += 8;
            long lastActive = BinaryPrimitives.ReadInt64BigEndian(snapshot.AsSpan(pos));
            pos += 8;
            long timeout = BinaryPrimitives.ReadInt64BigEndian(snapshot.AsSpan(pos));
            pos += 8;
            bool hasCache = snapshot[pos++] != 0;
            string key = "";
            string value = "";
            if (hasCache)
            {
                key = ReadUtf8(snapshot, ref pos);
                value = ReadUtf8(snapshot, ref pos);
            }

            restored[id] = new Session(id, lastSeq, ackFloor, lastActive, timeout, hasCache, key, value);
        }

        _sessions = restored.ToImmutable();
        if (pos != snapshot.Length)
        {
            throw new ArgumentException("trailing bytes after kv session table");
        }
    }

    private sealed record Session(
        long Id,
        long LastSeq,
        long AckFloor,
        long LastActive,
        long TimeoutMillis,
        bool HasCache,
        string CachedKey,
        string CachedValue);

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
