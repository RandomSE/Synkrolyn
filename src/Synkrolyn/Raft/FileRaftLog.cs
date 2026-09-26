using System.Buffers.Binary;

namespace Synkrolyn.Raft;

/// <summary>
/// Durable 1-indexed Raft log. <see cref="Append"/> may buffer. <see cref="Force"/>
/// and <see cref="Dispose"/> share one fsync per batch. A torn tail is dropped.
/// A checksum failure that is not the tail throws. Snapshot metadata lives in snapshot.bin.
/// </summary>
public sealed class FileRaftLog : IRaftLog, IDisposable
{
    /// <summary>Log file name.</summary>
    public const string FileName = "raft.log";

    /// <summary>Snapshot file name.</summary>
    public const string SnapshotFileName = "snapshot.bin";

    private readonly string _dir;
    private readonly string _file;
    private readonly string _snapshotFile;
    private readonly List<LogEntry> _pendingEntries = [];
    private readonly List<int> _pendingFrameLengths = [];
    private readonly MemoryStream _pending = new();
    private long[] _forcedOffsets = new long[8];
    private int _forcedCount;
    private long _forcedBaseIndex;
    private long _forcedLastTerm;
    private FileStream? _channel;
    private bool _closed;
    private long _lastIncludedIndex;
    private long _lastIncludedTerm;
    private byte[] _snapshot = [];
    private int _forceCount;

    /// <summary>Opens or creates the log under <paramref name="directory"/>.</summary>
    public FileRaftLog(string directory)
    {
        ArgumentNullException.ThrowIfNull(directory);
        _dir = directory;
        Directory.CreateDirectory(directory);
        _file = Path.Combine(directory, FileName);
        _snapshotFile = Path.Combine(directory, SnapshotFileName);
        _channel = new FileStream(_file, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        try
        {
            LoadSnapshot();
            LoadAndRepair();
            DropCompactedPrefixOnDisk();
        }
        catch
        {
            _closed = true;
            _channel.Dispose();
            _channel = null;
            throw;
        }
    }

    /// <summary>Number of successful fsync calls from <see cref="Force"/>.</summary>
    public int ForceCount
    {
        get
        {
            lock (this)
            {
                return _forceCount;
            }
        }
    }

    /// <inheritdoc />
    public void Append(LogEntry entry)
    {
        lock (this)
        {
            EnsureOpen();
            ArgumentNullException.ThrowIfNull(entry);
            long expected = LastIndex + 1;
            if (entry.Index != expected)
            {
                throw new ArgumentException(
                    "Raft log does not allow gaps: expected index " + expected + " but was " + entry.Index);
            }

            byte[] framed = ChecksummedRecords.Frame(Encode(entry));
            _pending.Write(framed);
            _pendingFrameLengths.Add(framed.Length);
            _pendingEntries.Add(entry);
        }
    }

    /// <inheritdoc />
    public void Force()
    {
        lock (this)
        {
            EnsureOpen();
            if (_pendingEntries.Count == 0)
            {
                return;
            }

            FileStream channel = Channel;
            long pos = channel.Length;
            channel.Position = pos;
            byte[] bytes = _pending.ToArray();
            channel.Write(bytes);
            channel.Flush(flushToDisk: true);
            _forceCount++;
            if (_forcedCount == 0)
            {
                _forcedBaseIndex = _pendingEntries[0].Index;
            }

            for (int i = 0; i < _pendingEntries.Count; i++)
            {
                AddOffset(pos);
                pos += _pendingFrameLengths[i];
            }

            _forcedLastTerm = _pendingEntries[^1].Term;
            ClearPending();
        }
    }

    /// <summary>
    /// Closes without flushing the unforced buffer. Reopen must not see those entries.
    /// Tests use this as a crash-before-fsync stand-in.
    /// </summary>
    public void CrashWithoutForce()
    {
        lock (this)
        {
            _closed = true;
            ClearPending();
            _channel?.Dispose();
            _channel = null;
        }
    }

    /// <inheritdoc />
    public LogEntry Read(long index)
    {
        lock (this)
        {
            EnsureOpen();
            RequirePositive(index);
            if (index <= _lastIncludedIndex)
            {
                throw new InvalidOperationException("log entry at index " + index + " was compacted");
            }

            if (index > LastIndex)
            {
                throw new KeyNotFoundException("no log entry at index " + index);
            }

            long forcedEnd = ForcedEndIndex();
            if (index <= forcedEnd)
            {
                return ReadForced(index);
            }

            int slot = checked((int)(index - forcedEnd - 1));
            return _pendingEntries[slot];
        }
    }

    /// <inheritdoc />
    public long LastIndex
    {
        get
        {
            lock (this)
            {
                EnsureOpen();
                if (_pendingEntries.Count > 0)
                {
                    return _pendingEntries[^1].Index;
                }

                return ForcedEndIndex();
            }
        }
    }

    /// <inheritdoc />
    public long LastTerm
    {
        get
        {
            lock (this)
            {
                EnsureOpen();
                if (_pendingEntries.Count > 0)
                {
                    return _pendingEntries[^1].Term;
                }

                if (_forcedCount == 0)
                {
                    return _lastIncludedTerm;
                }

                return _forcedLastTerm;
            }
        }
    }

    /// <inheritdoc />
    public long DurableIndex
    {
        get
        {
            lock (this)
            {
                EnsureOpen();
                return ForcedEndIndex();
            }
        }
    }

    /// <inheritdoc />
    public void TruncateFrom(long index)
    {
        lock (this)
        {
            EnsureOpen();
            RequirePositive(index);
            if (index <= _lastIncludedIndex)
            {
                throw new ArgumentException(
                    "truncateFrom index " + index + " is inside compacted prefix (lastIncludedIndex=" + _lastIncludedIndex + ")");
            }

            long endExclusive = LastIndexUnlocked() + 1;
            if (index > endExclusive)
            {
                throw new ArgumentException("truncateFrom index " + index + " is past lastIndex+1 (" + endExclusive + ")");
            }

            if (index == endExclusive)
            {
                return;
            }

            List<LogEntry> kept = CopyVisible();
            int from = checked((int)(index - kept[0].Index));
            kept.RemoveRange(from, kept.Count - from);
            PersistEntries(kept);
        }
    }

    /// <inheritdoc />
    public long LastIncludedIndex
    {
        get
        {
            lock (this)
            {
                EnsureOpen();
                return _lastIncludedIndex;
            }
        }
    }

    /// <inheritdoc />
    public long LastIncludedTerm
    {
        get
        {
            lock (this)
            {
                EnsureOpen();
                return _lastIncludedTerm;
            }
        }
    }

    /// <inheritdoc />
    public byte[] SnapshotBytes()
    {
        lock (this)
        {
            EnsureOpen();
            return (byte[])_snapshot.Clone();
        }
    }

    /// <inheritdoc />
    public void CompactThrough(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot)
    {
        lock (this)
        {
            EnsureOpen();
            List<LogEntry> kept = CopyVisible();
            LogCompaction.Apply(kept, _lastIncludedIndex, lastIncludedIndex, lastIncludedTerm, snapshot);
            _lastIncludedIndex = lastIncludedIndex;
            _lastIncludedTerm = lastIncludedTerm;
            _snapshot = (byte[])snapshot.Clone();
            PersistSnapshot();
            PersistEntries(kept);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (this)
        {
            if (!_closed)
            {
                try
                {
                    ForceUnlocked();
                }
                catch (InvalidOperationException)
                {
                    // already closed
                }
            }

            _closed = true;
            _channel?.Dispose();
            _channel = null;
        }
    }

    private void ForceUnlocked()
    {
        EnsureOpen();
        if (_pendingEntries.Count == 0)
        {
            return;
        }

        FileStream channel = Channel;
        long pos = channel.Length;
        channel.Position = pos;
        byte[] bytes = _pending.ToArray();
        channel.Write(bytes);
        channel.Flush(flushToDisk: true);
        _forceCount++;
        if (_forcedCount == 0)
        {
            _forcedBaseIndex = _pendingEntries[0].Index;
        }

        for (int i = 0; i < _pendingEntries.Count; i++)
        {
            AddOffset(pos);
            pos += _pendingFrameLengths[i];
        }

        _forcedLastTerm = _pendingEntries[^1].Term;
        ClearPending();
    }

    private FileStream Channel => _channel ?? throw new InvalidOperationException("FileRaftLog is closed");

    private long LastIndexUnlocked()
    {
        if (_pendingEntries.Count > 0)
        {
            return _pendingEntries[^1].Index;
        }

        return ForcedEndIndex();
    }

    private void LoadSnapshot()
    {
        if (!File.Exists(_snapshotFile) || new FileInfo(_snapshotFile).Length == 0)
        {
            return;
        }

        byte[] framed = File.ReadAllBytes(_snapshotFile);
        byte[] payload;
        try
        {
            payload = ChecksummedRecords.Unframe(framed);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException("incomplete or corrupt snapshot file", ex);
        }

        if (payload.Length < 16)
        {
            throw new InvalidOperationException("incomplete or corrupt snapshot file");
        }

        _lastIncludedIndex = Be.ReadLong(payload);
        _lastIncludedTerm = Be.ReadLong(payload.AsSpan(8));
        _snapshot = payload.AsSpan(16).ToArray();
    }

    private void PersistSnapshot()
    {
        string tmp = Path.Combine(_dir, SnapshotFileName + ".tmp");
        byte[] payload = new byte[16 + _snapshot.Length];
        BinaryPrimitives.WriteInt64BigEndian(payload, _lastIncludedIndex);
        BinaryPrimitives.WriteInt64BigEndian(payload.AsSpan(8), _lastIncludedTerm);
        _snapshot.CopyTo(payload.AsSpan(16));
        byte[] framed = ChecksummedRecords.Frame(payload);
        using (var channel = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            channel.Write(framed);
            channel.Flush(flushToDisk: true);
        }

        FilePersistentState.Replace(tmp, _snapshotFile);
    }

    private void DropCompactedPrefixOnDisk()
    {
        if (_lastIncludedIndex == 0)
        {
            if (_forcedCount > 0 && _forcedBaseIndex != 1)
            {
                throw new InvalidOperationException("log does not start at index 1 and has no snapshot");
            }

            return;
        }

        List<LogEntry> kept = CopyForced();
        int before = kept.Count;
        kept.RemoveAll(entry => entry.Index <= _lastIncludedIndex);
        if (kept.Count > 0 && kept[0].Index != _lastIncludedIndex + 1)
        {
            kept.Clear();
        }

        if (kept.Count != before)
        {
            PersistEntries(kept);
        }
    }

    private void LoadAndRepair()
    {
        FileStream channel = Channel;
        long size = channel.Length;
        long pos = 0;
        byte[] header = new byte[4];
        while (true)
        {
            long remaining = size - pos;
            if (remaining == 0)
            {
                break;
            }

            if (remaining < 4)
            {
                TruncateTo(pos);
                break;
            }

            channel.Position = pos;
            if (ReadAtMost(channel, header) < 4)
            {
                TruncateTo(pos);
                break;
            }

            int length = BinaryPrimitives.ReadInt32BigEndian(header);
            if (length < 0 || length > ChecksummedRecords.MaxPayloadBytes)
            {
                throw new InvalidOperationException("invalid log record length: " + length);
            }

            long recordEnd = pos + 4L + length + 4L;
            if (size < recordEnd)
            {
                TruncateTo(pos);
                break;
            }

            byte[] payload = new byte[length];
            ReadFully(channel, payload);
            byte[] crcBuf = new byte[4];
            ReadFully(channel, crcBuf);
            int crc = BinaryPrimitives.ReadInt32BigEndian(crcBuf);
            if (crc != ChecksummedRecords.Crc32(payload))
            {
                if (recordEnd < size)
                {
                    throw new InvalidOperationException("log checksum mismatch before tail at offset " + pos);
                }

                TruncateTo(pos);
                break;
            }

            LogEntry decoded = Decode(payload);
            if (_forcedCount == 0)
            {
                _forcedBaseIndex = decoded.Index;
            }

            AddOffset(pos);
            _forcedLastTerm = decoded.Term;
            pos = recordEnd;
        }

        channel.Position = channel.Length;
    }

    private void TruncateTo(long pos)
    {
        Channel.SetLength(pos);
        Channel.Flush(flushToDisk: true);
    }

    private void PersistEntries(List<LogEntry> kept)
    {
        string tmp = Path.Combine(_dir, FileName + ".tmp");
        long[] newOffsets = new long[Math.Max(8, kept.Count)];
        int newCount = 0;
        long newBase = kept.Count == 0 ? 0 : kept[0].Index;
        long newLastTerm = kept.Count == 0 ? _lastIncludedTerm : kept[^1].Term;
        using (var channel = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            long pos = 0;
            foreach (LogEntry entry in kept)
            {
                byte[] framed = ChecksummedRecords.Frame(Encode(entry));
                channel.Write(framed);
                newOffsets[newCount++] = pos;
                pos += framed.Length;
            }

            channel.Flush(flushToDisk: true);
        }

        _channel?.Dispose();
        FilePersistentState.Replace(tmp, _file);
        _channel = new FileStream(_file, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        _channel.Position = _channel.Length;
        _forcedOffsets = newOffsets;
        _forcedCount = newCount;
        _forcedBaseIndex = newBase;
        _forcedLastTerm = newLastTerm;
        ClearPending();
    }

    private List<LogEntry> CopyVisible()
    {
        List<LogEntry> copy = CopyForced();
        copy.AddRange(_pendingEntries);
        return copy;
    }

    private List<LogEntry> CopyForced()
    {
        var copy = new List<LogEntry>(_forcedCount);
        for (int i = 0; i < _forcedCount; i++)
        {
            copy.Add(ReadAt(_forcedOffsets[i]));
        }

        return copy;
    }

    private LogEntry ReadForced(long index)
    {
        int slot = checked((int)(index - _forcedBaseIndex));
        return ReadAt(_forcedOffsets[slot]);
    }

    private LogEntry ReadAt(long pos)
    {
        FileStream channel = Channel;
        channel.Position = pos;
        byte[] header = new byte[4];
        ReadFully(channel, header);
        int length = BinaryPrimitives.ReadInt32BigEndian(header);
        if (length < 0 || length > ChecksummedRecords.MaxPayloadBytes)
        {
            throw new InvalidOperationException("invalid log record length: " + length);
        }

        byte[] payload = new byte[length];
        ReadFully(channel, payload);
        byte[] crcBuf = new byte[4];
        ReadFully(channel, crcBuf);
        int crc = BinaryPrimitives.ReadInt32BigEndian(crcBuf);
        if (crc != ChecksummedRecords.Crc32(payload))
        {
            throw new InvalidOperationException("log checksum mismatch at offset " + pos);
        }

        return Decode(payload);
    }

    private static int ReadAtMost(FileStream channel, Span<byte> buf)
    {
        int read = 0;
        while (read < buf.Length)
        {
            int n = channel.Read(buf[read..]);
            if (n == 0)
            {
                break;
            }

            read += n;
        }

        return read;
    }

    private static void ReadFully(FileStream channel, Span<byte> buf)
    {
        if (ReadAtMost(channel, buf) != buf.Length)
        {
            throw new IOException("unexpected end of raft log");
        }
    }

    private long ForcedEndIndex() => _forcedCount == 0 ? _lastIncludedIndex : _forcedBaseIndex + _forcedCount - 1;

    private void AddOffset(long offset)
    {
        if (_forcedCount == _forcedOffsets.Length)
        {
            Array.Resize(ref _forcedOffsets, _forcedOffsets.Length * 2);
        }

        _forcedOffsets[_forcedCount++] = offset;
    }

    private void ClearPending()
    {
        _pending.SetLength(0);
        _pendingEntries.Clear();
        _pendingFrameLengths.Clear();
    }

    private static byte[] Encode(LogEntry entry)
    {
        byte[] command = entry.CommandUnsafe;
        byte[] buf = new byte[8 + 8 + 4 + command.Length];
        BinaryPrimitives.WriteInt64BigEndian(buf, entry.Index);
        BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(8), entry.Term);
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(16), command.Length);
        command.CopyTo(buf.AsSpan(20));
        return buf;
    }

    private static LogEntry Decode(byte[] payload)
    {
        if (payload.Length < 20)
        {
            throw new InvalidOperationException("log payload truncated");
        }

        long index = Be.ReadLong(payload);
        long term = Be.ReadLong(payload.AsSpan(8));
        int cmdLen = Be.ReadInt(payload.AsSpan(16));
        if (cmdLen < 0 || payload.Length < 20 + cmdLen)
        {
            throw new InvalidOperationException("log command truncated");
        }

        return new LogEntry(index, term, payload.AsSpan(20, cmdLen).ToArray());
    }

    private static void RequirePositive(long index)
    {
        if (index < 1)
        {
            throw new ArgumentException(LogEntry.IndexZeroMessage + " (got " + index + ")");
        }
    }

    private void EnsureOpen()
    {
        if (_closed)
        {
            throw new InvalidOperationException("FileRaftLog is closed");
        }
    }
}
