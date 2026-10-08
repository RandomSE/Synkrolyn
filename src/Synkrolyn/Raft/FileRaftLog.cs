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
    private readonly DataDirectoryLock _directoryLock;
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
    private int _diskReadCount;
    private int _dataSyncCount;
    private ArmedCapture? _armed;
    private readonly Action<FileStream>? _afterForceWrite;

    /// <summary>Opens or creates the log under <paramref name="directory"/>.</summary>
    public FileRaftLog(string directory)
        : this(directory, null)
    {
    }

    /// <summary>
    /// Opens the log. <paramref name="afterForceWrite"/> runs on this instance only,
    /// after the pending bytes are written and before they are flushed.
    /// </summary>
    internal FileRaftLog(string directory, Action<FileStream>? afterForceWrite)
    {
        ArgumentNullException.ThrowIfNull(directory);
        _afterForceWrite = afterForceWrite;
        _dir = directory;
        Directory.CreateDirectory(directory);
        _directoryLock = DataDirectoryLock.Acquire(directory, "raft.log");
        _file = Path.Combine(directory, FileName);
        _snapshotFile = Path.Combine(directory, SnapshotFileName);
        _channel = new FileStream(_file, FileMode.OpenOrCreate, FileAccess.ReadWrite, LogShare);
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
            _directoryLock.Dispose();
            throw;
        }
    }

    /// <summary>Forced-entry reads that hit the log file. Pending entries do not count.</summary>
    internal int DiskReadCount
    {
        get
        {
            lock (this)
            {
                return _diskReadCount;
            }
        }
    }

    /// <summary>Data fsyncs. A compaction commit must not add one; the executor already synced the new file.</summary>
    internal int DataSyncCount => Volatile.Read(ref _dataSyncCount);

    private void DataSync(FileStream channel)
    {
        channel.Flush(flushToDisk: true);
        Interlocked.Increment(ref _dataSyncCount);
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
            ForceUnlocked();
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
            _armed = null;
            ClearPending();
            _channel?.Dispose();
            _channel = null;
            _directoryLock.Dispose();
        }
    }

    /// <inheritdoc />
    public LogEntry Read(long index)
    {
        lock (this)
        {
            return ReadUnlocked(index);
        }
    }

    /// <inheritdoc />
    public ReplicationBatch ReadForReplication(long nextIndex, int maxEntries, int maxBytes)
    {
        lock (this)
        {
            EnsureOpen();
            if (_lastIncludedIndex > 0 && nextIndex <= _lastIncludedIndex)
            {
                return new ReplicationBatch(true, 0, 0, []);
            }

            long prevIndex = nextIndex - 1;
            long prevTerm = prevIndex == 0
                ? 0
                : prevIndex == _lastIncludedIndex
                    ? _lastIncludedTerm
                    : ReadUnlocked(prevIndex).Term;
            var entries = new List<LogEntry>();
            long end = LastIndexUnlocked();
            long last = Math.Min(end, nextIndex + maxEntries - 1);
            int bytes = 0;
            for (long i = nextIndex; i <= last; i++)
            {
                LogEntry entry = ReadUnlocked(i);
                if (entries.Count > 0 && bytes + entry.Command.Length > maxBytes)
                {
                    break;
                }

                entries.Add(entry);
                bytes += entry.Command.Length;
            }

            return new ReplicationBatch(false, prevIndex, prevTerm, entries);
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
        StagedCompaction staged = StageCompaction(lastIncludedIndex, lastIncludedTerm, snapshot);
        staged.Write();
        staged.Commit();
    }

    /// <summary>
    /// Share mode of every <c>raft.log</c> handle. Includes delete so Windows can
    /// rename the file. The snapshot capture itself is bytes, not a live handle.
    /// </summary>
    internal FileShare LastArmedShare { get; private set; }

    /// <summary>Read, write, and delete. A rename must not fail while a handle is open.</summary>
    private const FileShare LogShare = FileShare.ReadWrite | FileShare.Delete;

    /// <inheritdoc />
    public void ArmCompaction(long lastIncludedIndex, long lastIncludedTerm)
    {
        lock (this)
        {
            EnsureOpen();
            _armed = CaptureSuffix(lastIncludedIndex, lastIncludedTerm);
        }
    }

    /// <inheritdoc />
    public StagedCompaction StageCompaction(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot)
    {
        lock (this)
        {
            EnsureOpen();
            if (_armed is ArmedCapture armed && armed.Index == lastIncludedIndex)
            {
                _armed = null;
                var captured = new CompactJob(lastIncludedIndex, lastIncludedTerm, (byte[])snapshot.Clone(), [], _dir, DataSync);
                return new StagedCompaction(
                    () =>
                    {
                        captured.SetKept(ReadCaptured(armed));
                        captured.Write();
                    },
                    () => CommitJob(captured));
            }

            List<LogEntry> kept = CopyVisible();
            LogCompaction.Apply(kept, _lastIncludedIndex, lastIncludedIndex, lastIncludedTerm, snapshot);
            var job = new CompactJob(lastIncludedIndex, lastIncludedTerm, (byte[])snapshot.Clone(), kept, _dir, DataSync);
            return new StagedCompaction(() => job.Write(), () => CommitJob(job));
        }
    }

    private ArmedCapture CaptureSuffix(long index, long term)
    {
        long firstKept = index + 1;
        long fileEnd = Channel.Length;
        long keptOffset = fileEnd;
        long forcedEnd = ForcedEndIndex();
        if (_forcedCount > 0 && firstKept >= _forcedBaseIndex && firstKept <= forcedEnd)
        {
            int slot = checked((int)(firstKept - _forcedBaseIndex));
            keptOffset = _forcedOffsets[slot];
        }

        // Copy the suffix and drop the file position. A handle left open on
        // raft.log, even with FileShare.Delete, is what Windows refuses to
        // replace. The bytes stay valid after TruncateFrom renames the file.
        int suffixLength = checked((int)(fileEnd - keptOffset));
        byte[] suffix = suffixLength == 0 ? [] : new byte[suffixLength];
        if (suffixLength > 0)
        {
            FileStream channel = Channel;
            long restore = channel.Position;
            try
            {
                channel.Position = keptOffset;
                ReadFully(channel, suffix);
            }
            finally
            {
                channel.Position = restore;
            }
        }

        LastArmedShare = LogShare;
        return new ArmedCapture(index, term, suffix, _pendingEntries.ToList());
    }

    private static List<LogEntry> ReadCaptured(ArmedCapture armed)
    {
        var kept = new List<LogEntry>();
        ReadOnlySpan<byte> rest = armed.Suffix;
        while (rest.Length > 0)
        {
            if (rest.Length < 4)
            {
                throw new IOException("unexpected end of raft log");
            }

            int length = BinaryPrimitives.ReadInt32BigEndian(rest);
            rest = rest[4..];
            if (length < 0 || length > ChecksummedRecords.MaxPayloadBytes)
            {
                throw new InvalidOperationException("invalid log record length: " + length);
            }

            int record = length + 4;
            if (rest.Length < record)
            {
                throw new IOException("unexpected end of raft log");
            }

            byte[] payload = rest[..length].ToArray();
            int crc = BinaryPrimitives.ReadInt32BigEndian(rest.Slice(length, 4));
            if (crc != ChecksummedRecords.Crc32(payload))
            {
                throw new InvalidOperationException("log checksum mismatch in captured suffix");
            }

            kept.Add(Decode(payload));
            rest = rest[record..];
        }

        foreach (LogEntry entry in armed.PendingEntries)
        {
            if (entry.Index > armed.Index)
            {
                kept.Add(entry);
            }
        }

        return kept;
    }

    private LogEntry ReadUnlocked(long index)
    {
        EnsureOpen();
        RequirePositive(index);
        if (index <= _lastIncludedIndex)
        {
            throw new InvalidOperationException("log entry at index " + index + " was compacted");
        }

        if (index > LastIndexUnlocked())
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

    private void CommitJob(CompactJob job)
    {
        lock (this)
        {
            if (_closed || job.Index <= _lastIncludedIndex)
            {
                job.DeleteTemps();
                return;
            }

            if (!job.Written)
            {
                // The executor fsyncs the new file. Doing it here would stall the Raft thread.
                job.DeleteTemps();
                return;
            }

            List<LogEntry> current = CopyVisible();
            LogCompaction.Apply(current, _lastIncludedIndex, job.Index, job.Term, job.Snapshot);
            if (!job.MatchesPrefix(current))
            {
                // A truncation landed after the capture. The staged file is stale.
                // Leave the live log in place; a later snapshot captures it again.
                job.DeleteTemps();
                return;
            }

            long forcedEnd = ForcedEndIndex();
            var tail = new List<LogEntry>();
            bool tailWasForced = false;
            for (int i = job.Kept.Count; i < current.Count; i++)
            {
                tail.Add(current[i]);
                if (current[i].Index <= forcedEnd)
                {
                    tailWasForced = true;
                }
            }

            job.Publish(
                ref _channel,
                ref _forcedOffsets,
                ref _forcedCount,
                ref _forcedBaseIndex,
                ref _forcedLastTerm,
                _snapshotFile,
                _file);
            _lastIncludedIndex = job.Index;
            _lastIncludedTerm = job.Term;
            _snapshot = job.Snapshot;
            ClearPending();
            foreach (LogEntry entry in tail)
            {
                byte[] framed = ChecksummedRecords.Frame(Encode(entry));
                _pending.Write(framed);
                _pendingFrameLengths.Add(framed.Length);
                _pendingEntries.Add(entry);
            }

            if (tailWasForced)
            {
                // Those bytes were durable in the file the rename replaced.
                // One normal force puts them on the new file. It is not a rewrite.
                ForceUnlocked();
            }
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
            _armed = null;
            _channel?.Dispose();
            _channel = null;
            _directoryLock.Dispose();
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
        try
        {
            channel.Write(bytes);
            _afterForceWrite?.Invoke(channel);
            DataSync(channel);
        }
        catch
        {
            channel.SetLength(pos);
            channel.Position = pos;
            try
            {
                DataSync(channel);
            }
            catch (IOException)
            {
                // The torn tail is already truncated.
            }

            throw;
        }

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
        byte[] payload = ReadRecords(framed);
        if (payload.Length < 16)
        {
            throw new InvalidOperationException("incomplete or corrupt snapshot file");
        }

        _lastIncludedIndex = Be.ReadLong(payload);
        _lastIncludedTerm = Be.ReadLong(payload.AsSpan(8));
        _snapshot = payload.AsSpan(16).ToArray();
    }

    private static void WriteRecords(Stream channel, byte[] payload) =>
        ChecksummedRecords.WriteRecords(channel, payload);

    private static byte[] ReadRecords(byte[] framed)
    {
        try
        {
            return ChecksummedRecords.ReadRecords(framed);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException("incomplete or corrupt snapshot file", ex);
        }
    }

    private void PersistSnapshot()
    {
        string tmp = Path.Combine(_dir, SnapshotFileName + ".tmp");
        byte[] payload = new byte[16 + _snapshot.Length];
        BinaryPrimitives.WriteInt64BigEndian(payload, _lastIncludedIndex);
        BinaryPrimitives.WriteInt64BigEndian(payload.AsSpan(8), _lastIncludedTerm);
        _snapshot.CopyTo(payload.AsSpan(16));
        using (var channel = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            WriteRecords(channel, payload);
            DataSync(channel);
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
        DataSync(Channel);
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

            DataSync(channel);
        }

        _channel?.Dispose();
        FilePersistentState.Replace(tmp, _file);
        _channel = new FileStream(_file, FileMode.OpenOrCreate, FileAccess.ReadWrite, LogShare);
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
        _diskReadCount++;
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

    private sealed class CompactJob
    {
        private readonly string _snapshotTmp;
        private readonly string _entriesTmp;
        private readonly Action<FileStream> _sync;
        private long[] _offsets = new long[8];
        private int _count;
        private long _base;
        private long _lastTerm;
        private bool _written;

        public CompactJob(long index, long term, byte[] snapshot, List<LogEntry> kept, string directory, Action<FileStream> sync)
        {
            Index = index;
            Term = term;
            Snapshot = snapshot;
            Kept = kept;
            _sync = sync;
            string token = Guid.NewGuid().ToString("N");
            _snapshotTmp = Path.Combine(directory, SnapshotFileName + ".cmp-" + token);
            _entriesTmp = Path.Combine(directory, FileName + ".cmp-" + token);
        }

        public long Index { get; }

        public long Term { get; }

        public byte[] Snapshot { get; }

        public List<LogEntry> Kept { get; private set; }

        public void SetKept(List<LogEntry> kept) => Kept = kept ?? throw new ArgumentNullException(nameof(kept));

        public bool Written => _written;

        public void Write()
        {
            WriteSnapshotFile();
            WriteEntries(Kept, append: false);
            _written = true;
        }

        public bool MatchesPrefix(List<LogEntry> current)
        {
            if (current.Count < Kept.Count)
            {
                return false;
            }

            for (int i = 0; i < Kept.Count; i++)
            {
                if (current[i].Index != Kept[i].Index || current[i].Term != Kept[i].Term)
                {
                    return false;
                }
            }

            return true;
        }

        public void Publish(
            ref FileStream? channel,
            ref long[] forcedOffsets,
            ref int forcedCount,
            ref long forcedBaseIndex,
            ref long forcedLastTerm,
            string snapshotFile,
            string logFile)
        {
            channel?.Dispose();
            channel = null;
            FilePersistentState.Replace(_snapshotTmp, snapshotFile);
            FilePersistentState.Replace(_entriesTmp, logFile);
            channel = new FileStream(logFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, LogShare);
            channel.Position = channel.Length;
            forcedOffsets = _offsets;
            forcedCount = _count;
            forcedBaseIndex = _base;
            forcedLastTerm = _lastTerm;
        }

        public void DeleteTemps()
        {
            if (File.Exists(_snapshotTmp))
            {
                File.Delete(_snapshotTmp);
            }

            if (File.Exists(_entriesTmp))
            {
                File.Delete(_entriesTmp);
            }
        }

        private void WriteSnapshotFile()
        {
            byte[] payload = new byte[16 + Snapshot.Length];
            BinaryPrimitives.WriteInt64BigEndian(payload, Index);
            BinaryPrimitives.WriteInt64BigEndian(payload.AsSpan(8), Term);
            Snapshot.CopyTo(payload.AsSpan(16));
            using var channel = new FileStream(_snapshotTmp, FileMode.Create, FileAccess.Write, FileShare.None);
            WriteRecords(channel, payload);
            _sync(channel);
        }

        private void WriteEntries(List<LogEntry> entries, bool append)
        {
            if (!append)
            {
                _count = 0;
                _base = entries.Count == 0 ? 0 : entries[0].Index;
                _lastTerm = entries.Count == 0 ? Term : entries[^1].Term;
                _offsets = new long[Math.Max(8, entries.Count)];
            }
            else if (entries.Count > 0)
            {
                _lastTerm = entries[^1].Term;
            }

            var mode = append ? FileMode.Append : FileMode.Create;
            using var channel = new FileStream(_entriesTmp, mode, FileAccess.Write, FileShare.None);
            long pos = append ? channel.Length : 0;
            foreach (LogEntry entry in entries)
            {
                byte[] framed = ChecksummedRecords.Frame(Encode(entry));
                channel.Write(framed);
                if (_count == _offsets.Length)
                {
                    Array.Resize(ref _offsets, _offsets.Length * 2);
                }

                _offsets[_count++] = pos;
                pos += framed.Length;
            }

            _sync(channel);
        }
    }

    private sealed class ArmedCapture
    {
        public ArmedCapture(long index, long term, byte[] suffix, List<LogEntry> pendingEntries)
        {
            Index = index;
            Term = term;
            Suffix = suffix;
            PendingEntries = pendingEntries;
        }

        public long Index { get; }

        public long Term { get; }

        public byte[] Suffix { get; }

        public List<LogEntry> PendingEntries { get; }
    }
}
