using Synkrolyn.Raft;

namespace Synkrolyn.Kv;

/// <summary>
/// Disk-backed KV map. Apply updates memory only. Persist happens on snapshot, close, and restore.
/// </summary>
public sealed class FileKvStore : IKvStore, IDisposable
{
    /// <summary>Store file name.</summary>
    public const string FileName = "kv-store.bin";

    private readonly string _dir;
    private readonly string _file;
    private readonly InMemoryKvStore _inner = new();
    private bool _closed;
    private int _persistCount;

    /// <summary>Opens or creates the store in <paramref name="directory"/>.</summary>
    public FileKvStore(string directory)
    {
        ArgumentNullException.ThrowIfNull(directory);
        _dir = directory;
        Directory.CreateDirectory(directory);
        _file = Path.Combine(directory, FileName);
        Load();
    }

    /// <summary>Successful persist calls.</summary>
    public int PersistCount => _persistCount;

    /// <inheritdoc />
    public void Apply(long index, byte[] command)
    {
        EnsureOpen();
        _inner.Apply(index, command);
    }

    /// <inheritdoc />
    public byte[] Snapshot()
    {
        EnsureOpen();
        byte[] snapshot = _inner.Snapshot();
        Persist();
        return snapshot;
    }

    /// <inheritdoc />
    public byte[] SnapshotDelta(long baseIndex)
    {
        EnsureOpen();
        return _inner.SnapshotDelta(baseIndex);
    }

    /// <inheritdoc />
    public void Restore(byte[] snapshot)
    {
        EnsureOpen();
        _inner.Restore(snapshot);
        Persist();
    }

    /// <inheritdoc />
    public void RestoreChecked(long lastIncludedIndex, long lastApplied, byte[] snapshot)
    {
        EnsureOpen();
        _inner.RestoreChecked(lastIncludedIndex, lastApplied, snapshot);
        Persist();
    }

    /// <inheritdoc />
    public string? Get(string key)
    {
        EnsureOpen();
        return _inner.Get(key);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_closed)
        {
            Persist();
            _closed = true;
        }

        GC.SuppressFinalize(this);
    }

    private void Load()
    {
        if (!File.Exists(_file) || new FileInfo(_file).Length == 0)
        {
            return;
        }

        byte[] framed = File.ReadAllBytes(_file);
        _inner.Restore(ChecksummedRecords.Unframe(framed));
    }

    private void Persist()
    {
        _persistCount++;
        byte[] framed = ChecksummedRecords.Frame(_inner.EncodeMapForPersist());
        string tmp = Path.Combine(_dir, FileName + ".tmp");
        using (var channel = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            channel.Write(framed);
            channel.Flush(flushToDisk: true);
        }

        FilePersistentState.Replace(tmp, _file);
    }

    private void EnsureOpen()
    {
        if (_closed)
        {
            throw new InvalidOperationException("FileKvStore is closed");
        }
    }
}
