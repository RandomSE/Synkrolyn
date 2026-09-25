namespace Synkrolyn.Raft;

/// <summary>
/// Durable term and vote. Both fields are written together (temp file, fsync, replace)
/// before each mutating call returns.
/// </summary>
public sealed class FilePersistentState : IPersistentState, IDisposable
{
    /// <summary>Hard-state file name.</summary>
    public const string FileName = "hard-state.bin";

    /// <summary>Membership config file name.</summary>
    public const string ConfigFileName = "config.bin";

    private readonly string _dir;
    private readonly string _file;
    private readonly string _configFile;
    private long _currentTerm;
    private string? _votedFor;
    private byte[] _membership = [];
    private bool _closed;

    /// <summary>Loads or creates hard state in <paramref name="directory"/>.</summary>
    public FilePersistentState(string directory)
    {
        ArgumentNullException.ThrowIfNull(directory);
        _dir = directory;
        Directory.CreateDirectory(directory);
        _file = Path.Combine(directory, FileName);
        _configFile = Path.Combine(directory, ConfigFileName);
        Load();
        LoadMembership();
    }

    /// <inheritdoc />
    public long CurrentTerm
    {
        get
        {
            EnsureOpen();
            return _currentTerm;
        }
    }

    /// <inheritdoc />
    public string? VotedFor
    {
        get
        {
            EnsureOpen();
            return _votedFor;
        }
    }

    /// <inheritdoc />
    public void SetCurrentTerm(long term)
    {
        EnsureOpen();
        if (term < 0)
        {
            throw new ArgumentException("term must be >= 0 (got " + term + ")");
        }

        if (term < _currentTerm)
        {
            throw new ArgumentException("term must not decrease: current=" + _currentTerm + " proposed=" + term);
        }

        if (term > _currentTerm)
        {
            _currentTerm = term;
            _votedFor = null;
            Persist();
        }
    }

    /// <inheritdoc />
    public void RecordVote(string candidateId)
    {
        EnsureOpen();
        if (string.IsNullOrWhiteSpace(candidateId))
        {
            throw new ArgumentException("candidateId must be non-blank");
        }

        if (_votedFor is not null && _votedFor != candidateId)
        {
            throw new InvalidOperationException("already voted for " + _votedFor + " in term " + _currentTerm);
        }

        if (candidateId == _votedFor)
        {
            return;
        }

        _votedFor = candidateId;
        Persist();
    }

    /// <inheritdoc />
    public void PersistMembership(byte[] blob)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(blob);
        _membership = (byte[])blob.Clone();
        PersistConfig();
    }

    /// <inheritdoc />
    public byte[] MembershipBlob()
    {
        EnsureOpen();
        return (byte[])_membership.Clone();
    }

    /// <inheritdoc />
    public void Dispose() => _closed = true;

    private void Load()
    {
        if (!File.Exists(_file) || new FileInfo(_file).Length == 0)
        {
            _currentTerm = 0;
            _votedFor = null;
            return;
        }

        byte[] all = File.ReadAllBytes(_file);
        if (all.Length < 8)
        {
            throw new InvalidOperationException("hard-state file is truncated");
        }

        int length = Be.ReadInt(all);
        if (length < 0 || length > ChecksummedRecords.MaxPayloadBytes)
        {
            throw new InvalidOperationException("invalid hard-state length: " + length);
        }

        if (all.Length < 4 + length + 4)
        {
            throw new InvalidOperationException("hard-state file is truncated");
        }

        byte[] payload = all.AsSpan(4, length).ToArray();
        int crc = Be.ReadInt(all.AsSpan(4 + length));
        if (crc != ChecksummedRecords.Crc32(payload))
        {
            throw new InvalidOperationException("hard-state checksum mismatch");
        }

        if (payload.Length < 12)
        {
            throw new InvalidOperationException("hard-state vote field truncated");
        }

        _currentTerm = Be.ReadLong(payload);
        int voteLen = Be.ReadInt(payload.AsSpan(8));
        if (voteLen < 0 || payload.Length < 12 + voteLen)
        {
            throw new InvalidOperationException("hard-state vote field truncated");
        }

        _votedFor = voteLen == 0 ? null : Be.Utf8(payload.AsSpan(12, voteLen));
    }

    private void Persist()
    {
        byte[] voteBytes = _votedFor is null ? [] : Be.Utf8(_votedFor);
        byte[] body = new byte[8 + 4 + voteBytes.Length];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(body, _currentTerm);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(body.AsSpan(8), voteBytes.Length);
        voteBytes.CopyTo(body.AsSpan(12));
        AtomicWrite(_file, FileName, ChecksummedRecords.Frame(body));
    }

    private void LoadMembership()
    {
        if (!File.Exists(_configFile) || new FileInfo(_configFile).Length == 0)
        {
            _membership = [];
            return;
        }

        byte[] all = File.ReadAllBytes(_configFile);
        if (all.Length < 8)
        {
            throw new InvalidOperationException("config file is truncated");
        }

        int length = Be.ReadInt(all);
        if (length < 0 || length > ChecksummedRecords.MaxPayloadBytes)
        {
            throw new InvalidOperationException("invalid config length: " + length);
        }

        if (all.Length < 4 + length + 4)
        {
            throw new InvalidOperationException("config file is truncated");
        }

        byte[] payload = all.AsSpan(4, length).ToArray();
        int crc = Be.ReadInt(all.AsSpan(4 + length));
        if (crc != ChecksummedRecords.Crc32(payload))
        {
            throw new InvalidOperationException("config checksum mismatch");
        }

        _membership = payload;
    }

    private void PersistConfig() => AtomicWrite(_configFile, ConfigFileName, ChecksummedRecords.Frame(_membership));

    private void AtomicWrite(string dest, string name, byte[] framed)
    {
        string tmp = Path.Combine(_dir, name + ".tmp");
        using (var channel = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            channel.Write(framed);
            channel.Flush(flushToDisk: true);
        }

        Replace(tmp, dest);
    }

    /// <summary>Atomically replaces <paramref name="dest"/> with <paramref name="tmp"/>.</summary>
    public static void Replace(string tmp, string dest)
    {
        File.Move(tmp, dest, overwrite: true);
    }

    private void EnsureOpen()
    {
        if (_closed)
        {
            throw new InvalidOperationException("FilePersistentState is closed");
        }
    }
}
