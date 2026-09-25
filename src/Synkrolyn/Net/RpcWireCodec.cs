using System.Buffers.Binary;
using System.Text;
using Synkrolyn.Raft;

namespace Synkrolyn.Net;

/// <summary>Length-prefixed binary encoding of Raft RPC records.</summary>
public sealed class RpcWireCodec : IMessageCodec
{
    private const byte RequestVoteTag = 1;
    private const byte RequestVoteResponseTag = 2;
    private const byte AppendEntriesTag = 3;
    private const byte AppendEntriesResponseTag = 4;
    private const byte InstallSnapshotTag = 5;
    private const byte InstallSnapshotResponseTag = 6;
    private const byte TimeoutNowTag = 7;

    /// <inheritdoc />
    public byte[] Encode(string from, object payload)
    {
        if (string.IsNullOrWhiteSpace(from))
        {
            throw new ArgumentException("from must be non-blank", nameof(from));
        }

        ArgumentNullException.ThrowIfNull(payload);
        var writer = new Writer();
        writer.PutString(from);
        switch (payload)
        {
            case RequestVote vote:
                writer.PutByte(RequestVoteTag);
                writer.PutLong(vote.Term);
                writer.PutString(vote.CandidateId);
                writer.PutLong(vote.LastLogIndex);
                writer.PutLong(vote.LastLogTerm);
                writer.PutBool(vote.PreVote);
                break;
            case RequestVoteResponse voteResponse:
                writer.PutByte(RequestVoteResponseTag);
                writer.PutLong(voteResponse.Term);
                writer.PutBool(voteResponse.VoteGranted);
                writer.PutBool(voteResponse.PreVote);
                break;
            case AppendEntries append:
                writer.PutByte(AppendEntriesTag);
                writer.PutLong(append.Term);
                writer.PutString(append.LeaderId);
                writer.PutLong(append.PrevLogIndex);
                writer.PutLong(append.PrevLogTerm);
                writer.PutInt(append.Entries.Count);
                foreach (LogEntry entry in append.Entries)
                {
                    writer.PutLong(entry.Index);
                    writer.PutLong(entry.Term);
                    writer.PutBytes(entry.Command);
                }

                writer.PutLong(append.LeaderCommit);
                writer.PutLong(append.Stamp);
                break;
            case AppendEntriesResponse appendResponse:
                writer.PutByte(AppendEntriesResponseTag);
                writer.PutLong(appendResponse.Term);
                writer.PutBool(appendResponse.Success);
                writer.PutLong(appendResponse.MatchIndex);
                writer.PutLong(appendResponse.XLen);
                writer.PutLong(appendResponse.XTerm);
                writer.PutLong(appendResponse.XIndex);
                writer.PutLong(appendResponse.Stamp);
                break;
            case InstallSnapshot snapshot:
                writer.PutByte(InstallSnapshotTag);
                writer.PutLong(snapshot.Term);
                writer.PutString(snapshot.LeaderId);
                writer.PutLong(snapshot.LastIncludedIndex);
                writer.PutLong(snapshot.LastIncludedTerm);
                writer.PutLong(snapshot.Offset);
                writer.PutBytes(snapshot.DataUnsafe);
                writer.PutBool(snapshot.Done);
                break;
            case InstallSnapshotResponse snapshotResponse:
                writer.PutByte(InstallSnapshotResponseTag);
                writer.PutLong(snapshotResponse.Term);
                writer.PutBool(snapshotResponse.Success);
                writer.PutBool(snapshotResponse.Done);
                writer.PutLong(snapshotResponse.InstalledIndex);
                writer.PutLong(snapshotResponse.InstalledTerm);
                break;
            case TimeoutNow timeout:
                writer.PutByte(TimeoutNowTag);
                writer.PutLong(timeout.Term);
                writer.PutString(timeout.LeaderId);
                break;
            default:
                throw new ArgumentException("unknown RPC type: " + payload.GetType());
        }

        return writer.ToArray();
    }

    /// <inheritdoc />
    public IMessageCodec.Decoded Decode(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var reader = new Reader(payload);
        string from = reader.ReadString();
        byte type = reader.ReadByte();
        object rpc = type switch
        {
            RequestVoteTag => new RequestVote(reader.ReadLong(), reader.ReadString(), reader.ReadLong(), reader.ReadLong(), reader.ReadBool()),
            RequestVoteResponseTag => new RequestVoteResponse(reader.ReadLong(), reader.ReadBool(), reader.ReadBool()),
            AppendEntriesTag => ReadAppendEntries(reader),
            AppendEntriesResponseTag => new AppendEntriesResponse(
                reader.ReadLong(), reader.ReadBool(), reader.ReadLong(), reader.ReadLong(), reader.ReadLong(), reader.ReadLong(), reader.ReadLong()),
            InstallSnapshotTag => new InstallSnapshot(
                reader.ReadLong(), reader.ReadString(), reader.ReadLong(), reader.ReadLong(), reader.ReadLong(), reader.ReadBytes(), reader.ReadBool()),
            InstallSnapshotResponseTag => new InstallSnapshotResponse(
                reader.ReadLong(), reader.ReadBool(), reader.ReadBool(), reader.ReadLong(), reader.ReadLong()),
            TimeoutNowTag => new TimeoutNow(reader.ReadLong(), reader.ReadString()),
            _ => throw new ArgumentException("unknown RPC type tag: " + type),
        };
        reader.RequireDone();
        return new IMessageCodec.Decoded(from, rpc);
    }

    private static AppendEntries ReadAppendEntries(Reader reader)
    {
        long term = reader.ReadLong();
        string leaderId = reader.ReadString();
        long prevIndex = reader.ReadLong();
        long prevTerm = reader.ReadLong();
        int count = reader.ReadInt();
        if (count < 0 || count > 1_000_000)
        {
            throw new ArgumentException("invalid entry count: " + count);
        }

        var entries = new List<LogEntry>(count);
        for (int i = 0; i < count; i++)
        {
            entries.Add(new LogEntry(reader.ReadLong(), reader.ReadLong(), reader.ReadBytes()));
        }

        return new AppendEntries(term, leaderId, prevIndex, prevTerm, entries, reader.ReadLong(), reader.ReadLong());
    }

    private sealed class Writer
    {
        private readonly List<byte> _bytes = [];

        public void PutByte(byte value) => _bytes.Add(value);

        public void PutBool(bool value) => _bytes.Add(value ? (byte)1 : (byte)0);

        public void PutInt(int value)
        {
            Span<byte> buf = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(buf, value);
            Add(buf);
        }

        public void PutLong(long value)
        {
            Span<byte> buf = stackalloc byte[8];
            BinaryPrimitives.WriteInt64BigEndian(buf, value);
            Add(buf);
        }

        public void PutString(string value) => PutBytes(Encoding.UTF8.GetBytes(value));

        public void PutBytes(byte[] value)
        {
            PutInt(value.Length);
            _bytes.AddRange(value);
        }

        public byte[] ToArray() => [.. _bytes];

        private void Add(ReadOnlySpan<byte> buf)
        {
            for (int i = 0; i < buf.Length; i++)
            {
                _bytes.Add(buf[i]);
            }
        }
    }

    private sealed class Reader
    {
        private readonly byte[] _buf;
        private int _pos;

        public Reader(byte[] buf) => _buf = buf;

        public byte ReadByte()
        {
            if (_pos >= _buf.Length)
            {
                throw new ArgumentException("truncated byte");
            }

            return _buf[_pos++];
        }

        public bool ReadBool() => ReadByte() != 0;

        public int ReadInt()
        {
            if (_buf.Length - _pos < 4)
            {
                throw new ArgumentException("truncated int");
            }

            int value = BinaryPrimitives.ReadInt32BigEndian(_buf.AsSpan(_pos));
            _pos += 4;
            return value;
        }

        public long ReadLong()
        {
            if (_buf.Length - _pos < 8)
            {
                throw new ArgumentException("truncated long");
            }

            long value = BinaryPrimitives.ReadInt64BigEndian(_buf.AsSpan(_pos));
            _pos += 8;
            return value;
        }

        public byte[] ReadBytes()
        {
            int len = ReadInt();
            if (len < 0 || _buf.Length - _pos < len)
            {
                throw new ArgumentException("truncated bytes");
            }

            byte[] bytes = _buf.AsSpan(_pos, len).ToArray();
            _pos += len;
            return bytes;
        }

        public string ReadString() => Encoding.UTF8.GetString(ReadBytes());

        public void RequireDone()
        {
            if (_pos != _buf.Length)
            {
                throw new ArgumentException("trailing bytes after RPC");
            }
        }
    }
}
