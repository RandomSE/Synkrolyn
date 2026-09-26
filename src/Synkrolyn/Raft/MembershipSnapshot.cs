using System.Buffers.Binary;
using System.Text;

namespace Synkrolyn.Raft;

/// <summary>
/// Snapshot framing. Bytes without the PTCF prefix are state-machine-only.
/// Version 1 is voters plus learners. Version 2 adds Cold and Cnew.
/// </summary>
public static class MembershipSnapshot
{
    private static readonly byte[] Magic = "PTCF"u8.ToArray();
    private const byte VersionV1 = 1;
    private const byte Version = 2;

    /// <summary>Decoded snapshot header plus state-machine bytes.</summary>
    public sealed class Decoded
    {
        /// <summary>Creates a decoded snapshot.</summary>
        public Decoded(
            IReadOnlySet<string> voters,
            IReadOnlySet<string> learners,
            IReadOnlySet<string> cold,
            IReadOnlySet<string> cnew,
            byte[] stateMachineBytes,
            bool hasConfig)
        {
            Voters = voters;
            Learners = learners;
            Cold = cold;
            Cnew = cnew;
            StateMachineBytes = stateMachineBytes;
            HasConfig = hasConfig;
        }

        /// <summary>Voter ids, including this node when it votes.</summary>
        public IReadOnlySet<string> Voters { get; }

        /// <summary>Learner ids.</summary>
        public IReadOnlySet<string> Learners { get; }

        /// <summary>Cold voters when joint.</summary>
        public IReadOnlySet<string> Cold { get; }

        /// <summary>Cnew voters when joint.</summary>
        public IReadOnlySet<string> Cnew { get; }

        /// <summary>Opaque state-machine bytes.</summary>
        public byte[] StateMachineBytes { get; }

        /// <summary>True when a membership header was present.</summary>
        public bool HasConfig { get; }

        /// <summary>True when both Cold and Cnew are non-empty.</summary>
        public bool InJoint => Cold.Count > 0 && Cnew.Count > 0;
    }

    /// <summary>Encodes voters, learners, and state-machine bytes with empty Cold and Cnew.</summary>
    public static byte[] Encode(IReadOnlyCollection<string> voters, IReadOnlyCollection<string> learners, byte[] stateMachineBytes) =>
        Encode(voters, learners, [], [], stateMachineBytes);

    /// <summary>Encodes a version 2 snapshot.</summary>
    public static byte[] Encode(
        IReadOnlyCollection<string> voters,
        IReadOnlyCollection<string> learners,
        IReadOnlyCollection<string> cold,
        IReadOnlyCollection<string> cnew,
        byte[] stateMachineBytes)
    {
        ArgumentNullException.ThrowIfNull(voters);
        ArgumentNullException.ThrowIfNull(learners);
        ArgumentNullException.ThrowIfNull(cold);
        ArgumentNullException.ThrowIfNull(cnew);
        ArgumentNullException.ThrowIfNull(stateMachineBytes);
        byte[] voterBlob = EncodeIds(voters);
        byte[] learnerBlob = EncodeIds(learners);
        byte[] coldBlob = EncodeIds(cold);
        byte[] cnewBlob = EncodeIds(cnew);
        byte[] buf = new byte[Magic.Length + 1 + voterBlob.Length + learnerBlob.Length + coldBlob.Length + cnewBlob.Length + 4 + stateMachineBytes.Length];
        int pos = 0;
        Magic.CopyTo(buf.AsSpan(pos));
        pos += Magic.Length;
        buf[pos++] = Version;
        voterBlob.CopyTo(buf.AsSpan(pos));
        pos += voterBlob.Length;
        learnerBlob.CopyTo(buf.AsSpan(pos));
        pos += learnerBlob.Length;
        coldBlob.CopyTo(buf.AsSpan(pos));
        pos += coldBlob.Length;
        cnewBlob.CopyTo(buf.AsSpan(pos));
        pos += cnewBlob.Length;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(pos), stateMachineBytes.Length);
        pos += 4;
        stateMachineBytes.CopyTo(buf.AsSpan(pos));
        return buf;
    }

    /// <summary>Config blob with empty state-machine bytes.</summary>
    public static byte[] EncodeConfig(
        IReadOnlyCollection<string> voters,
        IReadOnlyCollection<string> learners,
        IReadOnlyCollection<string> cold,
        IReadOnlyCollection<string> cnew) =>
        Encode(voters, learners, cold, cnew, []);

    /// <summary>Decodes a snapshot. Missing magic yields state-machine-only bytes.</summary>
    public static Decoded Decode(byte[] raw)
    {
        ArgumentNullException.ThrowIfNull(raw);
        if (raw.Length < Magic.Length || !StartsWithMagic(raw))
        {
            return new Decoded(new HashSet<string>(), new HashSet<string>(), new HashSet<string>(), new HashSet<string>(), (byte[])raw.Clone(), false);
        }

        int pos = Magic.Length;
        byte version = raw[pos++];
        if (version != Version && version != VersionV1)
        {
            throw new ArgumentException("unknown membership snapshot version: " + version);
        }

        IReadOnlySet<string> voters = ReadIds(raw, ref pos);
        IReadOnlySet<string> learners = ReadIds(raw, ref pos);
        IReadOnlySet<string> cold = new HashSet<string>();
        IReadOnlySet<string> cnew = new HashSet<string>();
        if (version == Version)
        {
            cold = ReadIds(raw, ref pos);
            cnew = ReadIds(raw, ref pos);
        }

        if (raw.Length - pos < 4)
        {
            throw new ArgumentException("truncated membership snapshot payload");
        }

        int smLen = BinaryPrimitives.ReadInt32BigEndian(raw.AsSpan(pos));
        pos += 4;
        if (smLen < 0 || raw.Length - pos < smLen)
        {
            throw new ArgumentException("truncated membership snapshot state machine");
        }

        byte[] sm = raw.AsSpan(pos, smLen).ToArray();
        pos += smLen;
        if (pos != raw.Length)
        {
            throw new ArgumentException("trailing bytes after membership snapshot");
        }

        return new Decoded(voters, learners, cold, cnew, sm, true);
    }

    private static bool StartsWithMagic(byte[] raw)
    {
        for (int i = 0; i < Magic.Length; i++)
        {
            if (raw[i] != Magic[i])
            {
                return false;
            }
        }

        return true;
    }

    private static byte[] EncodeIds(IReadOnlyCollection<string> ids)
    {
        var encoded = new List<byte[]>();
        int size = 4;
        foreach (string id in ids)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(id);
            encoded.Add(bytes);
            size += 4 + bytes.Length;
        }

        byte[] buf = new byte[size];
        BinaryPrimitives.WriteInt32BigEndian(buf, encoded.Count);
        int pos = 4;
        foreach (byte[] bytes in encoded)
        {
            BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(pos), bytes.Length);
            pos += 4;
            bytes.CopyTo(buf.AsSpan(pos));
            pos += bytes.Length;
        }

        return buf;
    }

    private static HashSet<string> ReadIds(byte[] raw, ref int pos)
    {
        if (raw.Length - pos < 4)
        {
            throw new ArgumentException("truncated membership id list");
        }

        int count = BinaryPrimitives.ReadInt32BigEndian(raw.AsSpan(pos));
        pos += 4;
        if (count < 0)
        {
            throw new ArgumentException("negative membership id count");
        }

        var ids = new HashSet<string>();
        for (int i = 0; i < count; i++)
        {
            if (raw.Length - pos < 4)
            {
                throw new ArgumentException("truncated membership id length");
            }

            int len = BinaryPrimitives.ReadInt32BigEndian(raw.AsSpan(pos));
            pos += 4;
            if (len < 0 || raw.Length - pos < len)
            {
                throw new ArgumentException("truncated membership id");
            }

            ids.Add(Encoding.UTF8.GetString(raw, pos, len));
            pos += len;
        }

        return ids;
    }
}
