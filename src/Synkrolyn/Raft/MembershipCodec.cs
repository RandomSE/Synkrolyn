using System.Buffers.Binary;
using System.Text;

namespace Synkrolyn.Raft;

/// <summary>Membership commands stored in the Raft log.</summary>
public static class MembershipCodec
{
    private const byte AddLearnerTag = 0x30;
    private const byte PromoteVoterTag = 0x31;
    private const byte RemoveServerTag = 0x32;
    private const byte JointTag = 0x33;
    private const byte CnewTag = 0x34;

    /// <summary>Membership command kind.</summary>
    public enum Kind
    {
        /// <summary>Add a learner.</summary>
        AddLearner,

        /// <summary>Promote a learner to voter.</summary>
        PromoteVoter,

        /// <summary>Remove a voter or learner.</summary>
        RemoveServer,

        /// <summary>Enter joint consensus.</summary>
        Joint,

        /// <summary>Leave joint consensus for Cnew.</summary>
        Cnew,
    }

    /// <summary>Decoded membership command.</summary>
    public sealed record Change(Kind Kind, string NodeId, IReadOnlySet<string> OldVoters, IReadOnlySet<string> NewVoters)
    {
        /// <summary>Single-server change.</summary>
        public Change(Kind kind, string nodeId)
            : this(kind, nodeId, new HashSet<string>(), new HashSet<string>())
        {
        }
    }

    /// <summary>True when <paramref name="command"/> starts with a membership tag.</summary>
    public static bool IsMembership(byte[] command)
    {
        if (command is null || command.Length < 1)
        {
            return false;
        }

        byte tag = command[0];
        return tag is AddLearnerTag or PromoteVoterTag or RemoveServerTag or JointTag or CnewTag;
    }

    /// <summary>Encodes an add-learner command.</summary>
    public static byte[] EncodeAddLearner(string nodeId) => Encode(AddLearnerTag, nodeId);

    /// <summary>Encodes a promote command.</summary>
    public static byte[] EncodePromoteVoter(string nodeId) => Encode(PromoteVoterTag, nodeId);

    /// <summary>Encodes a remove command.</summary>
    public static byte[] EncodeRemoveServer(string nodeId) => Encode(RemoveServerTag, nodeId);

    /// <summary>Encodes Cold,Cnew.</summary>
    public static byte[] EncodeJoint(IReadOnlyCollection<string> oldVoters, IReadOnlyCollection<string> newVoters) =>
        EncodeSets(JointTag, oldVoters, newVoters);

    /// <summary>Encodes Cnew alone.</summary>
    public static byte[] EncodeCnew(IReadOnlyCollection<string> newVoters) =>
        EncodeSets(CnewTag, [], newVoters);

    /// <summary>Decodes a membership command.</summary>
    public static Change Decode(byte[] command)
    {
        ArgumentNullException.ThrowIfNull(command);
        if (command.Length < 1)
        {
            throw new ArgumentException("empty membership command");
        }

        int pos = 1;
        byte tag = command[0];
        if (tag is JointTag or CnewTag)
        {
            IReadOnlySet<string> oldVoters = ReadIds(command, ref pos);
            IReadOnlySet<string> newVoters = ReadIds(command, ref pos);
            if (pos != command.Length)
            {
                throw new ArgumentException("trailing bytes after membership config");
            }

            return new Change(tag == JointTag ? Kind.Joint : Kind.Cnew, "", oldVoters, newVoters);
        }

        Kind kind = tag switch
        {
            AddLearnerTag => Kind.AddLearner,
            PromoteVoterTag => Kind.PromoteVoter,
            RemoveServerTag => Kind.RemoveServer,
            _ => throw new ArgumentException("unknown membership tag: " + tag),
        };
        string id = ReadUtf8(command, ref pos);
        if (string.IsNullOrWhiteSpace(id))
        {
            throw new ArgumentException("membership node id must be non-blank");
        }

        if (pos != command.Length)
        {
            throw new ArgumentException("trailing bytes after membership command");
        }

        return new Change(kind, id);
    }

    private static byte[] Encode(byte tag, string nodeId)
    {
        if (string.IsNullOrWhiteSpace(nodeId))
        {
            throw new ArgumentException("node id must be non-blank");
        }

        byte[] idBytes = Encoding.UTF8.GetBytes(nodeId);
        byte[] buf = new byte[1 + 4 + idBytes.Length];
        buf[0] = tag;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), idBytes.Length);
        idBytes.CopyTo(buf.AsSpan(5));
        return buf;
    }

    private static byte[] EncodeSets(byte tag, IReadOnlyCollection<string> oldVoters, IReadOnlyCollection<string> newVoters)
    {
        ArgumentNullException.ThrowIfNull(oldVoters);
        ArgumentNullException.ThrowIfNull(newVoters);
        if (newVoters.Count == 0)
        {
            throw new ArgumentException("new voter set must not be empty");
        }

        byte[] oldBlob = EncodeIds(oldVoters);
        byte[] newBlob = EncodeIds(newVoters);
        byte[] buf = new byte[1 + oldBlob.Length + newBlob.Length];
        buf[0] = tag;
        oldBlob.CopyTo(buf.AsSpan(1));
        newBlob.CopyTo(buf.AsSpan(1 + oldBlob.Length));
        return buf;
    }

    private static byte[] EncodeIds(IReadOnlyCollection<string> ids)
    {
        var encoded = new List<byte[]>(ids.Count);
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

    private static HashSet<string> ReadIds(byte[] buf, ref int pos)
    {
        if (buf.Length - pos < 4)
        {
            throw new ArgumentException("truncated membership id list");
        }

        int count = BinaryPrimitives.ReadInt32BigEndian(buf.AsSpan(pos));
        pos += 4;
        if (count < 0)
        {
            throw new ArgumentException("negative membership id count");
        }

        var ids = new HashSet<string>();
        for (int i = 0; i < count; i++)
        {
            ids.Add(ReadUtf8(buf, ref pos));
        }

        return ids;
    }

    private static string ReadUtf8(byte[] buf, ref int pos)
    {
        if (buf.Length - pos < 4)
        {
            throw new ArgumentException("truncated membership id length");
        }

        int len = BinaryPrimitives.ReadInt32BigEndian(buf.AsSpan(pos));
        pos += 4;
        if (len < 0 || buf.Length - pos < len)
        {
            throw new ArgumentException("truncated membership id");
        }

        string id = Encoding.UTF8.GetString(buf, pos, len);
        pos += len;
        return id;
    }
}
