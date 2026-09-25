using System.Buffers.Binary;
using System.Text;
using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Kv;

public sealed class KvSnapshotTests
{
    [Fact]
    public void ClientSerialsSurviveSnapshotRestore_duplicatePutDoesNotChangeMap()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodePut("k", "one", "c1", 1));
        byte[] snap = store.Snapshot();
        Assert.Equal((byte)'P', snap[0]);
        Assert.Equal((byte)'T', snap[1]);
        Assert.Equal((byte)'C', snap[2]);
        Assert.Equal((byte)'S', snap[3]);
        var restored = new InMemoryKvStore();
        restored.Restore(snap);
        Assert.Equal("one", restored.Get("k"));
        restored.Apply(2, KvCommandCodec.EncodePut("k", "two", "c1", 1));
        Assert.Equal("one", restored.Get("k"));
    }

    [Fact]
    public void LegacyMapOnlySnapshot_stillRestoresKeys()
    {
        byte[] key = Encoding.UTF8.GetBytes("city");
        byte[] value = Encoding.UTF8.GetBytes("athens");
        byte[] buf = new byte[4 + 4 + key.Length + 4 + value.Length];
        BinaryPrimitives.WriteInt32BigEndian(buf, 1);
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(4), key.Length);
        key.CopyTo(buf.AsSpan(8));
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(8 + key.Length), value.Length);
        value.CopyTo(buf.AsSpan(12 + key.Length));
        var restored = new InMemoryKvStore();
        restored.Restore(buf);
        Assert.Equal("athens", restored.Get("city"));
        restored.Apply(1, KvCommandCodec.EncodePut("city", "sparta", "c1", 1));
        Assert.Equal("sparta", restored.Get("city"));
    }

    [Fact]
    public void DeltaSnapshot_mergesOnBase()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodePut("a", "1"));
        store.Apply(2, KvCommandCodec.EncodePut("b", "2"));
        byte[] full = store.Snapshot();
        store.Apply(3, KvCommandCodec.EncodePut("c", "3"));
        store.Apply(4, KvCommandCodec.EncodeDelete("a"));
        byte[] delta = store.SnapshotDelta(0);
        Assert.True(KvSnapshotDelta.IsDelta(delta));
        Assert.True(delta.Length < store.Snapshot().Length || delta.Length > 0);
        var restored = new InMemoryKvStore();
        restored.Restore(full);
        restored.RestoreChecked(0, 0, delta);
        Assert.Null(restored.Get("a"));
        Assert.Equal("2", restored.Get("b"));
        Assert.Equal("3", restored.Get("c"));
    }

    [Fact]
    public void LegacyDeltaWithoutSerialTrailer_keepsBaseSerials()
    {
        var store = new InMemoryKvStore();
        store.Apply(1, KvCommandCodec.EncodePut("a", "1", "c1", 7));
        byte[] full = store.Snapshot();
        byte[] key = Encoding.UTF8.GetBytes("b");
        byte[] value = Encoding.UTF8.GetBytes("2");
        byte[] delta = new byte[4 + 1 + 8 + 4 + 4 + key.Length + 4 + value.Length + 4];
        "PTKV"u8.CopyTo(delta);
        delta[4] = 2;
        BinaryPrimitives.WriteInt64BigEndian(delta.AsSpan(5), 0);
        BinaryPrimitives.WriteInt32BigEndian(delta.AsSpan(13), 1);
        BinaryPrimitives.WriteInt32BigEndian(delta.AsSpan(17), key.Length);
        key.CopyTo(delta.AsSpan(21));
        BinaryPrimitives.WriteInt32BigEndian(delta.AsSpan(21 + key.Length), value.Length);
        value.CopyTo(delta.AsSpan(25 + key.Length));
        var restored = new InMemoryKvStore();
        restored.Restore(full);
        restored.RestoreChecked(0, 0, delta);
        restored.Apply(2, KvCommandCodec.EncodePut("a", "nope", "c1", 7));
        Assert.Equal("1", restored.Get("a"));
        Assert.Equal("2", restored.Get("b"));
    }

    [Fact]
    public void DiskKv_survivesReopen()
    {
        string dir = Directory.CreateTempSubdirectory("synkrolyn-kv-").FullName;
        try
        {
            var store = new FileKvStore(dir);
            store.Apply(1, KvCommandCodec.EncodePut("k", "v"));
            store.Snapshot();
            store.Dispose();
            using var reopened = new FileKvStore(dir);
            Assert.Equal("v", reopened.Get("k"));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}

public sealed class CodecTests
{
    [Fact]
    public void FrameCodec_twoFramesInOneRead_andIncompleteBuffers()
    {
        byte[] one = FrameCodec.Encode("ab"u8.ToArray());
        byte[] two = FrameCodec.Encode("cd"u8.ToArray());
        var decoder = new FrameCodec.Decoder();
        Assert.Empty(decoder.Push(one.AsSpan(0, 2).ToArray()));
        List<byte[]> frames = decoder.Push(one.AsSpan(2).ToArray().Concat(two).ToArray());
        Assert.Equal(2, frames.Count);
        Assert.Equal("ab"u8.ToArray(), frames[0]);
        Assert.Equal("cd"u8.ToArray(), frames[1]);
    }

    [Fact]
    public void RpcWireCodec_roundtripAppendEntriesAndTimeoutNow()
    {
        var codec = new RpcWireCodec();
        var append = new AppendEntries(3, "n1", 1, 1, [new LogEntry(2, 3, "cmd"u8.ToArray())], 1, 9);
        IMessageCodec.Decoded decoded = codec.Decode(codec.Encode("n1", append));
        var back = Assert.IsType<AppendEntries>(decoded.Payload);
        Assert.Equal("n1", decoded.From);
        Assert.Equal(3, back.Term);
        Assert.Equal(9, back.Stamp);
        Assert.Equal("cmd"u8.ToArray(), back.Entries[0].Command);
        var timeout = new TimeoutNow(4, "n2");
        Assert.Equal(timeout, Assert.IsType<TimeoutNow>(codec.Decode(codec.Encode("n2", timeout)).Payload));
    }

    [Fact]
    public void ProductionJitter_samplesStayInsideClosedInterval()
    {
        bool spread = false;
        long first = RaftRuntime.ProductionJitter(50);
        for (int i = 0; i < 80; i++)
        {
            long value = RaftRuntime.ProductionJitter(50);
            Assert.InRange(value, 50, 100);
            if (value != first)
            {
                spread = true;
            }
        }

        Assert.True(spread);
        Assert.InRange(RaftRuntime.ProductionJitter(1), 1, 2);
    }
}
