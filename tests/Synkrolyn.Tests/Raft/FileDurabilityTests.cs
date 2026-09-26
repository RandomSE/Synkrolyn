using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

public class FileRaftLogTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "synkrolyn-log-" + Guid.NewGuid().ToString("n"));

    public FileRaftLogTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    [Fact]
    public void Append_isInvisibleUntilForce_andSurvivesReopen()
    {
        var first = new FileRaftLog(_dir);
        first.Append(new LogEntry(1, 1, "durable"u8.ToArray()));
        Assert.Equal(0, first.DurableIndex);
        Assert.Equal(0, first.ForceCount);
        first.CrashWithoutForce();
        using (var missed = new FileRaftLog(_dir))
        {
            Assert.Equal(0, missed.LastIndex);
        }

        using (var log = new FileRaftLog(_dir))
        {
            log.Append(new LogEntry(1, 1, []));
            log.Append(new LogEntry(2, 3, "hello"u8.ToArray()));
            log.Force();
            Assert.Equal(1, log.ForceCount);
            log.Append(new LogEntry(3, 3, "batched"u8.ToArray()));
            log.Force();
            Assert.Equal(2, log.ForceCount);
        }

        using var reopened = new FileRaftLog(_dir);
        Assert.Equal(3, reopened.LastIndex);
        Assert.Equal(3, reopened.DurableIndex);
        Assert.Equal("hello"u8.ToArray(), reopened.Read(2).Command);
        Assert.Empty(reopened.Read(1).Command);
    }

    [Fact]
    public void TornTail_isTruncated_midFileCorruptionThrows()
    {
        using (var log = new FileRaftLog(_dir))
        {
            log.Append(new LogEntry(1, 1, "keep"u8.ToArray()));
            log.Append(new LogEntry(2, 1, "tail"u8.ToArray()));
            log.Force();
        }

        string path = Path.Combine(_dir, FileRaftLog.FileName);
        byte[] bytes = File.ReadAllBytes(path);
        File.WriteAllBytes(path, bytes[..^3]);
        using (var repaired = new FileRaftLog(_dir))
        {
            Assert.Equal(1, repaired.LastIndex);
            Assert.Equal("keep"u8.ToArray(), repaired.Read(1).Command);
        }

        bytes = File.ReadAllBytes(path);
        bytes[10] ^= 0xFF;
        File.WriteAllBytes(path, bytes);
        File.AppendAllText(path, "more");
        Assert.Throws<InvalidOperationException>(() => new FileRaftLog(_dir));
    }

    [Fact]
    public void TruncateAndCompact_roundTrip()
    {
        using (var log = new FileRaftLog(_dir))
        {
            log.Append(new LogEntry(1, 1, "a"u8.ToArray()));
            log.Append(new LogEntry(2, 1, "b"u8.ToArray()));
            log.Append(new LogEntry(3, 2, "c"u8.ToArray()));
            log.Force();
            log.TruncateFrom(3);
            log.CompactThrough(1, 1, "snap"u8.ToArray());
        }

        using var reopened = new FileRaftLog(_dir);
        Assert.Equal(1, reopened.LastIncludedIndex);
        Assert.Equal(2, reopened.LastIndex);
        Assert.Equal("snap"u8.ToArray(), reopened.SnapshotBytes());
        Assert.Throws<InvalidOperationException>(() => reopened.Read(1));
        Assert.Equal("b"u8.ToArray(), reopened.Read(2).Command);
    }
}

public class FilePersistentStateTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "synkrolyn-hs-" + Guid.NewGuid().ToString("n"));

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    [Fact]
    public void TermAndVote_areAtomicAcrossReopen()
    {
        using (var state = new FilePersistentState(_dir))
        {
            Assert.Equal(0, state.CurrentTerm);
            Assert.Null(state.VotedFor);
            state.SetCurrentTerm(3);
            state.RecordVote("n2");
            state.PersistMembership("cfg"u8.ToArray());
        }

        using var reopened = new FilePersistentState(_dir);
        Assert.Equal(3, reopened.CurrentTerm);
        Assert.Equal("n2", reopened.VotedFor);
        Assert.Equal("cfg"u8.ToArray(), reopened.MembershipBlob());
        Assert.Throws<ArgumentException>(() => reopened.SetCurrentTerm(2));
        Assert.Throws<InvalidOperationException>(() => reopened.RecordVote("n1"));
    }
}
