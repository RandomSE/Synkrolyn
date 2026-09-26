using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

public class InMemoryRaftLogTests
{
    private readonly IRaftLog _log = new InMemoryRaftLog();

    [Fact]
    public void EmptyLog_lastIndexAndTermAreZero()
    {
        Assert.Equal(0, _log.LastIndex);
        Assert.Equal(0, _log.LastTerm);
        Assert.Equal(0, _log.DurableIndex);
        Assert.Equal(1, _log.FirstIndex);
        Assert.Throws<KeyNotFoundException>(() => _log.Read(1));
        _log.TruncateFrom(1);
        Assert.Equal(0, _log.LastIndex);
    }

    [Fact]
    public void Append_copiesCommandBytes()
    {
        byte[] raw = [1, 2, 3];
        _log.Append(new LogEntry(1, 1, raw));
        raw[0] = 9;
        Assert.Equal(new byte[] { 1, 2, 3 }, _log.Read(1).Command);
        byte[] leaked = _log.Read(1).Command;
        leaked[0] = 9;
        Assert.Equal(new byte[] { 1, 2, 3 }, _log.Read(1).Command);
    }

    [Fact]
    public void Append_rejectsGapsAndIndexZero()
    {
        var gap = Assert.Throws<ArgumentException>(() => _log.Append(new LogEntry(2, 1, "x"u8.ToArray())));
        Assert.Contains("gap", gap.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, _log.LastIndex);
        Assert.Throws<ArgumentException>(() => new LogEntry(0, 1, "a"u8.ToArray()));
        Assert.Throws<ArgumentException>(() => new LogEntry(1, 0, "a"u8.ToArray()));
    }

    [Fact]
    public void TruncateFrom_dropsSuffixAndKeepsPrefix()
    {
        _log.Append(new LogEntry(1, 1, "a"u8.ToArray()));
        _log.Append(new LogEntry(2, 1, "b"u8.ToArray()));
        _log.Append(new LogEntry(3, 2, "c"u8.ToArray()));
        _log.TruncateFrom(2);
        Assert.Equal(1, _log.LastIndex);
        Assert.Equal(1, _log.LastTerm);
        Assert.Throws<KeyNotFoundException>(() => _log.Read(2));
        _log.Append(new LogEntry(2, 4, "d"u8.ToArray()));
        Assert.Equal(4, _log.Read(2).Term);
    }

    [Fact]
    public void Force_advancesDurableIndex()
    {
        _log.Append(new LogEntry(1, 1, "a"u8.ToArray()));
        Assert.Equal(0, _log.DurableIndex);
        _log.Force();
        Assert.Equal(1, _log.DurableIndex);
    }

    [Fact]
    public void CompactThrough_hidesPrefixAndKeepsContiguousSuffix()
    {
        _log.Append(new LogEntry(1, 1, "a"u8.ToArray()));
        _log.Append(new LogEntry(2, 1, "b"u8.ToArray()));
        _log.Append(new LogEntry(3, 2, "c"u8.ToArray()));
        _log.CompactThrough(2, 1, "snap"u8.ToArray());
        Assert.Equal(2, _log.LastIncludedIndex);
        Assert.Equal(1, _log.LastIncludedTerm);
        Assert.Equal(3, _log.FirstIndex);
        Assert.Equal(3, _log.LastIndex);
        Assert.Equal("snap"u8.ToArray(), _log.SnapshotBytes());
        Assert.Throws<InvalidOperationException>(() => _log.Read(2));
        Assert.Equal("c"u8.ToArray(), _log.Read(3).Command);
    }
}
