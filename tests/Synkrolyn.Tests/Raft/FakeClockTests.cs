using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

public class FakeClockTests
{
    [Fact]
    public void Advance_notifiesListenersOncePerCall()
    {
        var clock = new FakeClock();
        var hits = 0;
        clock.OnAdvance(() => hits++);
        Assert.Equal(0, clock.Millis);
        clock.Advance(0);
        Assert.Equal(0, hits);
        clock.Advance(15);
        Assert.Equal(15, clock.Millis);
        Assert.Equal(1, hits);
        Assert.Throws<ArgumentException>(() => clock.Advance(-1));
    }

    [Fact]
    public void SystemClock_isMonotonicAndDoesNotUseWallTime()
    {
        var clock = new SystemRaftClock();
        long a = clock.Millis;
        long b = clock.Millis;
        Assert.True(b >= a);
        string raftDir = Path.Combine("src", "Synkrolyn");
        if (!Directory.Exists(raftDir))
        {
            raftDir = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "src", "Synkrolyn"));
        }

        Assert.True(Directory.Exists(raftDir), raftDir);
        var banned = new[] { "DateTime.Now", "DateTime.UtcNow", "DateTimeOffset.Now", "DateTimeOffset.UtcNow" };
        var hits = new List<string>();
        foreach (string file in Directory.EnumerateFiles(raftDir, "*.cs", SearchOption.AllDirectories))
        {
            string text = File.ReadAllText(file);
            foreach (string token in banned)
            {
                if (text.Contains(token, StringComparison.Ordinal))
                {
                    hits.Add(file + ":" + token);
                }
            }
        }

        Assert.Empty(hits);
    }
}
