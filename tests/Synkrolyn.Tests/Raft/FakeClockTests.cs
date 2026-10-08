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
    public void SystemClock_followsTheInjectedTimestampAndDoesNotMoveBackward()
    {
        long ticks = 1_000_000;
        var clock = new SystemRaftClock(() => ticks);
        int hits = 0;
        clock.OnAdvance(() => hits++);
        Assert.Equal(0, clock.Millis);
        ticks += System.Diagnostics.Stopwatch.Frequency;
        Assert.Equal(1000, clock.Millis);
        ticks -= System.Diagnostics.Stopwatch.Frequency / 2;
        Assert.Equal(1000, clock.Millis);
        Assert.Equal(0, hits);
    }
}
