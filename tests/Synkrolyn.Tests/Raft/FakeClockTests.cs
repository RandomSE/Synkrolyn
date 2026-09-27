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
    public void SystemClock_movesForwardAndIgnoresListeners()
    {
        var clock = new SystemRaftClock();
        int hits = 0;
        clock.OnAdvance(() => hits++);
        long first = clock.Millis;
        long previous = first;
        var started = System.Diagnostics.Stopwatch.StartNew();
        while (clock.Millis == first && started.Elapsed < TimeSpan.FromMilliseconds(50))
        {
            Thread.SpinWait(100);
        }

        long later = clock.Millis;
        Assert.True(later >= previous);
        Assert.True(later > first);
        Assert.Equal(0, hits);
    }
}
