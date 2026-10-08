using System.Diagnostics;

namespace Synkrolyn.Raft;

/// <summary>
/// Monotonic clock based on <see cref="Stopwatch.GetTimestamp"/>. A pause moves
/// <see cref="Millis"/> forward, so leases expire instead of stretching.
/// Does not fire <see cref="IRaftClock.OnAdvance"/>; the runtime enqueues ticks.
/// </summary>
public sealed class SystemRaftClock : IRaftClock
{
    private readonly Func<long> _timestamp;
    private readonly long _origin;
    private long _lastMillis;

    /// <summary>Clock driven by <see cref="Stopwatch.GetTimestamp"/>.</summary>
    public SystemRaftClock()
        : this(Stopwatch.GetTimestamp)
    {
    }

    /// <summary>Clock driven by <paramref name="timestamp"/>, in the same units as <see cref="Stopwatch.GetTimestamp"/>.</summary>
    internal SystemRaftClock(Func<long> timestamp)
    {
        ArgumentNullException.ThrowIfNull(timestamp);
        _timestamp = timestamp;
        _origin = timestamp();
    }

    /// <inheritdoc />
    public long Millis
    {
        get
        {
            long now = (long)((_timestamp() - _origin) * 1000.0 / Stopwatch.Frequency);
            long last = Volatile.Read(ref _lastMillis);
            while (now > last)
            {
                long prev = Interlocked.CompareExchange(ref _lastMillis, now, last);
                if (prev == last)
                {
                    return now;
                }

                last = prev;
            }

            return last;
        }
    }

    /// <inheritdoc />
    public void OnAdvance(Action listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
    }
}
