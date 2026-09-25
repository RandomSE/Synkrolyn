using System.Diagnostics;

namespace Synkrolyn.Raft;

/// <summary>
/// Monotonic clock based on <see cref="Stopwatch.GetTimestamp"/>. A pause moves
/// <see cref="Millis"/> forward, so leases expire instead of stretching.
/// Does not fire <see cref="IRaftClock.OnAdvance"/>; the runtime enqueues ticks.
/// </summary>
public sealed class SystemRaftClock : IRaftClock
{
    private readonly long _origin = Stopwatch.GetTimestamp();

    /// <inheritdoc />
    public long Millis =>
        (long)((Stopwatch.GetTimestamp() - _origin) * 1000.0 / Stopwatch.Frequency);

    /// <inheritdoc />
    public void OnAdvance(Action listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
    }
}
