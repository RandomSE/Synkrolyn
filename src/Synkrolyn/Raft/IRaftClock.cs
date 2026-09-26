namespace Synkrolyn.Raft;

/// <summary>
/// Time source for election timers, leases, CheckQuorum, and delayed test delivery.
/// Implementations used for those bounds must be monotonic. Wall clocks are not used.
/// </summary>
public interface IRaftClock
{
    /// <summary>Elapsed milliseconds of this clock. Test fakes start at 0 unless constructed otherwise.</summary>
    long Millis { get; }

    /// <summary>
    /// Registers <paramref name="listener"/> to run after this clock advances.
    /// <see cref="SystemRaftClock"/> does not fire listeners; the runtime enqueues ticks.
    /// </summary>
    void OnAdvance(Action listener);
}
