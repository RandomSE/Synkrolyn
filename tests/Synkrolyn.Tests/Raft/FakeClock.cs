namespace Synkrolyn.Raft;

/// <summary>
/// Deterministic monotonic clock. <see cref="Advance"/> notifies listeners.
/// Unit tests must drive time through this type and must not sleep.
/// </summary>
public sealed class FakeClock : IRaftClock
{
    private readonly List<Action> _listeners = [];
    private long _millis;

    /// <summary>Starts at 0.</summary>
    public FakeClock()
        : this(0)
    {
    }

    /// <summary>Starts at <paramref name="startMillis"/>.</summary>
    public FakeClock(long startMillis)
    {
        if (startMillis < 0)
        {
            throw new ArgumentException("startMillis must be >= 0 (got " + startMillis + ")");
        }

        _millis = startMillis;
    }

    /// <inheritdoc />
    public long Millis => _millis;

    /// <summary>Moves forward by <paramref name="deltaMillis"/> and runs advance listeners once.</summary>
    public void Advance(long deltaMillis)
    {
        if (deltaMillis < 0)
        {
            throw new ArgumentException("Cannot advance clock by a negative amount: " + deltaMillis);
        }

        if (deltaMillis == 0)
        {
            return;
        }

        _millis = checked(_millis + deltaMillis);
        foreach (Action listener in _listeners.ToArray())
        {
            listener();
        }
    }

    /// <inheritdoc />
    public void OnAdvance(Action listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
        _listeners.Add(listener);
    }
}
