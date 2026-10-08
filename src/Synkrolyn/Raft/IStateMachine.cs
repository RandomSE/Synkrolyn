namespace Synkrolyn.Raft;

/// <summary>
/// Applied committed log entries. Raft treats the command as opaque bytes.
/// </summary>
public interface IStateMachine
{
    /// <summary>Apply the command at <paramref name="index"/> at most once, in order, only while committed.</summary>
    void Apply(long index, byte[] command);

    /// <summary>Bytes of applied state through the last applied index.</summary>
    byte[] Snapshot() => [];

    /// <summary>Replaces applied state. Must not merge with existing keys.</summary>
    void Restore(byte[] snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
    }

    /// <summary>
    /// Restore <paramref name="snapshot"/>, which may be a key diff.
    /// The default delegates to <see cref="Restore"/>.
    /// </summary>
    void RestoreChecked(long lastIncludedIndex, long lastApplied, byte[] snapshot) => Restore(snapshot);

    /// <summary>Key diff versus the last full snapshot. The default is a full snapshot.</summary>
    byte[] SnapshotDelta(long baseIndex) => Snapshot();

    /// <summary>
    /// O(1) handle the Raft thread can take before serialization. The default encodes later
    /// by calling <see cref="Snapshot"/> on whatever thread consumes the handle.
    /// </summary>
    StateCapture CaptureState() => StateCapture.Deferred(this);

    /// <summary>Ignores every command.</summary>
    static IStateMachine NoOp() => NoOpStateMachine.Instance;
}

/// <summary>Immutable view of applied state. Encoding it must not block later applies.</summary>
public sealed class StateCapture
{
    private readonly Func<byte[]> _full;
    private readonly Func<long, byte[]> _delta;
    private readonly Action? _commitBase;

    private StateCapture(Func<byte[]> full, Func<long, byte[]> delta, Action? commitBase)
    {
        _full = full;
        _delta = delta;
        _commitBase = commitBase;
    }

    /// <summary>Encode by calling the live machine when the bytes are needed.</summary>
    public static StateCapture Deferred(IStateMachine machine)
    {
        ArgumentNullException.ThrowIfNull(machine);
        return new StateCapture(() => machine.Snapshot(), machine.SnapshotDelta, null);
    }

    /// <summary>Capture whose encode functions do not touch the live machine.</summary>
    public static StateCapture Frozen(Func<byte[]> full, Func<long, byte[]> delta, Action? commitBase)
    {
        ArgumentNullException.ThrowIfNull(full);
        ArgumentNullException.ThrowIfNull(delta);
        return new StateCapture(full, delta, commitBase);
    }

    /// <summary>Full snapshot bytes. Safe off the Raft thread when the capture is frozen.</summary>
    public byte[] Full() => _full();

    /// <summary>Delta versus <paramref name="baseIndex"/>.</summary>
    public byte[] Delta(long baseIndex) => _delta(baseIndex);

    /// <summary>Publishes the delta base. Called on the Raft thread after the snapshot commits.</summary>
    public void CommitBase() => _commitBase?.Invoke();
}

/// <summary>State machine that ignores commands.</summary>
public sealed class NoOpStateMachine : IStateMachine
{
    /// <summary>Shared instance.</summary>
    public static readonly NoOpStateMachine Instance = new();

    private NoOpStateMachine()
    {
    }

    /// <inheritdoc />
    public void Apply(long index, byte[] command)
    {
    }
}
