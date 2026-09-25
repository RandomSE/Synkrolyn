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

    /// <summary>Ignores every command.</summary>
    static IStateMachine NoOp() => NoOpStateMachine.Instance;
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
