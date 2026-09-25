using Synkrolyn.Net;

namespace Synkrolyn.Raft;

/// <summary>
/// One Raft thread per node. I/O threads only enqueue. Production start uses election
/// jitter in [T, 2T] and a snapshot threshold of 4096. Tests use <see cref="ForTests"/>.
/// </summary>
public sealed class RaftRuntime : IDisposable
{
    /// <summary>Snapshot threshold applied by production start.</summary>
    public const int DefaultProductionSnapshotThreshold = 4096;

    private readonly RaftNode _node;
    private readonly SocketTransport _transport;
    private readonly IRaftClock _clock;
    private readonly bool _productionJitter;
    private readonly object _park = new();
    private volatile bool _running;
    private Thread? _raftThread;
    private Exception? _uncaught;

    /// <summary>Production runtime: jitter and the production snapshot threshold on start.</summary>
    public RaftRuntime(RaftNode node, SocketTransport transport, IRaftClock clock)
        : this(node, transport, clock, true)
    {
    }

    private RaftRuntime(RaftNode node, SocketTransport transport, IRaftClock clock, bool productionJitter)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        _productionJitter = productionJitter;
        _transport.SetWakeup(Wakeup);
    }

    /// <summary>Alias of the production constructor.</summary>
    public static RaftRuntime ForProduction(RaftNode node, SocketTransport transport, IRaftClock clock) =>
        new(node, transport, clock, true);

    /// <summary>Identity runtime. Socket and fake-clock tests must use this.</summary>
    public static RaftRuntime ForTests(RaftNode node, SocketTransport transport, IRaftClock clock) =>
        new(node, transport, clock, false);

    /// <summary>True when start applies production jitter.</summary>
    public bool ProductionJitterEnabled => _productionJitter;

    /// <summary>Maps timeout T to a value in [T, 2T].</summary>
    public static long ProductionJitter(long timeout)
    {
        if (timeout <= 0)
        {
            return timeout;
        }

        return timeout + Random.Shared.NextInt64(timeout + 1);
    }

    /// <summary>Uncaught exception from the Raft thread, if any.</summary>
    public Exception? Uncaught => _uncaught;

    /// <summary>Starts the Raft thread, apply executor, and socket transport.</summary>
    public void Start()
    {
        if (_productionJitter)
        {
            _node.SetElectionJitter(ProductionJitter);
            _node.SnapshotThreshold = DefaultProductionSnapshotThreshold;
        }

        _node.SetApplyExecutor(action => ThreadPool.QueueUserWorkItem(_ => action()));
        _node.SetSnapshotExecutor(action => ThreadPool.QueueUserWorkItem(_ => action()));
        _node.Start();
        _transport.SetHandler(_node.Receive);
        _transport.Start();
        _running = true;
        _raftThread = new Thread(Loop) { IsBackground = true, Name = "synkrolyn-raft-" + _node.NodeId };
        _raftThread.Start();
    }

    private void Loop()
    {
        try
        {
            while (_running)
            {
                _node.EnqueueTick();
                _node.Drain();
                _node.CheckQuorumAfterDrain();
                long wait = Math.Max(1, _node.NextDeadlineMillis() - _clock.Millis);
                wait = Math.Min(wait, 50);
                lock (_park)
                {
                    if (!_running)
                    {
                        return;
                    }

                    Monitor.Wait(_park, TimeSpan.FromMilliseconds(wait));
                }
            }
        }
        catch (Exception ex)
        {
            _uncaught = ex;
        }
    }

    private void Wakeup()
    {
        lock (_park)
        {
            Monitor.PulseAll(_park);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _running = false;
        Wakeup();
        _transport.Dispose();
        _raftThread?.Join(2000);
        GC.SuppressFinalize(this);
    }
}
