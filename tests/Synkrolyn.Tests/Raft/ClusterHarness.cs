using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

/// <summary>In-process cluster. Advances the fake clock, then drains every mailbox.</summary>
public sealed class ClusterHarness
{
    private readonly FakeClock _clock = new();
    private readonly InMemoryTransport _transport;
    private readonly Dictionary<string, RaftNode> _nodes = [];
    private readonly Dictionary<string, IPersistentState> _states = [];
    private readonly Dictionary<string, IRaftLog> _logs = [];

    public ClusterHarness()
    {
        _transport = new InMemoryTransport(_clock);
    }

    public FakeClock Clock => _clock;

    public InMemoryTransport Transport => _transport;

    public RaftNode AddNode(
        string id,
        IEnumerable<string> peers,
        TimeSpan electionTimeout,
        TimeSpan heartbeat,
        IStateMachine? stateMachine = null,
        IPersistentState? state = null,
        IRaftLog? log = null)
    {
        stateMachine ??= IStateMachine.NoOp();
        state ??= new InMemoryPersistentState();
        log ??= new InMemoryRaftLog();
        var node = new RaftNode(id, peers, _clock, _transport, state, log, electionTimeout, heartbeat, stateMachine);
        node.SetElectionJitter(static timeout => timeout);
        node.SnapshotThreshold = 0;
        _transport.Register(id, node.Receive);
        _nodes[id] = node;
        _states[id] = state;
        _logs[id] = log;
        node.Start();
        return node;
    }

    public RaftNode ReplaceNode(
        string id,
        IEnumerable<string> peers,
        TimeSpan electionTimeout,
        TimeSpan heartbeat,
        IStateMachine stateMachine,
        IPersistentState state,
        IRaftLog log)
    {
        Close(_states[id]);
        Close(_logs[id]);
        var node = new RaftNode(id, peers, _clock, _transport, state, log, electionTimeout, heartbeat, stateMachine);
        node.SetElectionJitter(static timeout => timeout);
        node.SnapshotThreshold = 0;
        _transport.Reregister(id, node.Receive);
        _nodes[id] = node;
        _states[id] = state;
        _logs[id] = log;
        node.Start();
        return node;
    }

    public RaftNode Node(string id) => _nodes[id];

    public IRaftLog Log(string id) => _logs[id];

    public IPersistentState State(string id) => _states[id];

    public void CloseDurableHandles()
    {
        foreach (IPersistentState state in _states.Values)
        {
            Close(state);
        }

        foreach (IRaftLog log in _logs.Values)
        {
            Close(log);
        }
    }

    public string? TakeLinearizableGet(KvClient client, string id, string key)
    {
        _nodes[id].BeginReadIndex();
        DrainAll();
        return client.LinearizableGet(key);
    }

    public void Advance(long millis)
    {
        _clock.Advance(millis);
        DrainAll();
        AssertAtMostOneLeaderPerTerm();
    }

    public void DrainAll()
    {
        bool progress;
        do
        {
            progress = false;
            foreach (RaftNode node in _nodes.Values)
            {
                if (node.Drain())
                {
                    progress = true;
                }
            }
        }
        while (progress);

        foreach (RaftNode node in _nodes.Values)
        {
            node.CheckQuorumAfterDrain();
        }

        AssertAtMostOneLeaderPerTerm();
    }

    public void StartElections(params string[] ids)
    {
        foreach (string id in ids)
        {
            _nodes[id].StartElection();
        }

        DrainAll();
    }

    public long? Propose(string id, byte[] command)
    {
        long? index = _nodes[id].Propose(command);
        DrainAll();
        return index;
    }

    public void Isolate(string id)
    {
        foreach (string other in _nodes.Keys)
        {
            if (other != id)
            {
                _transport.PartitionBidirectional(id, other);
            }
        }
    }

    public void Heal(string id)
    {
        foreach (string other in _nodes.Keys)
        {
            if (other != id)
            {
                _transport.HealBidirectional(id, other);
            }
        }
    }

    public IReadOnlyList<RaftNode> Leaders() => _nodes.Values.Where(n => n.Role == Role.Leader).ToList();

    public void AssertAtMostOneLeaderPerTerm()
    {
        var byTerm = new Dictionary<long, List<string>>();
        foreach (RaftNode node in _nodes.Values)
        {
            if (node.Role == Role.Leader)
            {
                if (!byTerm.TryGetValue(node.CurrentTerm, out List<string>? ids))
                {
                    ids = [];
                    byTerm[node.CurrentTerm] = ids;
                }

                ids.Add(node.NodeId);
            }
        }

        foreach ((long term, List<string> ids) in byTerm)
        {
            Assert.True(ids.Count <= 1, "leaders in term " + term + ": " + string.Join(",", ids));
        }
    }

    private static void Close(object handle)
    {
        if (handle is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}
