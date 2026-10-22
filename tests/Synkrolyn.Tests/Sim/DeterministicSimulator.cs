using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Sim;

/// <summary>One client operation edge. Phase 6 can check linearizability from this list.</summary>
public readonly record struct ClientHistoryEntry(
    long TimeMillis,
    int Client,
    string Op,
    string Phase,
    string Key,
    string Result);

/// <summary>One seeded run. <see cref="TraceHash"/> is the event trace, not a summary.</summary>
public sealed class SimResult
{
    public required string TraceHash { get; init; }

    public required bool LeaderElected { get; init; }

    public required bool WriteCommitted { get; init; }

    /// <summary>Steps requested for this run.</summary>
    public required int Steps { get; init; }

    /// <summary>Steps with faults on. This is 80 percent of <see cref="Steps"/>.</summary>
    public required int FaultSteps { get; init; }

    /// <summary>One leader id stays in office after the post-heal grace.</summary>
    public required bool StableLeader { get; init; }

    /// <summary>Every live member's commit index has reached the leader's.</summary>
    public required bool CommitsAligned { get; init; }

    /// <summary>Every post-heal client operation completed or failed within the bound.</summary>
    public required bool PendingSettled { get; init; }

    /// <summary>Live members hold equal state-machine bytes.</summary>
    public required bool ReplicasEqual { get; init; }

    public required IReadOnlyList<ClientHistoryEntry> History { get; init; }

    /// <summary>True after a scripted full-voter crash kept every committed index.</summary>
    public bool CommittedPrefixKept { get; init; }

    /// <summary>
    /// True when a paused or clock-stepped former leader refused a lease read
    /// after another node had become leader.
    /// </summary>
    public bool StaleLeaseRefused { get; init; }
}

/// <summary>
/// One thread, one seed. Message delay, drop, duplicate, reorder, partition,
/// crash, and clock rate all come from that seed.
/// </summary>
public static class DeterministicSimulator
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);

    public static string ReproLine(int seed) =>
        "SIM_SEED=" + seed + " dotnet test tests/Synkrolyn.Tests/Synkrolyn.Tests.csproj -c Release --filter FullyQualifiedName~SimSeed_replaysEnvironmentSeed";

    /// <summary>Push and nightly depth. <c>SIM_STEPS</c> overrides the default of 2000.</summary>
    public static int CiSteps() => PositiveEnv("SIM_STEPS", 2000);

    /// <summary>Push seed count. <c>SIM_COUNT</c> overrides the default of 200. Nightly sets 10000.</summary>
    public static int CiSeeds() => PositiveEnv("SIM_COUNT", 200);

    private static int PositiveEnv(string name, int fallback)
    {
        string? raw = Environment.GetEnvironmentVariable(name);
        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out int value) && value > 0
            ? value
            : fallback;
    }

    public static SimResult Run(int seed, int steps) => Run(seed, steps, staleReads: false);

    /// <summary>
    /// Quiet commit, then every voter crashes with an unflushed suffix and restarts
    /// from its durable log. The history is the caller's linearizability input.
    /// </summary>
    public static SimResult RunFullClusterCrash(int seed)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(seed);
        var world = new World(seed, steps: 40, staleReads: false);
        world.ExecuteFullCrash();
        return world.Result();
    }

    /// <summary>
    /// Commits a value, pauses the leader past the election timeout, then steps
    /// that node's clock backward while a new leader exists. The history is the
    /// caller's linearizability input.
    /// </summary>
    public static SimResult RunClockAndPause(int seed)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(seed);
        var world = new World(seed, steps: 60, staleReads: false);
        world.ExecuteClockAndPause(500 + (seed * 50), 1_000L * seed);
        return world.Result();
    }

    /// <summary>
    /// <paramref name="staleReads"/> serves gets from a live node's local map.
    /// That skips ReadIndex and the lease. The linearizability checker should reject it.
    /// </summary>
    public static SimResult Run(int seed, int steps, bool staleReads)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(steps);

        var world = new World(seed, steps, staleReads);
        world.Execute();
        return world.Result();
    }

    private sealed class World
    {
        private readonly SimRng _rng;
        private const int LeaderGrace = 80;
        private const int PendingBound = 80;

        private readonly int _steps;
        private readonly int _healAt;
        private readonly bool _staleReads;
        private readonly SimNet _net;
        private readonly List<SimNode> _nodes;
        private readonly List<SimClient> _clients = [];
        private readonly List<ClientHistoryEntry> _history = [];
        private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        private readonly Queue<string> _recent = new();
        private readonly Dictionary<long, string> _termLeader = [];
        private readonly Dictionary<long, (long Term, int Hash)> _committed = [];
        private readonly Dictionary<long, (long Term, int Hash)> _applied = [];
        private readonly Dictionary<string, (long Term, List<EntryMark> Prefix)> _leaderPrefix = [];
        private readonly List<string?> _healedLeaders = [];
        private readonly Dictionary<int, Pending> _pending = [];
        private readonly Dictionary<int, (string Key, string Value)> _openPut = [];
        private readonly Dictionary<string, string> _ackedValues = [];
        private readonly Dictionary<(string Voter, long Term), string> _grants = [];
        private bool _writeCommitted;
        private bool _committedPrefixKept;
        private bool _staleLeaseRefused;
        private bool _healed;
        private bool _pendingSettled = true;
        private string? _safetyFailure;

        public World(int seed, int steps, bool staleReads)
        {
            _rng = new SimRng((ulong)(uint)seed + 1UL);
            _steps = steps;
            _staleReads = staleReads;
            _healAt = steps * 4 / 5;
            _net = new SimNet(_rng);
            _net.CrashSends = true;
            _net.OnSent = ObserveSend;
            _net.OnSenderCrash = CrashSender;
            _nodes =
            [
                Make("n1", ["n2", "n3"], 80, 100),
                Make("n2", ["n1", "n3"], 140, 100),
                Make("n3", ["n1", "n2"], 200, 100),
                Make("n4", ["n1", "n2", "n3"], 10_000, 100),
            ];
            foreach (SimNode node in _nodes)
            {
                _net.Known(node.Id);
                Boot(node);
            }

            for (int i = 0; i < 2; i++)
            {
                _clients.Add(new SimClient(i, new KvClient(_nodes[0].Raft!, _nodes[0].Store)));
            }
        }

        public void Execute()
        {
            Note("seed-steps " + _steps);
            Note("fault-steps " + _healAt);
            Drain();
            for (int step = 0; step < _steps; step++)
            {
                if (step == _healAt)
                {
                    Heal();
                }

                try
                {
                    if (step >= _healAt)
                    {
                        AdvanceSettled();
                        SettlePending(step);
                        if (step < _steps - PendingBound && step % 3 == 0)
                        {
                            ClientPut(step);
                        }
                        else
                        {
                            Note(step + " advance");
                        }

                        ObserveLeader();
                    }
                    else
                    {
                        Act(step, faults: true);
                    }
                }
                catch (SimCrash)
                {
                    ReapAll();
                }

                Drain();
                ThrowSafety();
                Check(step);
            }

            if (_pending.Count > 0)
            {
                _pendingSettled = false;
            }
        }

        public SimResult Result()
        {
            bool leader = _nodes.Any(static node => node.Live && node.Raft!.Role == Role.Leader);
            bool equal = false;
            bool aligned = CommitsAligned(out equal);
            return new SimResult
            {
                TraceHash = Convert.ToHexString(_hash.GetHashAndReset()),
                LeaderElected = leader,
                WriteCommitted = _writeCommitted,
                Steps = _steps,
                FaultSteps = _healAt,
                StableLeader = StableLeader(),
                CommitsAligned = aligned,
                PendingSettled = _pendingSettled,
                ReplicasEqual = equal,
                History = _history,
                CommittedPrefixKept = _committedPrefixKept,
                StaleLeaseRefused = _staleLeaseRefused,
            };
        }

        public void ExecuteClockAndPause(int pauseMs, long backwardMs)
        {
            _net.CrashSends = false;
            Note("clock-pause " + pauseMs.ToString(CultureInfo.InvariantCulture));
            Drain();
            for (int step = 0; step < 40 && !_writeCommitted; step++)
            {
                Advance(20);
                Drain();
                PutOnce(_clients[0], step, "k0", "before-pause");
                Drain();
                Check(step);
            }

            if (!_writeCommitted)
            {
                Fail("nothing committed before the clock faults");
            }

            for (int i = 0; i < 30 && LeaderWithLease() is null; i++)
            {
                Advance(20);
                Drain();
            }

            if (LeaderWithLease() is not SimNode leader)
            {
                Fail("no lease before the pause");
                return;
            }

            Freeze(leader);
            int slices = Math.Max(1, pauseMs / 20);
            for (int i = 0; i < slices; i++)
            {
                Advance(20);
                Drain();
            }

            if (OtherLeader(leader) is not SimNode elected)
            {
                Fail("no leader while " + leader.Id + " was paused");
                return;
            }

            leader.Clock.Jump(pauseMs);
            if (ServesLease(leader))
            {
                Fail("paused leader " + leader.Id + " served a lease read after " + elected.Id);
            }

            Thaw(leader);
            _net.DropQueued();
            _net.ClearFaults();
            for (int i = 0; i < 40 && LeaderWithLease() is null; i++)
            {
                Advance(20);
                Drain();
            }

            if (LeaderWithLease() is not SimNode stepped)
            {
                Fail("no lease before the backward step");
                return;
            }

            Freeze(stepped);
            Isolate(stepped);
            _net.DropQueued();
            for (int i = 0; i < slices; i++)
            {
                Advance(20);
                Drain();
            }

            if (OtherLeader(stepped) is not SimNode after)
            {
                Fail("no leader after stepping " + stepped.Id);
                return;
            }

            stepped.Clock.Jump(-backwardMs);
            if (ServesLease(stepped))
            {
                Fail("stepped leader " + stepped.Id + " served a lease read after " + after.Id);
            }

            _staleLeaseRefused = true;
            Thaw(stepped);
            _net.ClearFaults();
            _net.DropQueued();
            for (int i = 0; i < 20; i++)
            {
                Advance(20);
                Drain();
            }

            for (int step = 0; step < 16; step++)
            {
                if (_rng.Next(2) == 0)
                {
                    SimNode node = _nodes[_rng.Next(3)];
                    if (node.Live && !node.Frozen)
                    {
                        long delta = _rng.Next(2) == 0 ? 40 + _rng.Next(200) : -(40 + _rng.Next(200));
                        node.Clock.Jump(delta);
                        Note(step + " jump " + node.Id + " " + delta.ToString(CultureInfo.InvariantCulture));
                        if (delta < 0 && OtherLeader(node) is SimNode other && ServesLease(node))
                        {
                            Fail("stepped leader " + node.Id + " served a lease read after " + other.Id);
                        }
                    }
                }
                else if (step % 2 == 0)
                {
                    ClientPut(step);
                }
                else
                {
                    ClientGet(step);
                }

                Drain();
                Check(step);
            }
        }

        public void ExecuteFullCrash()
        {
            _net.CrashSends = false;
            Note("full-crash");
            Drain();
            for (int step = 0; step < 40 && !_writeCommitted; step++)
            {
                Advance(20);
                Drain();
                ClientPut(step);
                Drain();
                Check(step);
            }

            if (!_writeCommitted || _committed.Count == 0)
            {
                Fail("nothing committed before the full crash");
            }

            var kept = new Dictionary<long, (long Term, int Hash)>(_committed);
            CrashAllVoters();
            foreach (SimNode node in _nodes)
            {
                if (node.Id == "n4")
                {
                    continue;
                }

                Boot(node);
            }

            _net.ClearFaults();
            _healed = true;
            for (int step = 0; step < 400 && LiveLeader() is null; step++)
            {
                Advance(20);
                Drain();
                if (step % 20 == 0)
                {
                    Check(step);
                }
            }

            if (LiveLeader() is null)
            {
                var detail = new StringBuilder();
                foreach (SimNode node in _nodes)
                {
                    if (node.Frozen || node.Raft is null)
                    {
                        continue;
                    }

                    detail.Append(node.Id);
                    detail.Append(" term ");
                    detail.Append(node.Raft.CurrentTerm);
                    detail.Append(" deadline ");
                    detail.Append(node.Raft.NextDeadlineMillis());
                    detail.Append(" clock ");
                    detail.Append(node.Clock.Millis);
                    detail.Append(" prevote ");
                    detail.Append(node.Raft.LastPreVoteGranted);
                    detail.Append("; ");
                }

                Fail("no leader after full restart " + detail);
            }

            foreach ((long index, (long Term, int Hash) entry) in kept)
            {
                int copies = 0;
                foreach (SimNode node in _nodes)
                {
                    if (node.Id != "n4" && Covers(node, index, entry.Term, entry.Hash))
                    {
                        copies++;
                    }
                }

                if (copies < 2)
                {
                    Fail("lost committed index " + index.ToString(CultureInfo.InvariantCulture));
                }
            }

            ReadBackCommittedPuts();
            _committedPrefixKept = true;
        }

        private void CrashAllVoters()
        {
            foreach (SimNode node in _nodes)
            {
                if (node.Id == "n4" || node.Raft is null || !node.Raft.IsVoter)
                {
                    continue;
                }

                node.Raft.StopAfterRaftThreadFault();
                node.Clock.Clear();
                long term = node.State.CurrentTerm;
                long next = node.Log.LastIndex + 1;
                long flushed = node.Log.FlushedThrough;
                node.Log.Append(new LogEntry(next, term, KvCommandCodec.EncodePut("torn", "x")));
                node.Log.Append(new LogEntry(next + 1, term, KvCommandCodec.EncodePut("torn", "y")));
                long extended = node.Log.LastIndex;
                node.Log.LoseUnflushed(_rng.Next(2));
                if (node.Log.LastIndex >= extended || node.Log.LastIndex < flushed)
                {
                    Fail("crash did not trim the unflushed suffix on " + node.Id);
                }

                node.State.LoseUnflushed();
                node.Store = new InMemoryKvStore();
                node.Crashed = true;
                node.PendingCrash = false;
                node.Raft = null;
                _net.Bind(node.Id, null);
                Note("crash-all " + node.Id);
            }

            _net.DropQueued();
        }

        private void ReadBackCommittedPuts()
        {
            SimNode? leader = LiveLeader();
            if (leader?.Raft is null)
            {
                Fail("no leader for the read-back");
                return;
            }

            var reader = new SimClient(7, new KvClient(leader.Raft, leader.Store));
            for (int step = 0; step < 8 && reader.Kv.ClientId == 0; step++)
            {
                AdvanceSettled();
                Drain();
                Bind(reader);
                EnsureSession(reader);
            }

            if (reader.Kv.ClientId == 0)
            {
                Fail("session did not register after full restart");
            }

            foreach ((string key, string value) in _ackedValues)
            {
                string result = "unknown";
                for (int step = 0; step < 8 && result != value; step++)
                {
                    AdvanceSettled();
                    Drain();
                    Bind(reader);
                    Record(reader.Id, "get", "invoke", key, "");
                    result = LinearizableRead(reader, key);
                    Record(reader.Id, "get", "complete", key, result);
                }

                if (result != value)
                {
                    Fail("lost committed value " + key + " saw " + result);
                }
            }
        }

        private static SimNode Make(string id, string[] peers, int electionMs, int rate) =>
            new(id, peers, TimeSpan.FromMilliseconds(electionMs), rate);

        private void Boot(SimNode node)
        {
            node.Clock.Clear();
            node.Raft = new RaftNode(
                node.Id,
                node.Peers,
                node.Clock,
                _net,
                node.State,
                node.Log,
                node.Election,
                Heartbeat,
                node.Store);
            node.Raft.SetElectionJitter(static timeout => timeout);
            node.Raft.SnapshotThreshold = 0;
            _net.Bind(node.Id, node.Raft);
            node.Raft.Start();
            node.Crashed = false;
            node.Generation++;
            node.CommitSeen = node.Raft.CommitIndex;
            node.CommitGeneration = node.Generation;
        }

        private void Act(int step, bool faults)
        {
            int kind = _rng.Next(10);
            if (!faults && kind >= 8)
            {
                kind = 3;
            }

            switch (kind)
            {
                case 0:
                case 1:
                case 2:
                    Advance(5 + _rng.Next(20));
                    Note(step + " advance");
                    break;
                case 3:
                    ClientPut(step);
                    break;
                case 4:
                    ClientGet(step);
                    break;
                case 5:
                    Membership(step);
                    break;
                case 6:
                    Transfer(step);
                    break;
                case 7:
                    Snap(step);
                    break;
                case 8:
                    CrashOrRestart(step);
                    break;
                default:
                    FaultNetwork(step);
                    break;
            }
        }

        private void Advance(int delta)
        {
            _net.Now += delta;
            _net.DeliverDue();
            foreach (SimNode node in _nodes)
            {
                if (node.Frozen)
                {
                    continue;
                }

                node.Clock.AdvanceScaled(delta, node.RatePercent);
            }
        }

        private void Freeze(SimNode node)
        {
            node.Frozen = true;
            _net.SetFrozen(node.Id, true);
            _net.DropQueued();
            Note("freeze " + node.Id);
        }

        private void Thaw(SimNode node)
        {
            node.Frozen = false;
            _net.SetFrozen(node.Id, false);
            Note("thaw " + node.Id);
        }

        private void Isolate(SimNode node)
        {
            foreach (SimNode other in _nodes)
            {
                if (other.Id != node.Id)
                {
                    _net.TogglePartition(node.Id, other.Id);
                }
            }

            Note("isolate " + node.Id);
        }

        private SimNode? LeaderWithLease()
        {
            foreach (SimNode node in _nodes)
            {
                if (!node.Live || node.Frozen)
                {
                    continue;
                }

                RaftNode raft = node.Raft!;
                if (raft.Role == Role.Leader && (raft.ReadIndexSatisfied() || raft.QuorumLeaseValid))
                {
                    return node;
                }
            }

            return null;
        }

        private SimNode? OtherLeader(SimNode except)
        {
            foreach (SimNode node in _nodes)
            {
                if (node.Id == except.Id || !node.Live || node.Frozen)
                {
                    continue;
                }

                if (node.Raft!.Role == Role.Leader)
                {
                    return node;
                }
            }

            return null;
        }

        private static bool ServesLease(SimNode node)
        {
            RaftNode raft = node.Raft!;
            if (raft.ReadIndexSatisfied() || raft.QuorumLeaseValid || raft.FollowerReadLeaseValid)
            {
                return true;
            }

            var client = new KvClient(raft, node.Store);
            return client.LinearizableGet("k0") is not null || client.BoundedStaleGet("k0") is not null;
        }

        private void AdvanceSettled()
        {
            for (int slice = 0; slice < 12; slice++)
            {
                Advance(30);
                Drain();
            }
        }

        private void Heal()
        {
            _healed = true;
            _net.ClearFaults();
            foreach (SimNode node in _nodes)
            {
                node.RatePercent = 100;
                if (node.Crashed)
                {
                    Boot(node);
                }
            }

            Note("heal");
        }

        private void CrashOrRestart(int step)
        {
            SimNode node = _nodes[_rng.Next(_nodes.Count)];
            if (!node.Crashed && !node.PendingCrash && LiveCount() > 2)
            {
                LoseVolatile(node);
                node.Raft?.StopAfterRaftThreadFault();
                node.Clock.Clear();
                node.Crashed = true;
                node.Raft = null;
                _net.Bind(node.Id, null);
                Note(step + " crash " + node.Id);
                return;
            }

            if (node.Crashed)
            {
                Boot(node);
                Note(step + " restart " + node.Id);
                return;
            }

            Note(step + " crash-skip " + node.Id);
        }

        private void FaultNetwork(int step)
        {
            int which = _rng.Next(3);
            if (which == 0)
            {
                SimNode a = _nodes[_rng.Next(_nodes.Count)];
                SimNode b = _nodes[_rng.Next(_nodes.Count)];
                if (a.Id != b.Id)
                {
                    _net.TogglePartition(a.Id, b.Id);
                }

                Note(step + " partition " + a.Id + " " + b.Id);
                return;
            }

            if (which == 1)
            {
                SimNode node = _nodes[_rng.Next(_nodes.Count)];
                node.RatePercent = 50 + _rng.Next(101);
                Note(step + " skew " + node.Id + " " + node.RatePercent);
                return;
            }

            _net.DropPercent = _rng.Next(31);
            _net.DupPercent = _rng.Next(21);
            _net.MaxDelay = _rng.Next(41);
            Note(step + " link " + _net.DropPercent + " " + _net.DupPercent + " " + _net.MaxDelay);
        }

        private void ClientPut(int step)
        {
            SimClient client = _clients[_rng.Next(_clients.Count)];
            if (_pending.ContainsKey(client.Id))
            {
                Note(step + " put waiting " + client.Id);
                return;
            }

            string key = "k" + _rng.Next(3);
            string value = "v" + client.Id + "-" + step;
            if (_openPut.TryGetValue(client.Id, out (string Key, string Value) open))
            {
                key = open.Key;
                value = open.Value;
            }

            if (!_healed)
            {
                PutOnce(client, step, key, value);
                return;
            }

            Record(client.Id, "put", "invoke", key, value);
            string? outcome = Outcome(client, key, value);
            TrackOpen(client.Id, key, value, outcome);
            if (outcome is null)
            {
                _pending[client.Id] = new Pending(key, value, step);
                Note(step + " put pending " + client.Id);
                return;
            }

            CompletePut(client.Id, key, outcome);
            Note(step + " put " + client.Id + " " + key + " " + outcome);
        }

        private void PutOnce(SimClient client, int step, string key, string value)
        {
            if (!Bind(client))
            {
                string missed = _openPut.ContainsKey(client.Id) ? "null" : "no-leader";
                Record(client.Id, "put", "invoke", key, value);
                Record(client.Id, "put", "complete", key, missed);
                TrackOpen(client.Id, key, value, missed);
                Note(step + " put " + missed);
                return;
            }

            EnsureSession(client);
            Record(client.Id, "put", "invoke", key, value);
            try
            {
                client.Kv.Put(key, value);
                Drain();
                long? done = client.Kv.AwaitCommitted();
                string text = done is null ? "null" : done.Value.ToString(CultureInfo.InvariantCulture);
                TrackOpen(client.Id, key, value, text);
                if (done is not null)
                {
                    _ackedValues[key] = value;
                    _writeCommitted = true;
                }

                Record(client.Id, "put", "complete", key, text);
                Note(step + " put " + client.Id + " " + key + " " + text);
            }
            catch (SimCrash)
            {
                TrackOpen(client.Id, key, value, "null");
                Record(client.Id, "put", "complete", key, "null");
                Note(step + " put crash " + client.Id + " " + key);
                throw;
            }
        }

        private void SettlePending(int step)
        {
            var finished = new List<int>();
            foreach ((int id, Pending op) in _pending)
            {
                string? outcome = Outcome(_clients[id], op.Key, op.Value);
                if (outcome is null)
                {
                    if (step - op.Since >= PendingBound)
                    {
                        _pendingSettled = false;
                        TrackOpen(id, op.Key, op.Value, "unsettled");
                        Record(id, "put", "complete", op.Key, "unsettled");
                        finished.Add(id);
                        Note(step + " unsettled " + id);
                    }

                    continue;
                }

                TrackOpen(id, op.Key, op.Value, outcome);
                CompletePut(id, op.Key, outcome);
                finished.Add(id);
                Note(step + " settle " + id + " " + outcome);
            }

            foreach (int id in finished)
            {
                _pending.Remove(id);
            }
        }

        private void TrackOpen(int client, string key, string value, string? outcome)
        {
            if (outcome is null or "null" or "unsettled")
            {
                _openPut[client] = (key, value);
                return;
            }

            _openPut.Remove(client);
        }

        private void CompletePut(int client, string key, string outcome)
        {
            Record(client, "put", "complete", key, outcome);
            if (long.TryParse(outcome, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            {
                _writeCommitted = true;
            }
        }

        private string? Outcome(SimClient client, string key, string value)
        {
            if (Definitive(client) is string failed)
            {
                return failed;
            }

            if (!Bind(client))
            {
                return null;
            }

            EnsureSession(client);
            long? done = client.Kv.AwaitCommitted();
            if (done is null)
            {
                done = client.Kv.Put(key, value);
            }

            Drain();
            done ??= client.Kv.AwaitCommitted();
            if (done is not null)
            {
                return done.Value.ToString(CultureInfo.InvariantCulture);
            }

            return Definitive(client);
        }

        private static string? Definitive(SimClient client)
        {
            if (client.Kv.LastError is KvCommandError.SessionExpired or KvCommandError.OutOfOrder)
            {
                return client.Kv.LastError.ToString();
            }

            return null;
        }

        private void ObserveLeader()
        {
            _healedLeaders.Add(LiveLeader()?.Id);
        }

        private bool StableLeader()
        {
            if (_healedLeaders.Count <= LeaderGrace)
            {
                return false;
            }

            string? id = null;
            for (int i = LeaderGrace; i < _healedLeaders.Count; i++)
            {
                string? current = _healedLeaders[i];
                if (current is null)
                {
                    return false;
                }

                if (id is null)
                {
                    id = current;
                }
                else if (!string.Equals(id, current, StringComparison.Ordinal))
                {
                    return false;
                }
            }

            if (id is null)
            {
                return false;
            }

            return true;
        }

        private bool CommitsAligned(out bool equal)
        {
            equal = false;
            SimNode? leader = LiveLeader();
            if (leader is null)
            {
                return false;
            }

            long commit = leader.Raft!.CommitIndex;
            byte[]? snapshot = null;
            bool aligned = true;
            equal = true;
            foreach (SimNode node in Members(leader))
            {
                if (!node.Live)
                {
                    continue;
                }

                if (node.Raft!.CommitIndex < commit)
                {
                    aligned = false;
                }

                byte[] bytes = node.Store.Snapshot();
                if (snapshot is null)
                {
                    snapshot = bytes;
                }
                else if (!bytes.AsSpan().SequenceEqual(snapshot))
                {
                    equal = false;
                }
            }

            return aligned && snapshot is not null;
        }

        private List<SimNode> Members(SimNode leader)
        {
            var ids = new HashSet<string>(StringComparer.Ordinal) { leader.Id };
            foreach (string id in leader.Raft!.VoterPeerIds)
            {
                ids.Add(id);
            }

            foreach (string id in leader.Raft.LearnerPeerIds)
            {
                ids.Add(id);
            }

            var list = new List<SimNode>();
            foreach (SimNode node in _nodes)
            {
                if (ids.Contains(node.Id))
                {
                    list.Add(node);
                }
            }

            return list;
        }

        private void ObserveSend(string sender, string recipient, object payload)
        {
            SimNode node = Node(sender);
            if (payload is RequestVoteResponse vote && vote.VoteGranted && !vote.PreVote)
            {
                if (!string.Equals(node.State.VotedFor, recipient, StringComparison.Ordinal))
                {
                    _safetyFailure ??= "vote grant before flush " + sender + " term " + vote.Term.ToString(CultureInfo.InvariantCulture);
                }

                (string Voter, long Term) key = (sender, vote.Term);
                if (_grants.TryGetValue(key, out string? prior) && !string.Equals(prior, recipient, StringComparison.Ordinal))
                {
                    _safetyFailure ??= "double vote " + sender + " term " + vote.Term.ToString(CultureInfo.InvariantCulture);
                }
                else
                {
                    _grants[key] = recipient;
                }
            }

            if (payload is AppendEntriesResponse append && append.Success && append.MatchIndex > node.Log.FlushedThrough)
            {
                _safetyFailure ??= "append ack before flush " + sender + " index " + append.MatchIndex.ToString(CultureInfo.InvariantCulture);
            }
        }

        private void CrashSender(string id)
        {
            SimNode node = Node(id);
            if (node.Crashed || node.PendingCrash || node.Raft is null)
            {
                return;
            }

            LoseVolatile(node);
            node.PendingCrash = true;
            _net.Bind(node.Id, null);
        }

        private void LoseVolatile(SimNode node)
        {
            node.Log.LoseUnflushed(_rng.Next(2));
            node.State.LoseUnflushed();
            node.Store = new InMemoryKvStore();
        }

        private void Reap(SimNode node)
        {
            node.Raft?.StopAfterRaftThreadFault();
            node.PendingCrash = false;
            node.Clock.Clear();
            node.Crashed = true;
            node.Raft = null;
            _net.Bind(node.Id, null);
            Note("crash-send " + node.Id);
        }

        private void ReapAll()
        {
            foreach (SimNode node in _nodes)
            {
                if (node.PendingCrash)
                {
                    Reap(node);
                }
            }
        }

        private void ThrowSafety()
        {
            if (_safetyFailure is not null)
            {
                Fail(_safetyFailure);
            }
        }

        private SimNode Node(string id)
        {
            foreach (SimNode node in _nodes)
            {
                if (node.Id == id)
                {
                    return node;
                }
            }

            throw new InvalidOperationException("missing node " + id);
        }

        private void ClientGet(int step)
        {
            SimClient client = _clients[_rng.Next(_clients.Count)];
            string key = "k" + _rng.Next(3);
            if (!Bind(client))
            {
                Record(client.Id, "get", "invoke", key, "");
                Record(client.Id, "get", "complete", key, "no-leader");
                Note(step + " get no-leader");
                return;
            }

            EnsureSession(client);
            Record(client.Id, "get", "invoke", key, "");
            string result;
            try
            {
                result = _staleReads ? StaleRead(key) : LinearizableRead(client, key);
            }
            catch (KvSessionException)
            {
                result = "expired";
            }
            catch (SimCrash)
            {
                Record(client.Id, "get", "complete", key, "unknown");
                Note(step + " get crash " + client.Id + " " + key);
                throw;
            }

            Record(client.Id, "get", "complete", key, result);
            Note(step + " get " + client.Id + " " + key + " " + result);
        }

        private string LinearizableRead(SimClient client, string key)
        {
            string? value = client.Kv.LinearizableGet(key);
            if (value is not null)
            {
                return value;
            }

            SimNode? leader = LiveLeader();
            if (leader?.Raft is RaftNode raft && raft.Role == Role.Leader && raft.ReadIndexSatisfied())
            {
                return "absent";
            }

            return "unknown";
        }

        private string StaleRead(string key)
        {
            int start = _rng.Next(_nodes.Count);
            for (int i = 0; i < _nodes.Count; i++)
            {
                SimNode node = _nodes[(start + i) % _nodes.Count];
                if (node.Live)
                {
                    return node.Store.Get(key) ?? "absent";
                }
            }

            return "unknown";
        }

        private void EnsureSession(SimClient client)
        {
            if (client.Kv.ClientId != 0)
            {
                return;
            }

            Record(client.Id, "register", "invoke", "", "");
            client.Kv.Register(TimeSpan.FromMinutes(5));
            Drain();
            Record(client.Id, "register", "complete", "", client.Kv.ClientId == 0 ? "null" : client.Kv.ClientId.ToString(CultureInfo.InvariantCulture));
        }

        private bool Bind(SimClient client)
        {
            SimNode? leader = LiveLeader();
            if (leader is null)
            {
                return false;
            }

            client.Kv.Follow(leader.Raft!, leader.Store);
            return true;
        }

        private void Membership(int step)
        {
            SimNode? leader = LiveLeader();
            if (leader is null)
            {
                Note(step + " member no-leader");
                return;
            }

            RaftNode node = leader.Raft!;
            int kind = _rng.Next(4);
            if (kind == 0)
            {
                node.AddLearner("n4");
                Note(step + " add-learner");
            }
            else if (kind == 1)
            {
                node.PromoteVoter("n4");
                Note(step + " promote");
            }
            else if (kind == 2)
            {
                string target = _rng.Next(2) == 0 ? "n4" : PickPeer(node);
                node.RemoveServer(target);
                Note(step + " remove " + target);
            }
            else if (node.InJointConsensus)
            {
                node.LeaveJoint();
                Note(step + " leave-joint");
            }
            else
            {
                var voters = new List<string> { node.NodeId };
                voters.AddRange(node.VoterPeerIds);
                voters.Sort(StringComparer.Ordinal);
                if (voters.Count >= 3 && !voters.Contains("n4", StringComparer.Ordinal))
                {
                    voters[^1] = "n4";
                    voters.Sort(StringComparer.Ordinal);
                }

                node.EnterJoint(voters);
                Note(step + " joint " + string.Join(",", voters));
            }
        }

        private void Transfer(int step)
        {
            SimNode? leader = LiveLeader();
            if (leader is null)
            {
                Note(step + " transfer no-leader");
                return;
            }

            var peers = leader.Raft!.VoterPeerIds.OrderBy(static id => id, StringComparer.Ordinal).ToList();
            if (peers.Count == 0)
            {
                Note(step + " transfer none");
                return;
            }

            string target = peers[_rng.Next(peers.Count)];
            leader.Raft.TransferLeadership(target);
            Note(step + " transfer " + target);
        }

        private void Snap(int step)
        {
            int taken = 0;
            foreach (SimNode node in _nodes)
            {
                if (node.Live && node.Raft!.LastApplied > 0)
                {
                    node.Raft.Snapshot();
                    taken++;
                }
            }

            Note(step + " snap " + taken);
        }

        private void Drain()
        {
            for (int round = 0; round < 48; round++)
            {
                _net.DeliverDue();
                bool progress = false;
                foreach (SimNode node in _nodes)
                {
                    if (node.Frozen || node.Raft is null)
                    {
                        continue;
                    }

                    try
                    {
                        if (node.Live && node.Raft.Drain())
                        {
                            progress = true;
                        }
                    }
                    catch (SimCrash)
                    {
                        progress = true;
                    }

                    if (node.PendingCrash)
                    {
                        Reap(node);
                        progress = true;
                    }
                }

                if (progress)
                {
                    continue;
                }

                bool quorumCrash = false;
                foreach (SimNode node in _nodes)
                {
                    if (node.Frozen || !node.Live)
                    {
                        continue;
                    }

                    try
                    {
                        node.Raft!.CheckQuorumAfterDrain();
                    }
                    catch (SimCrash)
                    {
                        quorumCrash = true;
                    }

                    if (node.PendingCrash)
                    {
                        Reap(node);
                        quorumCrash = true;
                    }
                }

                if (quorumCrash)
                {
                    continue;
                }

                _net.DeliverDue();
                bool more = false;
                foreach (SimNode node in _nodes)
                {
                    if (node.Frozen || node.Raft is null)
                    {
                        continue;
                    }

                    try
                    {
                        if (node.Live && node.Raft.Drain())
                        {
                            more = true;
                        }
                    }
                    catch (SimCrash)
                    {
                        more = true;
                    }

                    if (node.PendingCrash)
                    {
                        Reap(node);
                        more = true;
                    }
                }

                if (!more)
                {
                    return;
                }
            }
        }

        private void Check(int step)
        {
            var line = new StringBuilder();
            line.Append(step);
            foreach (SimNode node in _nodes)
            {
                line.Append(' ');
                line.Append(node.Id);
                if (!node.Live)
                {
                    line.Append(" down");
                    continue;
                }

                RaftNode raft = node.Raft!;
                line.Append(' ');
                line.Append(raft.Role);
                line.Append(' ');
                line.Append(raft.CurrentTerm);
                line.Append(' ');
                line.Append(raft.CommitIndex);
                line.Append(' ');
                line.Append(node.Log.LastIndex);
                line.Append(' ');
                line.Append(node.Log.LastTerm);
                CheckNode(node);
            }

            Note(line.ToString());
            CheckLogs();
        }

        private void CheckNode(SimNode node)
        {
            RaftNode raft = node.Raft!;
            if (node.CommitGeneration == node.Generation && raft.CommitIndex < node.CommitSeen)
            {
                Fail("commit index moved backwards on " + node.Id);
            }

            node.CommitSeen = raft.CommitIndex;
            node.CommitGeneration = node.Generation;
            if (raft.Role != Role.Leader)
            {
                return;
            }

            if (_termLeader.TryGetValue(raft.CurrentTerm, out string? existing) && existing != node.Id)
            {
                Fail("election safety term " + raft.CurrentTerm + " leaders " + existing + " and " + node.Id);
            }

            _termLeader[raft.CurrentTerm] = node.Id;
            List<EntryMark> prefix = node.Log.Marks();
            if (_leaderPrefix.TryGetValue(node.Id, out (long Term, List<EntryMark> Prefix) prior) && prior.Term == raft.CurrentTerm)
            {
                if (prefix.Count < prior.Prefix.Count)
                {
                    Fail("leader append-only shortened " + node.Id);
                }

                for (int i = 0; i < prior.Prefix.Count; i++)
                {
                    if (prefix[i] != prior.Prefix[i])
                    {
                        Fail("leader append-only rewrote " + node.Id + " at " + prefix[i].Index);
                    }
                }
            }
            else
            {
                _leaderPrefix[node.Id] = (raft.CurrentTerm, prefix);
            }
        }

        private void CheckLogs()
        {
            var atIndex = new Dictionary<long, List<EntryMark>>();
            foreach (SimNode node in _nodes)
            {
                foreach (EntryMark stored in node.Log.Marks())
                {
                    EntryMark mark = stored with { Node = node.Id };
                    if (mark.Index <= node.Log.LastIncludedIndex || mark.Index > node.Log.LastIndex)
                    {
                        continue;
                    }

                    if (!atIndex.TryGetValue(mark.Index, out List<EntryMark>? list))
                    {
                        list = [];
                        atIndex[mark.Index] = list;
                    }

                    list.Add(mark);
                    if (mark.Index > node.Log.LastIncludedIndex && mark.Index <= node.LogAppliedCeiling())
                    {
                        Remember("state machine", _applied, mark);
                    }
                }

                if (!node.Live)
                {
                    continue;
                }

                long commit = node.Raft!.CommitIndex;
                foreach (EntryMark mark in node.Log.Marks())
                {
                    if (mark.Index <= node.Log.LastIncludedIndex)
                    {
                        continue;
                    }

                    if (mark.Index <= commit)
                    {
                        Remember("committed prefix", _committed, mark);
                    }
                }
            }

            foreach ((long index, List<EntryMark> marks) in atIndex)
            {
                var byTerm = new Dictionary<long, int>();
                foreach (EntryMark mark in marks)
                {
                    if (byTerm.TryGetValue(mark.Term, out int hash) && hash != mark.Hash)
                    {
                        var detail = new StringBuilder();
                        foreach (EntryMark item in marks)
                        {
                            if (item.Term != mark.Term)
                            {
                                continue;
                            }

                            detail.Append(' ');
                            detail.Append(item.Node);
                            detail.Append('#');
                            detail.Append(item.Hash.ToString(CultureInfo.InvariantCulture));
                        }

                        Fail("log matching command at " + index.ToString(CultureInfo.InvariantCulture) + " term " + mark.Term.ToString(CultureInfo.InvariantCulture) + detail);
                    }

                    byTerm[mark.Term] = mark.Hash;
                }
            }

            foreach ((long index, List<EntryMark> marks) in atIndex)
            {
                if (index <= 1 || !atIndex.TryGetValue(index - 1, out List<EntryMark>? previous))
                {
                    continue;
                }

                foreach (EntryMark mark in marks)
                {
                    foreach (EntryMark other in marks)
                    {
                        if (mark.Term != other.Term || ReferenceEquals(mark, other))
                        {
                            continue;
                        }
                    }

                    EntryMark? prev = previous.FirstOrDefault(item => item.Node == mark.Node);
                    if (prev is null)
                    {
                        continue;
                    }

                    foreach (EntryMark other in marks)
                    {
                        if (other.Term != mark.Term || other.Node == mark.Node)
                        {
                            continue;
                        }

                        EntryMark? otherPrev = previous.FirstOrDefault(item => item.Node == other.Node);
                        if (otherPrev is not null && otherPrev.Term != prev.Term)
                        {
                            Fail("log matching prefix at " + index);
                        }
                    }
                }
            }

            foreach (SimNode node in _nodes)
            {
                if (!node.Live || node.Raft!.Role != Role.Leader)
                {
                    continue;
                }

                foreach ((long index, (long Term, int Hash) entry) in _committed)
                {
                    if (!Covers(node, index, entry.Term, entry.Hash))
                    {
                        Fail("leader completeness " + node.Id + " term " + node.Raft.CurrentTerm + " missing " + index);
                    }
                }
            }
        }

        private static bool Covers(SimNode node, long index, long term, int hash)
        {
            // Marks inside a snapshot are the pre-compact entry and can disagree with the installed snapshot.
            if (index < node.Log.LastIncludedIndex)
            {
                return true;
            }

            if (index == node.Log.LastIncludedIndex)
            {
                return node.Log.LastIncludedTerm == term;
            }

            if (node.Log.TryMark(index, out EntryMark mark))
            {
                return mark.Term == term && mark.Hash == hash;
            }

            return false;
        }

        private void Remember(string name, Dictionary<long, (long Term, int Hash)> table, EntryMark mark)
        {
            if (table.TryGetValue(mark.Index, out (long Term, int Hash) prior)
                && (prior.Term != mark.Term || prior.Hash != mark.Hash))
            {
                Fail(name + " diverged at " + mark.Index.ToString(CultureInfo.InvariantCulture)
                    + " " + mark.Node + " term " + mark.Term.ToString(CultureInfo.InvariantCulture)
                    + " hash " + mark.Hash.ToString(CultureInfo.InvariantCulture)
                    + " prior term " + prior.Term.ToString(CultureInfo.InvariantCulture)
                    + " hash " + prior.Hash.ToString(CultureInfo.InvariantCulture));
            }

            table[mark.Index] = (mark.Term, mark.Hash);
        }

        private void Record(int client, string op, string phase, string key, string result)
        {
            var entry = new ClientHistoryEntry(_net.Now, client, op, phase, key, result);
            _history.Add(entry);
            Note("h " + entry.TimeMillis + " " + entry.Client + " " + entry.Op + " " + entry.Phase + " " + entry.Key + " " + entry.Result);
        }

        private void Note(string line)
        {
            _recent.Enqueue(line);
            if (_recent.Count > 8)
            {
                _recent.Dequeue();
            }

            _hash.AppendData(Encoding.UTF8.GetBytes(line + "\n"));
        }

        private SimNode? LiveLeader()
        {
            SimNode? found = null;
            foreach (SimNode node in _nodes)
            {
                if (node.Live && node.Raft!.Role == Role.Leader && (found is null || string.CompareOrdinal(node.Id, found.Id) < 0))
                {
                    found = node;
                }
            }

            return found;
        }

        private int LiveCount()
        {
            int count = 0;
            foreach (SimNode node in _nodes)
            {
                if (node.Live)
                {
                    count++;
                }
            }

            return count;
        }

        private string PickPeer(RaftNode node)
        {
            var peers = node.VoterPeerIds.OrderBy(static id => id, StringComparer.Ordinal).ToList();
            if (peers.Count == 0)
            {
                return "n4";
            }

            return peers[_rng.Next(peers.Count)];
        }

        private void Fail(string message) =>
            throw new InvalidOperationException(message + " | " + string.Join(" || ", _recent));
    }

    private sealed class SimClient(int id, KvClient kv)
    {
        public int Id { get; } = id;

        public KvClient Kv { get; } = kv;
    }

    private readonly record struct Pending(string Key, string Value, int Since);

    private sealed class SimNode(string id, string[] peers, TimeSpan election, int rate)
    {
        public string Id { get; } = id;

        public string[] Peers { get; } = peers;

        public TimeSpan Election { get; } = election;

        public SimClock Clock { get; } = new();

        public SimDurableLog Log { get; } = new();

        public SimDurableState State { get; } = new();

        public InMemoryKvStore Store { get; internal set; } = new();

        public RaftNode? Raft { get; set; }

        public bool Crashed { get; set; }

        public bool PendingCrash { get; set; }

        public bool Frozen { get; set; }

        public int Generation { get; set; }

        public long CommitSeen { get; set; }

        public int CommitGeneration { get; set; }

        public int RatePercent { get; set; } = rate;

        public bool Live => !Crashed && Raft is not null;

        public long LogAppliedCeiling() => Live ? Raft!.LastApplied : Log.LastIncludedIndex;
    }

    private sealed class SimClock : IRaftClock
    {
        private long _millis;
        private long _fraction;
        private Action? _onAdvance;

        public long Millis => _millis;

        public void OnAdvance(Action listener) => _onAdvance = listener;

        public void Clear() => _onAdvance = null;

        public void AdvanceScaled(long delta, int ratePercent)
        {
            if (delta <= 0)
            {
                return;
            }

            _fraction += delta * ratePercent;
            long whole = _fraction / 100;
            _fraction %= 100;
            if (whole > 0)
            {
                _millis += whole;
                _onAdvance?.Invoke();
            }
        }

        public void Jump(long delta)
        {
            long next = checked(_millis + delta);
            if (next < 0)
            {
                next = 0;
            }

            if (next == _millis)
            {
                return;
            }

            _millis = next;
            _fraction = 0;
            _onAdvance?.Invoke();
        }
    }

    private sealed class SimNet : ITransport
    {
        private readonly SimRng _rng;
        private readonly HashSet<string> _known = [];
        private readonly Dictionary<string, RaftNode?> _handlers = [];
        private readonly HashSet<Link> _cuts = [];
        private readonly HashSet<string> _frozen = [];
        private readonly List<Delivery> _queue = [];
        private long _seq;

        public SimNet(SimRng rng) => _rng = rng;

        public long Now { get; set; }

        public int DropPercent { get; set; }

        public int DupPercent { get; set; }

        public int MaxDelay { get; set; }

        public bool CrashSends { get; set; }

        public Action<string, string, object>? OnSent { get; set; }

        public Action<string>? OnSenderCrash { get; set; }

        public void Known(string id) => _known.Add(id);

        public void Bind(string id, RaftNode? node) => _handlers[id] = node;

        public void SetFrozen(string id, bool frozen)
        {
            if (frozen)
            {
                _frozen.Add(id);
            }
            else
            {
                _frozen.Remove(id);
            }
        }

        public void ClearFaults()
        {
            _cuts.Clear();
            DropPercent = 0;
            DupPercent = 0;
            MaxDelay = 0;
            CrashSends = false;
        }

        public void DropQueued() => _queue.Clear();

        public void TogglePartition(string a, string b)
        {
            var link = Link.Of(a, b);
            if (!_cuts.Add(link))
            {
                _cuts.Remove(link);
            }
        }

        public bool HasPeer(string nodeId) => _known.Contains(nodeId);

        public bool Send(string sender, string recipient, object payload)
        {
            OnSent?.Invoke(sender, recipient, payload);
            if (!_known.Contains(recipient))
            {
                MaybeCrash(sender);
                return false;
            }

            if (_cuts.Contains(Link.Of(sender, recipient)))
            {
                MaybeCrash(sender);
                return false;
            }

            if (DropPercent > 0 && _rng.Next(100) < DropPercent)
            {
                MaybeCrash(sender);
                return true;
            }

            Enqueue(sender, recipient, payload, MaxDelay > 0 ? _rng.Next(MaxDelay + 1) : 0);
            if (DupPercent > 0 && _rng.Next(100) < DupPercent)
            {
                int extra = 1 + (MaxDelay > 0 ? _rng.Next(MaxDelay + 1) : 0);
                Enqueue(sender, recipient, payload, extra);
            }

            MaybeCrash(sender);
            return true;
        }

        private void MaybeCrash(string sender)
        {
            if (!CrashSends || _rng.Next(40) != 0)
            {
                return;
            }

            OnSenderCrash?.Invoke(sender);
            throw new SimCrash();
        }

        public void DeliverDue()
        {
            _queue.Sort(static (left, right) =>
            {
                int byDue = left.Due.CompareTo(right.Due);
                return byDue != 0 ? byDue : left.Seq.CompareTo(right.Seq);
            });
            var keep = new List<Delivery>();
            var ready = new List<Delivery>();
            foreach (Delivery item in _queue)
            {
                if (item.Due <= Now
                    && !_frozen.Contains(item.Envelope.To)
                    && _handlers.TryGetValue(item.Envelope.To, out RaftNode? node)
                    && node is not null)
                {
                    ready.Add(item);
                }
                else
                {
                    keep.Add(item);
                }
            }

            _queue.Clear();
            _queue.AddRange(keep);
            foreach (Delivery item in ready)
            {
                _handlers[item.Envelope.To]!.Receive(item.Envelope);
            }
        }

        private void Enqueue(string sender, string recipient, object payload, int delay) =>
            _queue.Add(new Delivery(Now + delay, _seq++, new Envelope(sender, recipient, payload)));

        private readonly record struct Link(string A, string B)
        {
            public static Link Of(string a, string b) =>
                string.CompareOrdinal(a, b) <= 0 ? new Link(a, b) : new Link(b, a);
        }

        private readonly record struct Delivery(long Due, long Seq, Envelope Envelope);
    }

    private sealed class SimCrash : Exception
    {
    }

    private sealed class SimRng
    {
        private ulong _state;

        public SimRng(ulong seed) => _state = seed == 0 ? 0x9E3779B97F4A7C15UL : seed;

        public int Next(int bound)
        {
            _state += 0x9E3779B97F4A7C15UL;
            ulong z = _state;
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
            z ^= z >> 31;
            return (int)(z % (ulong)bound);
        }
    }
}

internal sealed record EntryMark(long Index, long Term, int Hash, string Node);

/// <summary>Log that keeps unflushed appends visible until a crash drops or tears them.</summary>
internal sealed class SimDurableLog : IRaftLog
{
    private readonly InMemoryRaftLog _inner = new();
    private readonly Dictionary<long, EntryMark> _marks = [];
    private long _flushedThrough;

    public long FlushedThrough => _flushedThrough;

    public void Append(LogEntry entry)
    {
        _inner.Append(entry);
        _marks[entry.Index] = new EntryMark(entry.Index, entry.Term, Hash(entry.Command), "");
    }

    public LogEntry Read(long index) => _inner.Read(index);

    public ReplicationBatch ReadForReplication(long nextIndex, int maxEntries, int maxBytes) =>
        _inner.ReadForReplication(nextIndex, maxEntries, maxBytes);

    public long LastIndex => _inner.LastIndex;

    public long LastTerm => _inner.LastTerm;

    public long DurableIndex => _inner.DurableIndex;

    public void TruncateFrom(long index)
    {
        _inner.TruncateFrom(index);
        var drop = new List<long>();
        foreach (long key in _marks.Keys)
        {
            if (key >= index)
            {
                drop.Add(key);
            }
        }

        foreach (long key in drop)
        {
            _marks.Remove(key);
        }

        if (_flushedThrough >= index)
        {
            _flushedThrough = _inner.LastIndex;
        }
    }

    public long LastIncludedIndex => _inner.LastIncludedIndex;

    public long LastIncludedTerm => _inner.LastIncludedTerm;

    public byte[] SnapshotBytes() => _inner.SnapshotBytes();

    public void CompactThrough(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot)
    {
        _inner.CompactThrough(lastIncludedIndex, lastIncludedTerm, snapshot);
        if (_flushedThrough < _inner.LastIncludedIndex)
        {
            _flushedThrough = _inner.LastIncludedIndex;
        }
    }

    public void Force()
    {
        _inner.Force();
        _flushedThrough = _inner.LastIndex;
    }

    public void LoseUnflushed(int mode)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(mode);
        long last = LastIndex;
        if (last <= _flushedThrough)
        {
            return;
        }

        long keep = mode == 0 ? _flushedThrough : last - 1;
        if (keep < last)
        {
            TruncateFrom(keep + 1);
        }

        _flushedThrough = LastIndex;
    }

    public List<EntryMark> Marks()
    {
        var list = new List<EntryMark>(_marks.Count);
        foreach ((long index, EntryMark mark) in _marks)
        {
            list.Add(mark with { Index = index, Node = "" });
        }

        list.Sort(static (left, right) => left.Index.CompareTo(right.Index));
        return list;
    }

    public bool TryMark(long index, out EntryMark mark)
    {
        if (_marks.TryGetValue(index, out EntryMark? found))
        {
            mark = found!;
            return true;
        }

        mark = new EntryMark(0, 0, 0, "");
        return false;
    }

    private static int Hash(byte[] command)
    {
        uint hash = 2166136261;
        foreach (byte value in command)
        {
            hash = (hash ^ value) * 16777619;
        }

        return (int)hash;
    }
}

/// <summary>Term and vote that flush before the mutating call returns, unless a test defers one flush.</summary>
internal sealed class SimDurableState : IPersistentState
{
    private long _term;
    private string? _vote;
    private long _lease;
    private byte[] _membership = [];
    private long _durableTerm;
    private string? _durableVote;
    private long _durableLease;
    private byte[] _durableMembership = [];
    private bool _defer;

    public long CurrentTerm => _term;

    public string? VotedFor => _vote;

    public long MaxAdvertisedLeaseMillis => _lease;

    public void DeferFlush() => _defer = true;

    public void SetCurrentTerm(long term)
    {
        if (term < 0)
        {
            throw new ArgumentException("term must be >= 0 (got " + term + ")");
        }

        if (term < _term)
        {
            throw new ArgumentException("term must not decrease: current=" + _term + " proposed=" + term);
        }

        if (term > _term)
        {
            _term = term;
            _vote = null;
            Publish();
        }
    }

    public void RecordVote(string candidateId)
    {
        if (string.IsNullOrWhiteSpace(candidateId))
        {
            throw new ArgumentException("candidateId must be non-blank");
        }

        if (_vote is not null && _vote != candidateId)
        {
            throw new InvalidOperationException("already voted for " + _vote + " in term " + _term);
        }

        _vote = candidateId;
        Publish();
    }

    public void NoteAdvertisedLease(long millis)
    {
        if (millis <= _lease)
        {
            return;
        }

        _lease = millis;
        Publish();
    }

    public void PersistMembership(byte[] blob)
    {
        ArgumentNullException.ThrowIfNull(blob);
        _membership = (byte[])blob.Clone();
        Publish();
    }

    public byte[] MembershipBlob() => (byte[])_membership.Clone();

    public void LoseUnflushed()
    {
        _term = _durableTerm;
        _vote = _durableVote;
        _lease = _durableLease;
        _membership = (byte[])_durableMembership.Clone();
    }

    private void Publish()
    {
        if (_defer)
        {
            _defer = false;
            return;
        }

        _durableTerm = _term;
        _durableVote = _vote;
        _durableLease = _lease;
        _durableMembership = (byte[])_membership.Clone();
    }
}
