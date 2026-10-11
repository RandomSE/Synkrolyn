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

    public required IReadOnlyList<ClientHistoryEntry> History { get; init; }
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

    public static SimResult Run(int seed, int steps)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(steps);

        var world = new World(seed, steps);
        world.Execute();
        return world.Result();
    }

    private sealed class World
    {
        private readonly SimRng _rng;
        private readonly int _steps;
        private readonly int _healAt;
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
        private bool _writeCommitted;
        private bool _healed;

        public World(int seed, int steps)
        {
            _rng = new SimRng((ulong)(uint)seed + 1UL);
            _steps = steps;
            _healAt = steps * 2 / 3;
            _net = new SimNet(_rng);
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
            Drain();
            for (int step = 0; step < _steps; step++)
            {
                if (step == _healAt)
                {
                    Heal();
                }

                if (step >= _healAt)
                {
                    Advance(30);
                    if (step % 3 == 0)
                    {
                        ClientPut(step);
                    }
                    else
                    {
                        Note(step + " advance");
                    }
                }
                else
                {
                    Act(step, faults: true);
                }

                Drain();
                Check(step);
            }
        }

        public SimResult Result()
        {
            bool leader = _nodes.Any(static node => node.Live && node.Raft!.Role == Role.Leader);
            return new SimResult
            {
                TraceHash = Convert.ToHexString(_hash.GetHashAndReset()),
                LeaderElected = leader,
                WriteCommitted = _writeCommitted,
                History = _history,
            };
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
                node.Clock.AdvanceScaled(delta, node.RatePercent);
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
            if (!node.Crashed && LiveCount() > 2)
            {
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
            string key = "k" + _rng.Next(3);
            string value = "v" + client.Id + "-" + step;
            if (!Bind(client))
            {
                Record(client.Id, "put", "invoke", key, value);
                Record(client.Id, "put", "complete", key, "no-leader");
                Note(step + " put no-leader");
                return;
            }

            EnsureSession(client);
            Record(client.Id, "put", "invoke", key, value);
            client.Kv.Put(key, value);
            Drain();
            long? done = client.Kv.AwaitCommitted();
            Record(client.Id, "put", "complete", key, done is null ? "null" : done.Value.ToString(CultureInfo.InvariantCulture));
            if (done is not null && _healed)
            {
                _writeCommitted = true;
            }

            Note(step + " put " + client.Id + " " + key + " " + (done is null ? "null" : done.Value.ToString(CultureInfo.InvariantCulture)));
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
                result = client.Kv.Get(key) ?? "null";
            }
            catch (KvSessionException)
            {
                result = "expired";
            }

            Record(client.Id, "get", "complete", key, result);
            Note(step + " get " + client.Id + " " + key);
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
                    if (node.Live && node.Raft!.Drain())
                    {
                        progress = true;
                    }
                }

                if (progress)
                {
                    continue;
                }

                foreach (SimNode node in _nodes)
                {
                    if (node.Live)
                    {
                        node.Raft!.CheckQuorumAfterDrain();
                    }
                }

                _net.DeliverDue();
                bool more = false;
                foreach (SimNode node in _nodes)
                {
                    if (node.Live && node.Raft!.Drain())
                    {
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
                    if (!atIndex.TryGetValue(mark.Index, out List<EntryMark>? list))
                    {
                        list = [];
                        atIndex[mark.Index] = list;
                    }

                    list.Add(mark);
                    if (mark.Index <= node.LogAppliedCeiling())
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
                        Fail("log matching command at " + index + " term " + mark.Term);
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
            if (node.Log.TryMark(index, out EntryMark mark))
            {
                return mark.Term == term && mark.Hash == hash;
            }

            if (node.Log.LastIncludedIndex == index)
            {
                return node.Log.LastIncludedTerm == term;
            }

            return node.Log.LastIncludedIndex > index;
        }

        private void Remember(string name, Dictionary<long, (long Term, int Hash)> table, EntryMark mark)
        {
            if (table.TryGetValue(mark.Index, out (long Term, int Hash) prior)
                && (prior.Term != mark.Term || prior.Hash != mark.Hash))
            {
                Fail(name + " diverged at " + mark.Index);
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

    private sealed class SimNode(string id, string[] peers, TimeSpan election, int rate)
    {
        public string Id { get; } = id;

        public string[] Peers { get; } = peers;

        public TimeSpan Election { get; } = election;

        public SimClock Clock { get; } = new();

        public TracingLog Log { get; } = new();

        public InMemoryPersistentState State { get; } = new();

        public InMemoryKvStore Store { get; } = new();

        public RaftNode? Raft { get; set; }

        public bool Crashed { get; set; }

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
    }

    private sealed class SimNet : ITransport
    {
        private readonly SimRng _rng;
        private readonly HashSet<string> _known = [];
        private readonly Dictionary<string, RaftNode?> _handlers = [];
        private readonly HashSet<Link> _cuts = [];
        private readonly List<Delivery> _queue = [];
        private long _seq;

        public SimNet(SimRng rng) => _rng = rng;

        public long Now { get; set; }

        public int DropPercent { get; set; }

        public int DupPercent { get; set; }

        public int MaxDelay { get; set; }

        public void Known(string id) => _known.Add(id);

        public void Bind(string id, RaftNode? node) => _handlers[id] = node;

        public void ClearFaults()
        {
            _cuts.Clear();
            DropPercent = 0;
            DupPercent = 0;
            MaxDelay = 0;
        }

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
            if (!_known.Contains(recipient))
            {
                return false;
            }

            if (_cuts.Contains(Link.Of(sender, recipient)))
            {
                return false;
            }

            if (DropPercent > 0 && _rng.Next(100) < DropPercent)
            {
                return true;
            }

            Enqueue(sender, recipient, payload, MaxDelay > 0 ? _rng.Next(MaxDelay + 1) : 0);
            if (DupPercent > 0 && _rng.Next(100) < DupPercent)
            {
                int extra = 1 + (MaxDelay > 0 ? _rng.Next(MaxDelay + 1) : 0);
                Enqueue(sender, recipient, payload, extra);
            }

            return true;
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
                if (item.Due <= Now && _handlers.TryGetValue(item.Envelope.To, out RaftNode? node) && node is not null)
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

    private sealed class TracingLog : IRaftLog
    {
        private readonly InMemoryRaftLog _inner = new();
        private readonly Dictionary<long, EntryMark> _marks = [];

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
        }

        public long LastIncludedIndex => _inner.LastIncludedIndex;

        public long LastIncludedTerm => _inner.LastIncludedTerm;

        public byte[] SnapshotBytes() => _inner.SnapshotBytes();

        public void CompactThrough(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot) =>
            _inner.CompactThrough(lastIncludedIndex, lastIncludedTerm, snapshot);

        public void Force() => _inner.Force();

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

    private sealed record EntryMark(long Index, long Term, int Hash, string Node);

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
