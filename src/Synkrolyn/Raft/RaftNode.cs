using System.Collections.Concurrent;
using Synkrolyn.Net;

namespace Synkrolyn.Raft;

/// <summary>
/// Single-threaded Raft node. I/O only enqueues. One drain loop applies election,
/// replication, snapshots, and membership. The leader counts itself toward commit
/// only through <see cref="IRaftLog.DurableIndex"/>, and still sends AppendEntries
/// before its own <see cref="IRaftLog.Force"/>.
/// </summary>
public sealed class RaftNode
{
    /// <summary>Cap on AppendEntries payload size.</summary>
    public const int MaxAppendEntriesBatch = 16;

    /// <summary>Auto-snapshot is off unless <see cref="SnapshotThreshold"/> is set.</summary>
    public const int DefaultSnapshotThreshold = 0;

    /// <summary>PreVote heard-recently window, in heartbeat intervals.</summary>
    public const int PreVoteLeaderLeaseFactor = 2;

    private readonly string _nodeId;
    private readonly HashSet<string> _voterPeers;
    private readonly HashSet<string> _learnerPeers;
    private readonly IRaftClock _clock;
    private readonly ITransport _transport;
    private readonly IPersistentState _persistentState;
    private readonly IRaftLog _log;
    private readonly IStateMachine _stateMachine;
    private readonly long _electionTimeoutMillis;
    private readonly long _heartbeatIntervalMillis;
    private readonly object _eventLock = new();
    private readonly ConcurrentQueue<Action> _mailbox = new();
    private readonly Dictionary<string, long> _nextIndex = [];
    private readonly Dictionary<string, long> _matchIndex = [];
    private readonly Dictionary<string, long> _lastAppendAckMillis = [];
    private readonly Dictionary<string, long> _leaseAckMillis = [];
    private readonly Dictionary<string, long> _nextAeStamp = [];
    private readonly Dictionary<string, Dictionary<long, long>> _aeSendMillis = [];
    private readonly Dictionary<string, long> _linearizableAckSendMillis = [];
    private readonly Dictionary<string, long> _readStampFloor = [];
    private readonly Dictionary<string, long> _peerInstalledIndex = [];
    private readonly Dictionary<string, int> _snapshotNextOffset = [];
    private readonly Dictionary<string, long> _snapshotGenIndex = [];
    private readonly Dictionary<string, long> _snapshotGenTerm = [];
    private readonly HashSet<string> _snapshotInFlight = [];
    private readonly List<Action> _afterForce = [];
    private readonly HashSet<string> _votesGranted = [];
    private readonly HashSet<string> _preVotesGranted = [];
    private readonly List<byte> _incomingSnapshot = [];
    private readonly HashSet<string> _snapshotForceFull = [];
    private readonly HashSet<string> _coldVoters = [];
    private readonly HashSet<string> _cnewVoters = [];
    private readonly HashSet<string> _bootstrapVoters = [];
    private readonly HashSet<string> _bootstrapLearners = [];
    private readonly List<ApplyWaiter> _applyWaiters = [];
    private readonly HashSet<string> _readIndexAcks = [];

    private bool _draining;
    private bool _started;
    private bool _preCandidate;
    private bool _logNeedsForce;
    private bool _applyInFlight;
    private bool _selfVoter = true;
    private bool _selfLearner;
    private bool _holdJoint;
    private string? _pendingTransferTo;
    private long _pendingTransferDeadline;
    private LeadershipTransferStatus _transferStatus = LeadershipTransferStatus.None;
    private Action<Action> _applyExecutor = static action => action();
    private Action<Action> _snapshotExecutor = static action => action();
    private byte[]? _lastDeltaFramed;
    private long _deltaBaseIndex;
    private long _lastLeaderContactMillis = long.MinValue;
    private long _lastAcceptedLeaderCommit;
    private bool _lastPreVoteGranted;
    private Func<long, long> _electionJitter = static timeout => timeout;
    private int _snapshotChunkSize = int.MaxValue;
    private int _snapshotThreshold = DefaultSnapshotThreshold;
    private long _followerLeasePauseSlackMillis;
    private int _snapshotChunksReceived;
    private int _snapshotsInstalled;
    private long _incomingSnapshotExpectedOffset;
    private long _incomingLastIncludedIndex;
    private long _incomingLastIncludedTerm;
    private Role _role = Role.Follower;
    private string? _leaderId;
    private long _electionDeadline;
    private long _nextHeartbeatAt;
    private long _commitIndex;
    private long _lastApplied;
    private long _pendingReadIndex;
    private long _pendingReadTerm;

    /// <summary>Creates a node with a no-op state machine.</summary>
    public RaftNode(
        string nodeId,
        IEnumerable<string> peerIds,
        IRaftClock clock,
        ITransport transport,
        IPersistentState persistentState,
        IRaftLog log,
        TimeSpan electionTimeout,
        TimeSpan heartbeatInterval)
        : this(nodeId, peerIds, clock, transport, persistentState, log, electionTimeout, heartbeatInterval, IStateMachine.NoOp())
    {
    }

    /// <summary>Creates a node.</summary>
    public RaftNode(
        string nodeId,
        IEnumerable<string> peerIds,
        IRaftClock clock,
        ITransport transport,
        IPersistentState persistentState,
        IRaftLog log,
        TimeSpan electionTimeout,
        TimeSpan heartbeatInterval,
        IStateMachine stateMachine)
    {
        _nodeId = RequireNodeId(nodeId);
        ArgumentNullException.ThrowIfNull(peerIds);
        _voterPeers = CopyPeers(_nodeId, peerIds);
        _learnerPeers = [];
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _persistentState = persistentState ?? throw new ArgumentNullException(nameof(persistentState));
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _stateMachine = stateMachine ?? throw new ArgumentNullException(nameof(stateMachine));
        _electionTimeoutMillis = RequirePositive(electionTimeout, nameof(electionTimeout));
        _heartbeatIntervalMillis = RequirePositive(heartbeatInterval, nameof(heartbeatInterval));
        foreach (string peer in _voterPeers)
        {
            _nextIndex[peer] = 1;
            _matchIndex[peer] = 0;
        }

        _bootstrapVoters.UnionWith(AllVoters());
        _bootstrapLearners.UnionWith(AllLearners());
    }

    /// <summary>This node's id.</summary>
    public string NodeId => _nodeId;

    /// <summary>Current role.</summary>
    public Role Role
    {
        get { lock (_eventLock) { return _role; } }
    }

    /// <summary>Current term.</summary>
    public long CurrentTerm
    {
        get { lock (_eventLock) { return _persistentState.CurrentTerm; } }
    }

    /// <summary>Known leader id, or null.</summary>
    public string? LeaderId
    {
        get { lock (_eventLock) { return _leaderId; } }
    }

    /// <summary>Commit index.</summary>
    public long CommitIndex
    {
        get { lock (_eventLock) { return _commitIndex; } }
    }

    /// <summary>Match index for <paramref name="peer"/>, or 0.</summary>
    public long MatchIndex(string peer)
    {
        lock (_eventLock)
        {
            return _matchIndex.GetValueOrDefault(RequireNodeId(peer));
        }
    }

    /// <summary>Last applied index.</summary>
    public long LastApplied
    {
        get { lock (_eventLock) { return _lastApplied; } }
    }

    /// <summary>Snapshot chunks accepted by this follower.</summary>
    public int SnapshotChunksReceived
    {
        get { lock (_eventLock) { return _snapshotChunksReceived; } }
    }

    /// <summary>Snapshots fully installed by this follower.</summary>
    public int SnapshotsInstalled
    {
        get { lock (_eventLock) { return _snapshotsInstalled; } }
    }

    /// <summary>Outbound InstallSnapshot chunk size. The default sends one chunk.</summary>
    public int SnapshotChunkSize
    {
        get { lock (_eventLock) { return _snapshotChunkSize; } }
        set
        {
            lock (_eventLock)
            {
                if (value < 1)
                {
                    throw new ArgumentException("snapshotChunkSize must be >= 1");
                }

                _snapshotChunkSize = value;
            }
        }
    }

    /// <summary>Auto-snapshot after this many applied entries past the snapshot. 0 disables it.</summary>
    public int SnapshotThreshold
    {
        get { lock (_eventLock) { return _snapshotThreshold; } }
        set
        {
            lock (_eventLock)
            {
                if (value < 0)
                {
                    throw new ArgumentException("snapshotThreshold must be >= 0");
                }

                _snapshotThreshold = value;
            }
        }
    }

    /// <summary>Extra monotonic slack for follower lease expiry. Default 0.</summary>
    public void SetFollowerLeasePauseSlackMillis(long millis)
    {
        lock (_eventLock)
        {
            if (millis < 0)
            {
                throw new ArgumentException("followerLeasePauseSlackMillis must be >= 0");
            }

            _followerLeasePauseSlackMillis = millis;
        }
    }

    /// <summary>Maps election timeout T to a deadline span. Values outside [T, 2T] are clamped.</summary>
    public void SetElectionJitter(Func<long, long> jitter)
    {
        lock (_eventLock)
        {
            _electionJitter = jitter ?? throw new ArgumentNullException(nameof(jitter));
            if (_started)
            {
                ResetElectionDeadline();
            }
        }
    }

    /// <summary>Runs state-machine apply. The default is inline so a drain observes <see cref="LastApplied"/>.</summary>
    public void SetApplyExecutor(Action<Action> executor)
    {
        lock (_eventLock)
        {
            _applyExecutor = executor ?? throw new ArgumentNullException(nameof(executor));
        }
    }

    /// <summary>Runs log compaction. The default is inline.</summary>
    public void SetSnapshotExecutor(Action<Action> executor)
    {
        lock (_eventLock)
        {
            _snapshotExecutor = executor ?? throw new ArgumentNullException(nameof(executor));
        }
    }

    /// <summary>True when this node is a voter.</summary>
    public bool IsVoter
    {
        get { lock (_eventLock) { return _selfVoter; } }
    }

    /// <summary>Voter peers, excluding self.</summary>
    public IReadOnlySet<string> VoterPeerIds
    {
        get { lock (_eventLock) { return _voterPeers.ToHashSet(); } }
    }

    /// <summary>Learner peers, excluding self.</summary>
    public IReadOnlySet<string> LearnerPeerIds
    {
        get { lock (_eventLock) { return _learnerPeers.ToHashSet(); } }
    }

    /// <summary>True when joint Cold and Cnew are both set.</summary>
    public bool InJointConsensus
    {
        get { lock (_eventLock) { return InJointLocked(); } }
    }

    /// <summary>Cold voter ids while joint.</summary>
    public IReadOnlySet<string> ColdVoterIds
    {
        get { lock (_eventLock) { return _coldVoters.ToHashSet(); } }
    }

    /// <summary>Cnew voter ids while joint.</summary>
    public IReadOnlySet<string> CnewVoterIds
    {
        get { lock (_eventLock) { return _cnewVoters.ToHashSet(); } }
    }

    /// <summary>Leader quorum lease. CheckQuorum grace does not set this.</summary>
    public bool QuorumLeaseValid
    {
        get { lock (_eventLock) { return QuorumLeaseValidLocked(); } }
    }

    /// <summary>Follower lease for bounded-stale reads.</summary>
    public bool FollowerReadLeaseValid
    {
        get { lock (_eventLock) { return FollowerReadLeaseValidLocked(); } }
    }

    /// <summary>Leadership transfer status. Success means the target was observed as leader.</summary>
    public LeadershipTransferStatus TransferStatus
    {
        get { lock (_eventLock) { return _transferStatus; } }
    }

    /// <summary>True when the last PreVote request was granted by this node.</summary>
    public bool LastPreVoteGranted
    {
        get { lock (_eventLock) { return _lastPreVoteGranted; } }
    }

    /// <summary>Registers the clock listener and arms the election deadline.</summary>
    public void Start()
    {
        lock (_eventLock)
        {
            if (_started)
            {
                return;
            }

            _started = true;
            if (_log.LastIncludedIndex > 0)
            {
                MembershipSnapshot.Decoded decoded = MembershipSnapshot.Decode(_log.SnapshotBytes());
                if (decoded.HasConfig)
                {
                    InstallDecodedConfig(decoded);
                }

                _stateMachine.Restore(decoded.StateMachineBytes);
                _lastApplied = _log.LastIncludedIndex;
                _commitIndex = _log.LastIncludedIndex;
            }
            else
            {
                byte[] blob = _persistentState.MembershipBlob();
                if (blob.Length > 0)
                {
                    MembershipSnapshot.Decoded decoded = MembershipSnapshot.Decode(blob);
                    if (decoded.HasConfig)
                    {
                        InstallDecodedConfig(decoded);
                    }
                }
            }

            ReplayLogConfigs();
            ResetElectionDeadline();
        }

        _clock.OnAdvance(EnqueueTick);
    }

    /// <summary>Enqueues a clock tick.</summary>
    public void EnqueueTick() => _mailbox.Enqueue(OnTick);

    /// <summary>Enqueues an inbound envelope. The caller must <see cref="Drain"/>.</summary>
    public void Receive(Envelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        _mailbox.Enqueue(() => Dispatch(envelope));
    }

    /// <summary>Enqueues a candidate transition without draining.</summary>
    public void StartElection() => _mailbox.Enqueue(BeginElection);

    /// <summary>Next election or heartbeat deadline, in clock milliseconds.</summary>
    public long NextDeadlineMillis()
    {
        lock (_eventLock)
        {
            return _role == Role.Leader ? _nextHeartbeatAt : _electionDeadline;
        }
    }

    /// <summary>Drains the mailbox. One drainer at a time. Returns true if any task ran.</summary>
    public bool Drain()
    {
        lock (_eventLock)
        {
            if (_draining)
            {
                return false;
            }

            _draining = true;
            bool worked = false;
            try
            {
                while (true)
                {
                    bool ran = false;
                    while (_mailbox.TryDequeue(out Action? task))
                    {
                        task();
                        ran = true;
                        worked = true;
                    }

                    if (_logNeedsForce || _afterForce.Count > 0)
                    {
                        if (_logNeedsForce)
                        {
                            _log.Force();
                            _logNeedsForce = false;
                        }

                        Action[] pending = _afterForce.ToArray();
                        _afterForce.Clear();
                        foreach (Action action in pending)
                        {
                            action();
                        }

                        MaybeAdvanceCommit();
                        ApplyCommitted();
                        continue;
                    }

                    if (!ran)
                    {
                        break;
                    }
                }
            }
            finally
            {
                _draining = false;
            }

            return worked;
        }
    }

    /// <summary>Leader CheckQuorum after a full cluster drain.</summary>
    public void CheckQuorumAfterDrain()
    {
        lock (_eventLock)
        {
            if (_role == Role.Leader)
            {
                MaybeCheckQuorum(_clock.Millis);
            }
        }
    }

    /// <summary>Leader-only append. Empty when this node is not the leader. Does not wait for fsync.</summary>
    public long? Propose(byte[] command)
    {
        lock (_eventLock)
        {
            if (_role != Role.Leader)
            {
                return null;
            }

            return ProposeLocked(command);
        }
    }

    /// <summary>True when this node is leader and <see cref="LastApplied"/> has reached <paramref name="index"/>.</summary>
    public bool AwaitCommitted(long index)
    {
        lock (_eventLock)
        {
            return _role == Role.Leader && _lastApplied >= index;
        }
    }

    /// <summary>Registers a waiter completed by apply or failed by step-down. Does not block.</summary>
    public ApplyWaiter RegisterApplyWaiter(long index)
    {
        lock (_eventLock)
        {
            var waiter = new ApplyWaiter(index);
            if (_role != Role.Leader)
            {
                waiter.Failed = true;
                return waiter;
            }

            if (_lastApplied >= index)
            {
                waiter.Applied = true;
                return waiter;
            }

            _applyWaiters.Add(waiter);
            return waiter;
        }
    }

    /// <summary>Leader ReadIndex. Empty when not leader or no current-term commit.</summary>
    public long? BeginReadIndex()
    {
        lock (_eventLock)
        {
            if (_role != Role.Leader || !HasCommittedEntryInCurrentTerm())
            {
                return null;
            }

            _pendingReadIndex = _commitIndex;
            _pendingReadTerm = CurrentTermLocked();
            _readIndexAcks.Clear();
            _readStampFloor.Clear();
            foreach (string peer in _voterPeers)
            {
                _readStampFloor[peer] = _nextAeStamp.GetValueOrDefault(peer) + 1;
            }

            ReplicateToAll();
            return _pendingReadIndex;
        }
    }

    /// <summary>True when a pending ReadIndex or the send-time lease is satisfied and applied.</summary>
    public bool ReadIndexSatisfied()
    {
        lock (_eventLock)
        {
            return ReadIndexSatisfiedLocked();
        }
    }

    /// <summary>
    /// Arms leadership transfer. Returns false when this node is not leader or the target is not a voter.
    /// Success is <see cref="LeadershipTransferStatus.Succeeded"/>, not this return value.
    /// </summary>
    public bool TransferLeadership(string target)
    {
        lock (_eventLock)
        {
            if (_role != Role.Leader)
            {
                return false;
            }

            string peer = RequireNodeId(target);
            if (!_voterPeers.Contains(peer))
            {
                return false;
            }

            if (_matchIndex.GetValueOrDefault(peer) >= _log.LastIndex)
            {
                CompleteTransfer(peer);
                return true;
            }

            _pendingTransferTo = peer;
            _transferStatus = LeadershipTransferStatus.CatchingUp;
            _pendingTransferDeadline = _clock.Millis + _electionTimeoutMillis;
            ReplicateToAll();
            return true;
        }
    }

    /// <summary>Propose adding a non-voting learner.</summary>
    public long? AddLearner(string id)
    {
        lock (_eventLock)
        {
            if (_role != Role.Leader)
            {
                return null;
            }

            string peer = RequireNodeId(id);
            if (peer == _nodeId || _voterPeers.Contains(peer) || _learnerPeers.Contains(peer))
            {
                return null;
            }

            return ProposeLocked(MembershipCodec.EncodeAddLearner(peer));
        }
    }

    /// <summary>Promote a caught-up learner. Empty when match is behind commit.</summary>
    public long? PromoteVoter(string id)
    {
        lock (_eventLock)
        {
            if (_role != Role.Leader)
            {
                return null;
            }

            string peer = RequireNodeId(id);
            if (!_learnerPeers.Contains(peer) || _matchIndex.GetValueOrDefault(peer) < _commitIndex)
            {
                return null;
            }

            return ProposeLocked(MembershipCodec.EncodePromoteVoter(peer));
        }
    }

    /// <summary>
    /// Remove one voter or learner. Refuses a voter removal that would not leave a majority
    /// of the current voter set. A 3-voter cluster may become 2. A 2-voter cluster may not become 1.
    /// </summary>
    public long? RemoveServer(string id)
    {
        lock (_eventLock)
        {
            if (_role != Role.Leader)
            {
                return null;
            }

            string peer = RequireNodeId(id);
            if (peer == _nodeId || (!_voterPeers.Contains(peer) && !_learnerPeers.Contains(peer)))
            {
                return null;
            }

            if (_voterPeers.Contains(peer) && !RemoveLeavesMajority())
            {
                return null;
            }

            return ProposeLocked(MembershipCodec.EncodeRemoveServer(peer));
        }
    }

    /// <summary>Append a joint config. Live on append, not only on commit.</summary>
    public long? EnterJoint(IEnumerable<string> newVoters)
    {
        lock (_eventLock)
        {
            if (_role != Role.Leader || InJointLocked())
            {
                return null;
            }

            ArgumentNullException.ThrowIfNull(newVoters);
            var cnew = new HashSet<string>();
            foreach (string id in newVoters)
            {
                cnew.Add(RequireNodeId(id));
            }

            if (cnew.Count == 0 || !cnew.Contains(_nodeId))
            {
                return null;
            }

            HashSet<string> cold = AllVoters();
            if (cold.SetEquals(cnew))
            {
                return null;
            }

            return ProposeLocked(MembershipCodec.EncodeJoint(cold, cnew));
        }
    }

    /// <summary>Append Cnew alone.</summary>
    public long? LeaveJoint()
    {
        lock (_eventLock)
        {
            if (_role != Role.Leader || !InJointLocked())
            {
                return null;
            }

            return ProposeLocked(MembershipCodec.EncodeCnew(_cnewVoters.ToHashSet()));
        }
    }

    /// <summary>When true, the leader does not auto-append Cnew after the joint entry commits.</summary>
    public void SetHoldJoint(bool hold)
    {
        lock (_eventLock)
        {
            _holdJoint = hold;
            if (!hold)
            {
                MaybeAutoCnew();
            }
        }
    }

    /// <summary>Compacts through <see cref="LastApplied"/>.</summary>
    public void Snapshot()
    {
        lock (_eventLock)
        {
            SnapshotThroughLocked(_lastApplied);
        }
    }

    /// <summary>Compacts through <paramref name="index"/> when it is applied.</summary>
    public void SnapshotThrough(long index)
    {
        lock (_eventLock)
        {
            SnapshotThroughLocked(index);
        }
    }

    /// <summary>Test hook: raise commit index without an AppendEntries success.</summary>
    internal void CommitIndexForTest(long index)
    {
        lock (_eventLock)
        {
            if (index < _commitIndex)
            {
                throw new ArgumentException("commitIndex");
            }

            _commitIndex = index;
        }
    }

    /// <summary>Test helper: leave leadership without bumping the term.</summary>
    internal void StepDownForTest()
    {
        lock (_eventLock)
        {
            StepDown(CurrentTermLocked());
        }
    }

    /// <summary>Client apply waiter. Signaled from apply or step-down.</summary>
    public sealed class ApplyWaiter
    {
        internal ApplyWaiter(long index) => Index = index;

        /// <summary>Log index this waiter tracks.</summary>
        public long Index { get; }

        /// <summary>True after the index is applied.</summary>
        public bool Applied { get; internal set; }

        /// <summary>True after step-down before apply.</summary>
        public bool Failed { get; internal set; }
    }

    private void OnTick()
    {
        long now = _clock.Millis;
        if (_role == Role.Leader)
        {
            MaybeFinishTransfer();
            if (_role != Role.Leader)
            {
                return;
            }

            if (now >= _nextHeartbeatAt)
            {
                ReplicateToAll();
                _nextHeartbeatAt = now + _heartbeatIntervalMillis;
            }

            return;
        }

        if (_transferStatus == LeadershipTransferStatus.AwaitingWinner)
        {
            if (now >= _pendingTransferDeadline)
            {
                _transferStatus = LeadershipTransferStatus.Aborted;
                _pendingTransferTo = null;
            }
            else
            {
                ResetElectionDeadline();
                return;
            }
        }

        if (now < _electionDeadline)
        {
            return;
        }

        if (!_selfVoter)
        {
            ResetElectionDeadline();
            return;
        }

        if (_voterPeers.Count == 0)
        {
            BeginElection();
        }
        else
        {
            BeginPreVote();
        }
    }

    private void BeginPreVote()
    {
        _preCandidate = true;
        _preVotesGranted.Clear();
        _preVotesGranted.Add(_nodeId);
        ResetElectionDeadline();
        var request = new RequestVote(CurrentTermLocked() + 1, _nodeId, _log.LastIndex, _log.LastTerm, true);
        foreach (string peer in _voterPeers)
        {
            _transport.Send(_nodeId, peer, request);
        }

        if (VoteQuorum(_preVotesGranted))
        {
            BeginElection();
        }
    }

    private void BeginElection()
    {
        if (!_selfVoter)
        {
            return;
        }

        CampaignAt(CurrentTermLocked() + 1);
    }

    private void CampaignAt(long term)
    {
        _preCandidate = false;
        _preVotesGranted.Clear();
        _persistentState.SetCurrentTerm(term);
        _role = Role.Candidate;
        _leaderId = null;
        _votesGranted.Clear();
        _votesGranted.Add(_nodeId);
        _persistentState.RecordVote(_nodeId);
        ResetElectionDeadline();
        var request = new RequestVote(CurrentTermLocked(), _nodeId, _log.LastIndex, _log.LastTerm);
        foreach (string peer in _voterPeers)
        {
            _transport.Send(_nodeId, peer, request);
        }

        if (VoteQuorum(_votesGranted))
        {
            BecomeLeader();
        }
    }

    private void BecomeLeader()
    {
        _preCandidate = false;
        _preVotesGranted.Clear();
        _votesGranted.Clear();
        _role = Role.Leader;
        _leaderId = _nodeId;
        _snapshotInFlight.Clear();
        _snapshotNextOffset.Clear();
        _snapshotGenIndex.Clear();
        _snapshotGenTerm.Clear();
        long now = _clock.Millis;
        foreach (string peer in ReplicationTargets())
        {
            _nextIndex[peer] = _log.LastIndex + 1;
            _matchIndex[peer] = 0;
            _lastAppendAckMillis[peer] = now;
        }

        AppendLocked(new LogEntry(_log.LastIndex + 1, CurrentTermLocked(), []));
        ReplicateToAll();
        _nextHeartbeatAt = now + _heartbeatIntervalMillis;
        MaybeAdvanceCommit();
    }

    private void CompleteTransfer(string peer)
    {
        _transport.Send(_nodeId, peer, new TimeoutNow(CurrentTermLocked(), _nodeId));
        StepDown(CurrentTermLocked());
        _pendingTransferTo = peer;
        _transferStatus = LeadershipTransferStatus.AwaitingWinner;
        _pendingTransferDeadline = _clock.Millis + _electionTimeoutMillis;
    }

    private void MaybeFinishTransfer()
    {
        if (_role != Role.Leader || _transferStatus != LeadershipTransferStatus.CatchingUp || _pendingTransferTo is null)
        {
            return;
        }

        if (_clock.Millis >= _pendingTransferDeadline)
        {
            _transferStatus = LeadershipTransferStatus.Aborted;
            _pendingTransferTo = null;
            return;
        }

        if (_matchIndex.GetValueOrDefault(_pendingTransferTo) >= _log.LastIndex)
        {
            CompleteTransfer(_pendingTransferTo);
        }
    }

    private long? ProposeLocked(byte[] command)
    {
        ArgumentNullException.ThrowIfNull(command);
        long index = _log.LastIndex + 1;
        AppendLocked(new LogEntry(index, CurrentTermLocked(), command));
        ReplicateToAll();
        MaybeAdvanceCommit();
        return index;
    }

    private void ReplicateToAll()
    {
        foreach (string peer in ReplicationTargets())
        {
            SendTo(peer);
        }
    }

    private void SendTo(string peer)
    {
        if (_role != Role.Leader || _snapshotInFlight.Contains(peer))
        {
            return;
        }

        long next = _nextIndex.GetValueOrDefault(peer, 1);
        if (_log.LastIncludedIndex > 0 && next < _log.FirstIndex)
        {
            SendSnapshot(peer);
            return;
        }

        long prevIndex = next - 1;
        long prevTerm = prevIndex == 0
            ? 0
            : prevIndex == _log.LastIncludedIndex
                ? _log.LastIncludedTerm
                : _log.Read(prevIndex).Term;
        var entries = new List<LogEntry>();
        long last = Math.Min(_log.LastIndex, next + MaxAppendEntriesBatch - 1);
        for (long i = next; i <= last; i++)
        {
            entries.Add(_log.Read(i));
        }

        long stamp = _nextAeStamp.TryGetValue(peer, out long previous) ? previous + 1 : 1;
        _nextAeStamp[peer] = stamp;
        if (!_aeSendMillis.TryGetValue(peer, out Dictionary<long, long>? sent))
        {
            sent = [];
            _aeSendMillis[peer] = sent;
        }

        sent[stamp] = _clock.Millis;
        _transport.Send(
            _nodeId,
            peer,
            new AppendEntries(CurrentTermLocked(), _nodeId, prevIndex, prevTerm, entries, _commitIndex, stamp));
    }

    private void SendSnapshot(string peer)
    {
        _snapshotInFlight.Add(peer);
        byte[] all = _log.SnapshotBytes();
        if (!_snapshotForceFull.Contains(peer)
            && _lastDeltaFramed is not null
            && _peerInstalledIndex.TryGetValue(peer, out long installed)
            && installed == _deltaBaseIndex
            && _lastDeltaFramed.Length > 0
            && _lastDeltaFramed.Length < all.Length)
        {
            all = _lastDeltaFramed;
        }

        long lastIncluded = _log.LastIncludedIndex;
        long lastTerm = _log.LastIncludedTerm;
        _snapshotGenIndex[peer] = lastIncluded;
        _snapshotGenTerm[peer] = lastTerm;
        if (all.Length == 0)
        {
            _transport.Send(_nodeId, peer, new InstallSnapshot(CurrentTermLocked(), _nodeId, lastIncluded, lastTerm, 0, [], true));
            return;
        }

        int offset = _snapshotNextOffset.GetValueOrDefault(peer);
        if (offset < 0 || offset >= all.Length)
        {
            offset = 0;
            _snapshotNextOffset[peer] = 0;
        }

        int n = Math.Min(_snapshotChunkSize, all.Length - offset);
        byte[] chunk = all.AsSpan(offset, n).ToArray();
        bool done = offset + n >= all.Length;
        _transport.Send(
            _nodeId,
            peer,
            new InstallSnapshot(CurrentTermLocked(), _nodeId, lastIncluded, lastTerm, offset, chunk, done));
    }

    private void Dispatch(Envelope envelope)
    {
        switch (envelope.Payload)
        {
            case RequestVote request:
                OnRequestVote(envelope.From, request);
                break;
            case RequestVoteResponse response:
                OnRequestVoteResponse(envelope.From, response);
                break;
            case AppendEntries append:
                OnAppendEntries(envelope.From, append);
                break;
            case AppendEntriesResponse appendResponse:
                OnAppendEntriesResponse(envelope.From, appendResponse);
                break;
            case InstallSnapshot install:
                OnInstallSnapshot(envelope.From, install);
                break;
            case InstallSnapshotResponse installResponse:
                OnInstallSnapshotResponse(envelope.From, installResponse);
                break;
            case TimeoutNow timeout:
                OnTimeoutNow(timeout);
                break;
            default:
                throw new ArgumentException("unknown RPC payload: " + envelope.Payload.GetType());
        }
    }

    private void OnRequestVote(string from, RequestVote request)
    {
        if (request.PreVote)
        {
            OnPreVote(from, request);
            return;
        }

        if (request.Term < CurrentTermLocked())
        {
            _transport.Send(_nodeId, from, new RequestVoteResponse(CurrentTermLocked(), false));
            return;
        }

        if (request.Term > CurrentTermLocked())
        {
            StepDown(request.Term);
        }

        bool logOk = CandidateLogUpToDate(request);
        string? votedFor = _persistentState.VotedFor;
        bool canVote = votedFor is null || votedFor == request.CandidateId;
        if (logOk && canVote)
        {
            _persistentState.RecordVote(request.CandidateId);
            ResetElectionDeadline();
            _transport.Send(_nodeId, from, new RequestVoteResponse(CurrentTermLocked(), true));
        }
        else
        {
            _transport.Send(_nodeId, from, new RequestVoteResponse(CurrentTermLocked(), false));
        }
    }

    private void OnPreVote(string from, RequestVote request)
    {
        bool grant = false;
        if (_role != Role.Leader
            && request.Term >= CurrentTermLocked()
            && CandidateLogUpToDate(request)
            && !HeardLeaderRecently())
        {
            if (_preCandidate && string.CompareOrdinal(request.CandidateId, _nodeId) >= 0)
            {
                _lastPreVoteGranted = false;
                _transport.Send(_nodeId, from, new RequestVoteResponse(CurrentTermLocked(), false, true));
                return;
            }

            _preCandidate = false;
            _preVotesGranted.Clear();
            grant = true;
        }

        _lastPreVoteGranted = grant;
        _transport.Send(_nodeId, from, new RequestVoteResponse(CurrentTermLocked(), grant, true));
    }

    private bool HeardLeaderRecently()
    {
        if (_lastLeaderContactMillis == long.MinValue)
        {
            return false;
        }

        return _clock.Millis - _lastLeaderContactMillis < PreVoteLeaderLeaseFactor * _heartbeatIntervalMillis;
    }

    private void OnRequestVoteResponse(string from, RequestVoteResponse response)
    {
        if (response.PreVote)
        {
            if (!_preCandidate)
            {
                return;
            }

            if (response.VoteGranted)
            {
                _preVotesGranted.Add(from);
                if (VoteQuorum(_preVotesGranted))
                {
                    BeginElection();
                }
            }

            return;
        }

        if (response.Term > CurrentTermLocked())
        {
            StepDown(response.Term);
            return;
        }

        if (_role != Role.Candidate || response.Term != CurrentTermLocked() || !response.VoteGranted)
        {
            return;
        }

        _votesGranted.Add(from);
        if (VoteQuorum(_votesGranted))
        {
            BecomeLeader();
        }
    }

    private void OnAppendEntries(string from, AppendEntries append)
    {
        if (append.Term < CurrentTermLocked())
        {
            _transport.Send(_nodeId, from, new AppendEntriesResponse(CurrentTermLocked(), false, 0, append.Stamp));
            return;
        }

        if (append.Term > CurrentTermLocked())
        {
            StepDown(append.Term);
        }
        else if (_role == Role.Candidate)
        {
            _role = Role.Follower;
        }

        _leaderId = append.LeaderId;
        _lastLeaderContactMillis = _clock.Millis;
        ResetElectionDeadline();
        ObserveTransfer(append.LeaderId);
        if (!PrevLogMatches(append.PrevLogIndex, append.PrevLogTerm))
        {
            _transport.Send(_nodeId, from, PrevConflict(append.PrevLogIndex, append.Stamp));
            return;
        }

        AppendEntriesIfPresent(append.Entries);
        long storedThrough = append.PrevLogIndex + append.Entries.Count;
        _lastAcceptedLeaderCommit = append.LeaderCommit;
        _commitIndex = Math.Max(_commitIndex, Math.Min(append.LeaderCommit, storedThrough));
        long term = CurrentTermLocked();
        long stamp = append.Stamp;
        string leader = from;
        if (_logNeedsForce)
        {
            _afterForce.Add(() => _transport.Send(_nodeId, leader, new AppendEntriesResponse(term, true, storedThrough, stamp)));
        }
        else
        {
            ApplyCommitted();
            _transport.Send(_nodeId, from, new AppendEntriesResponse(CurrentTermLocked(), true, storedThrough, stamp));
        }
    }

    private void ObserveTransfer(string leaderId)
    {
        if (_transferStatus != LeadershipTransferStatus.AwaitingWinner)
        {
            return;
        }

        _transferStatus = leaderId == _pendingTransferTo
            ? LeadershipTransferStatus.Succeeded
            : LeadershipTransferStatus.Aborted;
        _pendingTransferTo = null;
    }

    private bool PrevLogMatches(long prevLogIndex, long prevLogTerm)
    {
        if (prevLogIndex == 0)
        {
            return true;
        }

        if (prevLogIndex == _log.LastIncludedIndex)
        {
            return prevLogTerm == _log.LastIncludedTerm;
        }

        if (prevLogIndex < _log.FirstIndex || prevLogIndex > _log.LastIndex)
        {
            return false;
        }

        return _log.Read(prevLogIndex).Term == prevLogTerm;
    }

    private void AppendEntriesIfPresent(IReadOnlyList<LogEntry> entries)
    {
        if (entries.Count == 0)
        {
            return;
        }

        long insertAt = entries[0].Index;
        bool conflict = false;
        foreach (LogEntry entry in entries)
        {
            if (entry.Index <= _log.LastIncludedIndex)
            {
                continue;
            }

            if (entry.Index <= _log.LastIndex && _log.Read(entry.Index).Term != entry.Term)
            {
                conflict = true;
                break;
            }
        }

        if (conflict)
        {
            long truncateAt = Math.Max(insertAt, _log.FirstIndex);
            _log.TruncateFrom(truncateAt);
            RebuildMembershipFromLog();
        }

        foreach (LogEntry entry in entries)
        {
            if (entry.Index <= _log.LastIncludedIndex)
            {
                continue;
            }

            if (entry.Index == _log.LastIndex + 1)
            {
                AppendLocked(entry);
            }
        }
    }

    private void OnAppendEntriesResponse(string from, AppendEntriesResponse response)
    {
        if (response.Term > CurrentTermLocked())
        {
            StepDown(response.Term);
            return;
        }

        if (_role != Role.Leader || response.Term != CurrentTermLocked())
        {
            return;
        }

        if (response.Success)
        {
            long now = _clock.Millis;
            _lastAppendAckMillis[from] = now;
            _leaseAckMillis[from] = now;
            long matched = Math.Max(_matchIndex.GetValueOrDefault(from), response.MatchIndex);
            _matchIndex[from] = matched;
            _nextIndex[from] = matched + 1;
            MaybeAdvanceCommit();
            MaybeFinishTransfer();
            if (_role != Role.Leader)
            {
                return;
            }

            if (_nextIndex.GetValueOrDefault(from, 1) <= _log.LastIndex)
            {
                _mailbox.Enqueue(() => SendTo(from));
            }

            if (_pendingReadIndex > 0
                && CurrentTermLocked() == _pendingReadTerm
                && _voterPeers.Contains(from)
                && response.Stamp > 0
                && response.Stamp >= _readStampFloor.GetValueOrDefault(from, long.MaxValue))
            {
                _readIndexAcks.Add(from);
            }

            NoteLinearizableAck(from, response.Stamp);
            return;
        }

        long current = _nextIndex.GetValueOrDefault(from, 1);
        _nextIndex[from] = FastBackupNext(current, response);
        _mailbox.Enqueue(() => SendTo(from));
    }

    private long FastBackupNext(long current, AppendEntriesResponse response)
    {
        long raw;
        if (response.XTerm == 0)
        {
            raw = response.XLen == 0 ? current - 1 : response.XLen;
        }
        else
        {
            long lastWithTerm = LastIndexWithTerm(response.XTerm);
            raw = lastWithTerm > 0
                ? Math.Min(lastWithTerm + 1, current - 1)
                : response.XIndex == 0 ? current - 1 : response.XIndex;
        }

        if (raw < 1)
        {
            raw = 1;
        }

        if (raw >= current)
        {
            raw = Math.Max(1, current - 1);
        }

        return raw;
    }

    private long LastIndexWithTerm(long term)
    {
        long found = _log.LastIncludedTerm == term ? _log.LastIncludedIndex : 0;
        for (long i = _log.FirstIndex; i <= _log.LastIndex; i++)
        {
            if (_log.Read(i).Term == term)
            {
                found = i;
            }
        }

        return found;
    }

    private AppendEntriesResponse PrevConflict(long prevLogIndex, long stamp)
    {
        long last = _log.LastIndex;
        if (prevLogIndex > last)
        {
            return new AppendEntriesResponse(CurrentTermLocked(), false, 0, last + 1, 0, 0, stamp);
        }

        if (prevLogIndex < _log.FirstIndex)
        {
            return new AppendEntriesResponse(CurrentTermLocked(), false, 0, stamp);
        }

        long xTerm = _log.Read(prevLogIndex).Term;
        long xIndex = prevLogIndex;
        long first = _log.FirstIndex;
        while (xIndex > first && _log.Read(xIndex - 1).Term == xTerm)
        {
            xIndex--;
        }

        return new AppendEntriesResponse(CurrentTermLocked(), false, 0, 0, xTerm, xIndex, stamp);
    }

    private void OnInstallSnapshot(string from, InstallSnapshot install)
    {
        _snapshotChunksReceived++;
        if (install.Term < CurrentTermLocked())
        {
            _transport.Send(_nodeId, from, new InstallSnapshotResponse(CurrentTermLocked(), false, false));
            return;
        }

        if (install.Term > CurrentTermLocked())
        {
            StepDown(install.Term);
        }
        else if (_role == Role.Candidate)
        {
            _role = Role.Follower;
        }

        _leaderId = install.LeaderId;
        _lastLeaderContactMillis = _clock.Millis;
        ResetElectionDeadline();
        ObserveTransfer(install.LeaderId);
        if (install.Offset == 0)
        {
            _incomingSnapshot.Clear();
            _incomingSnapshotExpectedOffset = 0;
            _incomingLastIncludedIndex = install.LastIncludedIndex;
            _incomingLastIncludedTerm = install.LastIncludedTerm;
        }
        else if (install.Offset != _incomingSnapshotExpectedOffset
            || install.LastIncludedIndex != _incomingLastIncludedIndex
            || install.LastIncludedTerm != _incomingLastIncludedTerm)
        {
            _transport.Send(_nodeId, from, new InstallSnapshotResponse(CurrentTermLocked(), false, false));
            return;
        }

        byte[] chunk = install.DataUnsafe;
        _incomingSnapshot.AddRange(chunk);
        _incomingSnapshotExpectedOffset += chunk.Length;
        if (!install.Done)
        {
            _transport.Send(_nodeId, from, new InstallSnapshotResponse(CurrentTermLocked(), true, false));
            return;
        }

        byte[] bytes = _incomingSnapshot.ToArray();
        _incomingSnapshot.Clear();
        _incomingSnapshotExpectedOffset = 0;
        if (install.LastIncludedIndex < _log.LastIncludedIndex)
        {
            _transport.Send(
                _nodeId,
                from,
                new InstallSnapshotResponse(CurrentTermLocked(), true, true, _log.LastIncludedIndex, _log.LastIncludedTerm));
            return;
        }

        MembershipSnapshot.Decoded decoded = MembershipSnapshot.Decode(bytes);
        try
        {
            _stateMachine.RestoreChecked(_log.LastIncludedIndex, _lastApplied, decoded.StateMachineBytes);
        }
        catch (ArgumentException)
        {
            _transport.Send(_nodeId, from, new InstallSnapshotResponse(CurrentTermLocked(), false, false));
            return;
        }

        if (decoded.HasConfig)
        {
            InstallDecodedConfig(decoded);
        }

        byte[] persisted = MembershipSnapshot.Encode(AllVoters(), AllLearners(), _coldVoters, _cnewVoters, _stateMachine.Snapshot());
        _log.CompactThrough(install.LastIncludedIndex, install.LastIncludedTerm, persisted);
        if (_commitIndex < install.LastIncludedIndex)
        {
            _commitIndex = install.LastIncludedIndex;
        }

        _lastAcceptedLeaderCommit = Math.Max(_lastAcceptedLeaderCommit, install.LastIncludedIndex);
        if (_lastApplied < install.LastIncludedIndex)
        {
            _lastApplied = install.LastIncludedIndex;
        }

        _snapshotsInstalled++;
        _transport.Send(
            _nodeId,
            from,
            new InstallSnapshotResponse(CurrentTermLocked(), true, true, _log.LastIncludedIndex, _log.LastIncludedTerm));
    }

    private void OnInstallSnapshotResponse(string from, InstallSnapshotResponse response)
    {
        if (response.Term > CurrentTermLocked())
        {
            StepDown(response.Term);
            return;
        }

        if (_role != Role.Leader || response.Term != CurrentTermLocked())
        {
            return;
        }

        if (!response.Success)
        {
            _snapshotForceFull.Add(from);
            _snapshotNextOffset[from] = 0;
            _mailbox.Enqueue(() => SendSnapshot(from));
            return;
        }

        if (!response.Done)
        {
            int offset = _snapshotNextOffset.GetValueOrDefault(from);
            _snapshotNextOffset[from] = offset + _snapshotChunkSize;
            _mailbox.Enqueue(() => SendSnapshot(from));
            return;
        }

        _snapshotInFlight.Remove(from);
        _snapshotNextOffset.Remove(from);
        bool haveIndex = _snapshotGenIndex.TryGetValue(from, out long sentIndex);
        bool haveTerm = _snapshotGenTerm.TryGetValue(from, out long sentTerm);
        _snapshotGenIndex.Remove(from);
        _snapshotGenTerm.Remove(from);
        bool sameGeneration = haveIndex && haveTerm && response.InstalledIndex == sentIndex && response.InstalledTerm == sentTerm;
        bool followerAhead = haveIndex && response.InstalledIndex > sentIndex;
        if (!sameGeneration && !followerAhead)
        {
            _snapshotForceFull.Add(from);
            _snapshotNextOffset[from] = 0;
            _mailbox.Enqueue(() => SendSnapshot(from));
            return;
        }

        long matched = Math.Max(_matchIndex.GetValueOrDefault(from), response.InstalledIndex);
        _matchIndex[from] = matched;
        _peerInstalledIndex[from] = response.InstalledIndex;
        _nextIndex[from] = matched + 1;
        MaybeAdvanceCommit();
        if (_nextIndex.GetValueOrDefault(from, 1) <= _log.LastIndex)
        {
            _mailbox.Enqueue(() => SendTo(from));
        }
    }

    private void MaybeAdvanceCommit()
    {
        if (_role != Role.Leader)
        {
            return;
        }

        long previous = _commitIndex;
        for (long n = _log.LastIndex; n > _commitIndex; n--)
        {
            if (n <= _log.LastIncludedIndex)
            {
                break;
            }

            if (_log.Read(n).Term != CurrentTermLocked())
            {
                continue;
            }

            if (CanCommitIndex(n))
            {
                _commitIndex = n;
                break;
            }
        }

        ApplyCommitted();
        if (_commitIndex > previous)
        {
            ReplicateToAll();
        }
    }

    private void ApplyCommitted()
    {
        if (_logNeedsForce || _applyInFlight)
        {
            return;
        }

        if (_lastApplied < _log.LastIncludedIndex)
        {
            _lastApplied = _log.LastIncludedIndex;
        }

        if (_lastApplied >= _commitIndex)
        {
            SignalApplyWaiters();
            MaybeAutoSnapshot();
            MaybeAutoCnew();
            return;
        }

        long next = _lastApplied + 1;
        byte[] command = _log.Read(next).Command;
        if (MembershipCodec.IsMembership(command))
        {
            ApplyMembershipCommand(command);
            FinishApply(next);
            return;
        }

        _applyInFlight = true;
        long index = next;
        byte[] body = command;
        _applyExecutor(() =>
        {
            try
            {
                _stateMachine.Apply(index, body);
            }
            finally
            {
                _mailbox.Enqueue(() => FinishApply(index));
            }
        });
    }

    private void FinishApply(long index)
    {
        _applyInFlight = false;
        if (index != _lastApplied + 1)
        {
            return;
        }

        _lastApplied = index;
        SignalApplyWaiters();
        MaybeAutoSnapshot();
        MaybeAutoCnew();
        ApplyCommitted();
    }

    private void SignalApplyWaiters()
    {
        _applyWaiters.RemoveAll(waiter =>
        {
            if (_lastApplied >= waiter.Index)
            {
                waiter.Applied = true;
                return true;
            }

            return false;
        });
    }

    private void FailApplyWaiters()
    {
        foreach (ApplyWaiter waiter in _applyWaiters)
        {
            waiter.Failed = true;
        }

        _applyWaiters.Clear();
    }

    private void MaybeAutoSnapshot()
    {
        if (_snapshotThreshold <= 0)
        {
            return;
        }

        if (_lastApplied - _log.LastIncludedIndex >= _snapshotThreshold)
        {
            SnapshotThroughLocked(_lastApplied);
        }
    }

    private void SnapshotThroughLocked(long index)
    {
        if (index > _lastApplied)
        {
            throw new ArgumentException("snapshot index " + index + " is past lastApplied " + _lastApplied);
        }

        if (index < _log.LastIncludedIndex)
        {
            throw new ArgumentException("snapshot index " + index + " is before lastIncludedIndex " + _log.LastIncludedIndex);
        }

        if (index == 0 || index == _log.LastIncludedIndex)
        {
            return;
        }

        long term = _log.Read(index).Term;
        long baseIndex = _log.LastIncludedIndex;
        byte[] deltaSm = _stateMachine.SnapshotDelta(baseIndex);
        byte[] fullSm = _stateMachine.Snapshot();
        byte[] bytes = MembershipSnapshot.Encode(AllVoters(), AllLearners(), _coldVoters, _cnewVoters, fullSm);
        byte[] deltaFramed = MembershipSnapshot.Encode(AllVoters(), AllLearners(), _coldVoters, _cnewVoters, deltaSm);
        if (deltaFramed.Length < bytes.Length)
        {
            _lastDeltaFramed = deltaFramed;
            _deltaBaseIndex = baseIndex;
        }
        else
        {
            _lastDeltaFramed = null;
        }

        _snapshotForceFull.Clear();
        _snapshotExecutor(() => _mailbox.Enqueue(() => CompactSnapshot(index, term, bytes)));
        if (!_draining)
        {
            Drain();
        }
    }

    private void CompactSnapshot(long index, long term, byte[] bytes)
    {
        if (index <= _log.LastIncludedIndex)
        {
            return;
        }

        _log.CompactThrough(index, term, bytes);
    }

    private void StepDown(long newTerm)
    {
        _persistentState.SetCurrentTerm(newTerm);
        _role = Role.Follower;
        _leaderId = null;
        _votesGranted.Clear();
        _preVotesGranted.Clear();
        _preCandidate = false;
        _snapshotInFlight.Clear();
        _snapshotNextOffset.Clear();
        _snapshotGenIndex.Clear();
        _snapshotGenTerm.Clear();
        _peerInstalledIndex.Clear();
        _lastAppendAckMillis.Clear();
        _leaseAckMillis.Clear();
        _linearizableAckSendMillis.Clear();
        _aeSendMillis.Clear();
        if (_transferStatus == LeadershipTransferStatus.CatchingUp)
        {
            _transferStatus = LeadershipTransferStatus.Aborted;
            _pendingTransferTo = null;
        }

        _afterForce.Clear();
        _lastLeaderContactMillis = long.MinValue;
        FailApplyWaiters();
        AbortReadIndex();
        ResetElectionDeadline();
    }

    private bool HasCommittedEntryInCurrentTerm()
    {
        if (_commitIndex < 1)
        {
            return false;
        }

        long term = CurrentTermLocked();
        if (_commitIndex <= _log.LastIncludedIndex)
        {
            return _log.LastIncludedTerm == term;
        }

        return _log.Read(_commitIndex).Term == term;
    }

    private bool ReadIndexSatisfiedLocked()
    {
        if (_role != Role.Leader)
        {
            return false;
        }

        if (LinearizableLeaseValidLocked() && _lastApplied >= _commitIndex)
        {
            return true;
        }

        if (_pendingReadIndex < 1 || CurrentTermLocked() != _pendingReadTerm || _lastApplied < _pendingReadIndex)
        {
            return false;
        }

        if (InJointLocked())
        {
            var granted = new HashSet<string>(_readIndexAcks) { _nodeId };
            return VoteQuorum(granted);
        }

        return 1 + _readIndexAcks.Count >= Majority();
    }

    private bool FollowerReadLeaseValidLocked()
    {
        if (_role == Role.Leader || _lastLeaderContactMillis == long.MinValue)
        {
            return false;
        }

        if (_clock.Millis - _lastLeaderContactMillis >= PreVoteLeaderLeaseFactor * _heartbeatIntervalMillis + _followerLeasePauseSlackMillis)
        {
            return false;
        }

        return _lastApplied >= _lastAcceptedLeaderCommit;
    }

    private bool QuorumLeaseValidLocked()
    {
        if (_role != Role.Leader || !HasCommittedEntryInCurrentTerm())
        {
            return false;
        }

        if (_voterPeers.Count == 0)
        {
            return true;
        }

        if (InJointLocked())
        {
            return HeardQuorum(_coldVoters, _leaseAckMillis) && HeardQuorum(_cnewVoters, _leaseAckMillis);
        }

        return HeardCount(_leaseAckMillis) >= Majority();
    }

    private bool LinearizableLeaseValidLocked()
    {
        if (_role != Role.Leader || !HasCommittedEntryInCurrentTerm())
        {
            return false;
        }

        if (_voterPeers.Count == 0)
        {
            return true;
        }

        if (InJointLocked())
        {
            return HeardQuorum(_coldVoters, _linearizableAckSendMillis) && HeardQuorum(_cnewVoters, _linearizableAckSendMillis);
        }

        return HeardCount(_linearizableAckSendMillis) >= Majority();
    }

    private int HeardCount(Dictionary<string, long> acks)
    {
        int heard = 1;
        long now = _clock.Millis;
        foreach (string peer in _voterPeers)
        {
            long last = acks.GetValueOrDefault(peer, long.MinValue);
            if (last != long.MinValue && now - last < _electionTimeoutMillis)
            {
                heard++;
            }
        }

        return heard;
    }

    private void NoteLinearizableAck(string from, long stamp)
    {
        if (stamp <= 0 || !_aeSendMillis.TryGetValue(from, out Dictionary<long, long>? sent) || !sent.TryGetValue(stamp, out long at))
        {
            return;
        }

        if (!_linearizableAckSendMillis.TryGetValue(from, out long previous) || at > previous)
        {
            _linearizableAckSendMillis[from] = at;
        }

        foreach (long older in sent.Keys.Where(older => older < stamp).ToArray())
        {
            sent.Remove(older);
        }
    }

    private void AbortReadIndex()
    {
        _pendingReadIndex = 0;
        _pendingReadTerm = 0;
        _readIndexAcks.Clear();
        _readStampFloor.Clear();
    }

    private bool CandidateLogUpToDate(RequestVote request)
    {
        long localTerm = _log.LastTerm;
        if (request.LastLogTerm != localTerm)
        {
            return request.LastLogTerm > localTerm;
        }

        return request.LastLogIndex >= _log.LastIndex;
    }

    private void ResetElectionDeadline()
    {
        long span = _electionJitter(_electionTimeoutMillis);
        if (span < _electionTimeoutMillis)
        {
            span = _electionTimeoutMillis;
        }

        long cap = 2 * _electionTimeoutMillis;
        if (span > cap)
        {
            span = cap;
        }

        _electionDeadline = _clock.Millis + span;
    }

    private void MaybeCheckQuorum(long now)
    {
        if (_voterPeers.Count == 0)
        {
            return;
        }

        if (InJointLocked())
        {
            if (!HeardQuorum(_coldVoters, _lastAppendAckMillis, now) || !HeardQuorum(_cnewVoters, _lastAppendAckMillis, now))
            {
                StepDown(CurrentTermLocked());
            }

            return;
        }

        int heard = 1;
        foreach (string peer in _voterPeers)
        {
            long last = _lastAppendAckMillis.GetValueOrDefault(peer, long.MinValue);
            if (last != long.MinValue && now - last < _electionTimeoutMillis)
            {
                heard++;
            }
        }

        if (heard < Majority())
        {
            StepDown(CurrentTermLocked());
        }
    }

    private void AppendLocked(LogEntry entry)
    {
        _log.Append(entry);
        _logNeedsForce = true;
        ActivateAppendedConfig(entry.Command);
    }

    private int Majority()
    {
        int voters = _voterPeers.Count + (_selfVoter ? 1 : 0);
        if (voters <= 0)
        {
            return 1;
        }

        return voters / 2 + 1;
    }

    private bool RemoveLeavesMajority()
    {
        int voters = _voterPeers.Count + (_selfVoter ? 1 : 0);
        int remaining = voters - 1;
        return remaining >= voters / 2 + 1;
    }

    private IEnumerable<string> ReplicationTargets()
    {
        foreach (string peer in _voterPeers)
        {
            yield return peer;
        }

        foreach (string peer in _learnerPeers)
        {
            yield return peer;
        }
    }

    private HashSet<string> AllVoters()
    {
        var all = new HashSet<string>(_voterPeers);
        if (_selfVoter)
        {
            all.Add(_nodeId);
        }

        return all;
    }

    private HashSet<string> AllLearners()
    {
        var all = new HashSet<string>(_learnerPeers);
        if (_selfLearner)
        {
            all.Add(_nodeId);
        }

        return all;
    }

    private void ApplyMembershipCommand(byte[] command)
    {
        MembershipCodec.Change change = MembershipCodec.Decode(command);
        HashSet<string> learners = AllLearners();
        if (change.Kind == MembershipCodec.Kind.Joint)
        {
            _coldVoters.Clear();
            _coldVoters.UnionWith(change.OldVoters);
            _cnewVoters.Clear();
            _cnewVoters.UnionWith(change.NewVoters);
            var union = new HashSet<string>(_coldVoters);
            union.UnionWith(_cnewVoters);
            InstallMembership(union, learners);
            return;
        }

        if (change.Kind == MembershipCodec.Kind.Cnew)
        {
            _coldVoters.Clear();
            _cnewVoters.Clear();
            InstallMembership(change.NewVoters.ToHashSet(), learners);
            return;
        }

        HashSet<string> voters = AllVoters();
        string id = change.NodeId;
        switch (change.Kind)
        {
            case MembershipCodec.Kind.AddLearner:
                voters.Remove(id);
                learners.Add(id);
                break;
            case MembershipCodec.Kind.PromoteVoter:
                learners.Remove(id);
                voters.Add(id);
                break;
            case MembershipCodec.Kind.RemoveServer:
                voters.Remove(id);
                learners.Remove(id);
                break;
        }

        InstallMembership(voters, learners);
    }

    private bool InJointLocked() => _coldVoters.Count > 0 && _cnewVoters.Count > 0;

    private bool CanCommitIndex(long n)
    {
        if (InJointLocked())
        {
            return ReplicatedOn(_coldVoters, n) && ReplicatedOn(_cnewVoters, n);
        }

        int stored = _selfVoter && _log.DurableIndex >= n ? 1 : 0;
        foreach (string peer in _voterPeers)
        {
            if (_matchIndex.GetValueOrDefault(peer) >= n)
            {
                stored++;
            }
        }

        return stored >= Majority();
    }

    private bool ReplicatedOn(HashSet<string> voters, long n)
    {
        int stored = 0;
        foreach (string id in voters)
        {
            if (id == _nodeId)
            {
                if (_log.DurableIndex >= n)
                {
                    stored++;
                }
            }
            else if (_matchIndex.GetValueOrDefault(id) >= n)
            {
                stored++;
            }
        }

        return stored >= voters.Count / 2 + 1;
    }

    private bool VoteQuorum(HashSet<string> granted)
    {
        if (InJointLocked())
        {
            return QuorumHits(granted, _coldVoters) && QuorumHits(granted, _cnewVoters);
        }

        return granted.Count >= Majority();
    }

    private static bool QuorumHits(HashSet<string> granted, HashSet<string> voters)
    {
        int hits = voters.Count(granted.Contains);
        return hits >= voters.Count / 2 + 1;
    }

    private bool HeardQuorum(IReadOnlySet<string> voters, Dictionary<string, long> acks) =>
        HeardQuorum(voters, acks, _clock.Millis);

    private bool HeardQuorum(IReadOnlySet<string> voters, Dictionary<string, long> acks, long now)
    {
        int heard = 0;
        foreach (string id in voters)
        {
            if (id == _nodeId)
            {
                heard++;
                continue;
            }

            long last = acks.GetValueOrDefault(id, long.MinValue);
            if (last != long.MinValue && now - last < _electionTimeoutMillis)
            {
                heard++;
            }
        }

        return heard >= voters.Count / 2 + 1;
    }

    private void InstallMembership(HashSet<string> voters, HashSet<string> learners)
    {
        _selfVoter = voters.Contains(_nodeId);
        _selfLearner = learners.Contains(_nodeId);
        _voterPeers.Clear();
        _voterPeers.UnionWith(voters);
        _voterPeers.Remove(_nodeId);
        _learnerPeers.Clear();
        _learnerPeers.UnionWith(learners);
        _learnerPeers.Remove(_nodeId);
        foreach (string peer in ReplicationTargets())
        {
            _nextIndex.TryAdd(peer, 1);
            _matchIndex.TryAdd(peer, 0);
        }

        _persistentState.PersistMembership(
            MembershipSnapshot.EncodeConfig(AllVoters(), AllLearners(), _coldVoters, _cnewVoters));
    }

    private void InstallDecodedConfig(MembershipSnapshot.Decoded decoded)
    {
        _coldVoters.Clear();
        _cnewVoters.Clear();
        if (decoded.InJoint)
        {
            _coldVoters.UnionWith(decoded.Cold);
            _cnewVoters.UnionWith(decoded.Cnew);
        }

        InstallMembership(decoded.Voters.ToHashSet(), decoded.Learners.ToHashSet());
    }

    private void ActivateAppendedConfig(byte[] command)
    {
        if (!MembershipCodec.IsMembership(command))
        {
            return;
        }

        MembershipCodec.Change change = MembershipCodec.Decode(command);
        if (change.Kind is MembershipCodec.Kind.Joint or MembershipCodec.Kind.Cnew)
        {
            ApplyMembershipCommand(command);
        }
    }

    private void ReplayLogConfigs()
    {
        for (long i = _log.FirstIndex; i <= _log.LastIndex; i++)
        {
            byte[] command = _log.Read(i).Command;
            if (!MembershipCodec.IsMembership(command))
            {
                continue;
            }

            MembershipCodec.Change change = MembershipCodec.Decode(command);
            if (change.Kind is MembershipCodec.Kind.Joint or MembershipCodec.Kind.Cnew || i <= _lastApplied)
            {
                ApplyMembershipCommand(command);
            }
        }
    }

    private void RebuildMembershipFromLog()
    {
        if (_log.LastIncludedIndex > 0)
        {
            MembershipSnapshot.Decoded decoded = MembershipSnapshot.Decode(_log.SnapshotBytes());
            if (decoded.HasConfig)
            {
                InstallDecodedConfig(decoded);
            }
            else
            {
                ResetToBootstrapMembership();
            }
        }
        else
        {
            ResetToBootstrapMembership();
        }

        ReplayLogConfigs();
    }

    private void ResetToBootstrapMembership()
    {
        _coldVoters.Clear();
        _cnewVoters.Clear();
        InstallMembership(new HashSet<string>(_bootstrapVoters), new HashSet<string>(_bootstrapLearners));
    }

    private void MaybeAutoCnew()
    {
        if (_holdJoint || _role != Role.Leader || !InJointLocked() || LogHasCnew())
        {
            return;
        }

        if (LatestJointIndex() < 1 || LatestJointIndex() > _commitIndex)
        {
            return;
        }

        ProposeLocked(MembershipCodec.EncodeCnew(_cnewVoters.ToHashSet()));
    }

    private bool LogHasCnew()
    {
        for (long i = _log.FirstIndex; i <= _log.LastIndex; i++)
        {
            byte[] command = _log.Read(i).Command;
            if (MembershipCodec.IsMembership(command) && MembershipCodec.Decode(command).Kind == MembershipCodec.Kind.Cnew)
            {
                return true;
            }
        }

        return false;
    }

    private long LatestJointIndex()
    {
        long found = 0;
        for (long i = _log.FirstIndex; i <= _log.LastIndex; i++)
        {
            byte[] command = _log.Read(i).Command;
            if (MembershipCodec.IsMembership(command) && MembershipCodec.Decode(command).Kind == MembershipCodec.Kind.Joint)
            {
                found = i;
            }
        }

        return found;
    }

    private void OnTimeoutNow(TimeoutNow timeout)
    {
        if (timeout.Term < CurrentTermLocked() || !_selfVoter)
        {
            return;
        }

        if (timeout.Term > CurrentTermLocked())
        {
            CampaignAt(timeout.Term);
            return;
        }

        BeginElection();
    }

    private long CurrentTermLocked() => _persistentState.CurrentTerm;

    private static string RequireNodeId(string nodeId)
    {
        if (string.IsNullOrWhiteSpace(nodeId))
        {
            throw new ArgumentException("node id must be non-blank");
        }

        return nodeId;
    }

    private static HashSet<string> CopyPeers(string self, IEnumerable<string> peerIds)
    {
        var copy = new HashSet<string>();
        foreach (string peer in peerIds)
        {
            string id = RequireNodeId(peer);
            if (id == self)
            {
                throw new ArgumentException("peer list must not contain self");
            }

            if (!copy.Add(id))
            {
                throw new ArgumentException("duplicate peer id: " + id);
            }
        }

        return copy;
    }

    private static long RequirePositive(TimeSpan duration, string name)
    {
        if (duration <= TimeSpan.Zero)
        {
            throw new ArgumentException(name + " must be > 0");
        }

        return (long)duration.TotalMilliseconds;
    }
}
