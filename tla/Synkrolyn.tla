---- MODULE Synkrolyn ----
\* Bounded model of Synkrolyn Raft as implemented.
\* Entries are terms. One atomic step is one RPC.
\* Not modeled: membership, snapshots, sockets, the XLen/XTerm fast
\* backup (a mismatch drops the follower's last entry), leadership transfer.
\* Replication walks the follower log: one dropped entry, or one appended entry.
\* CommitPriorTerms commits a replicated entry from an older term.
\* That is the figure-8 bug. MC.cfg keeps it FALSE.

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Server, Nil, MaxTerm, MaxLogLen, Majority, CommitPriorTerms

ASSUME /\ Server # {}
       /\ Nil \notin Server
       /\ MaxTerm \in Nat \ {0}
       /\ MaxLogLen \in Nat \ {0}
       /\ Majority \in Nat \ {0}
       /\ CommitPriorTerms \in BOOLEAN
       /\ \A s \in Server : ToString(s) \in {"n1", "n2", "n3", "n4", "n5"}

Follower == "Follower"
Candidate == "Candidate"
Leader == "Leader"
Roles == {Follower, Candidate, Leader}

VARIABLES currentTerm, votedFor, log, commitIndex, role,
          votesGranted, forcedIndex,
          preCand, preVotes, restartGate, contact, acked,
          readPrefix, bugStep

vars == <<currentTerm, votedFor, log, commitIndex, role,
          votesGranted, forcedIndex,
          preCand, preVotes, restartGate, contact, acked,
          readPrefix, bugStep>>

MinNat(a, b) == IF a <= b THEN a ELSE b
MaxNat(a, b) == IF a >= b THEN a ELSE b

LastIndex(s) == Len(log[s])
LastTerm(s) == IF log[s] = << >> THEN 0 ELSE log[s][Len(log[s])]

\* CompareOrdinal order of the model ids. A higher rank is a higher id.
\* Names, not a config sequence: this TLC rejects << >> in the cfg file.
IdNames == <<"n1", "n2", "n3", "n4", "n5">>
Rank(s) ==
  LET hits == {i \in 1..Len(IdNames) : IdNames[i] = ToString(s)}
  IN CHOOSE i \in hits : TRUE
Named(name) == CHOOSE s \in Server : ToString(s) = name

LogUpToDate(cand, voter) ==
  \/ LastTerm(cand) > LastTerm(voter)
  \/ /\ LastTerm(cand) = LastTerm(voter)
     /\ LastIndex(cand) >= LastIndex(voter)

LogStrictlyNewer(cand, voter) ==
  \/ LastTerm(cand) > LastTerm(voter)
  \/ /\ LastTerm(cand) = LastTerm(voter)
     /\ LastIndex(cand) > LastIndex(voter)

\* VoteBlockedByLeaderContact returns immediately when there has been no
\* leader contact, so the restart window blocks votes only while contact is set.
\* Start sets both. Timeout is refused while the window is still armed.
VoteBlocked(s) == contact[s]

\* Paper 5.4.2, unless CommitPriorTerms skips the current-term test.
\* The leader counts itself only after Force has covered n.
CanCommit(s, n) ==
  /\ role[s] = Leader
  /\ n \in 1..Len(log[s])
  /\ \/ CommitPriorTerms
     \/ IF n < 1 \/ n > Len(log[s]) THEN FALSE
        ELSE log[s][n] = currentTerm[s]
  /\ Cardinality({t \in Server :
                    /\ Len(log[t]) >= n
                    /\ SubSeq(log[t], 1, n) = SubSeq(log[s], 1, n)
                    /\ (t # s \/ forcedIndex[s] >= n)}) >= Majority

TypeOK ==
  /\ currentTerm \in [Server -> 0..MaxTerm]
  /\ votedFor \in [Server -> Server \union {Nil}]
  /\ log \in [Server -> Seq(1..MaxTerm)]
  /\ \A s \in Server : Len(log[s]) <= MaxLogLen
  /\ commitIndex \in [Server -> 0..MaxLogLen]
  /\ \A s \in Server : commitIndex[s] <= Len(log[s])
  /\ role \in [Server -> Roles]
  /\ votesGranted \in [Server -> SUBSET Server]
  /\ forcedIndex \in [Server -> 0..MaxLogLen]
  /\ \A s \in Server : forcedIndex[s] <= Len(log[s])
  /\ preCand \in [Server -> BOOLEAN]
  /\ preVotes \in [Server -> SUBSET Server]
  /\ restartGate \in [Server -> BOOLEAN]
  /\ contact \in [Server -> BOOLEAN]
  /\ acked \in [Server -> SUBSET Server]
  /\ readPrefix \in Seq(1..MaxTerm)
  /\ Len(readPrefix) <= MaxLogLen
  /\ bugStep \in 0..8

ElectionSafety ==
  \A t \in 0..MaxTerm :
    Cardinality({s \in Server : role[s] = Leader /\ currentTerm[s] = t}) <= 1

LogMatching ==
  \A s, t \in Server :
    \A i \in 1..MinNat(Len(log[s]), Len(log[t])) :
      log[s][i] = log[t][i] => SubSeq(log[s], 1, i) = SubSeq(log[t], 1, i)

LeaderCompleteness ==
  \A s, t \in Server :
    \A n \in 1..commitIndex[s] :
      (role[t] = Leader /\ currentTerm[t] > log[s][n]) =>
        /\ Len(log[t]) >= n
        /\ log[t][n] = log[s][n]

StateMachineSafety ==
  \A s, t \in Server :
    \A i \in 1..MinNat(commitIndex[s], commitIndex[t]) :
      /\ i <= Len(log[s])
      /\ i <= Len(log[t])
      /\ log[s][i] = log[t][i]

\* A recorded read prefix stays committed on every server.
ReadLinearizability ==
  \A s \in Server :
    \A i \in 1..MinNat(Len(readPrefix), commitIndex[s]) :
      /\ i <= Len(log[s])
      /\ log[s][i] = readPrefix[i]

LeaderAppendOnly ==
  \A s \in Server :
    \/ role[s] # Leader
    \/ role'[s] # Leader
    \/ currentTerm'[s] # currentTerm[s]
    \/ /\ Len(log'[s]) >= Len(log[s])
       /\ SubSeq(log'[s], 1, Len(log[s])) = log[s]

LeaderAppendOnlyProperty == [][LeaderAppendOnly]_vars

Init ==
  /\ currentTerm = [s \in Server |-> 0]
  /\ votedFor = [s \in Server |-> Nil]
  /\ log = [s \in Server |-> << >>]
  /\ commitIndex = [s \in Server |-> 0]
  /\ role = [s \in Server |-> Follower]
  /\ votesGranted = [s \in Server |-> {}]
  /\ forcedIndex = [s \in Server |-> 0]
  /\ preCand = [s \in Server |-> FALSE]
  /\ preVotes = [s \in Server |-> {}]
  /\ restartGate = [s \in Server |-> FALSE]
  /\ contact = [s \in Server |-> FALSE]
  /\ acked = [s \in Server |-> {}]
  /\ readPrefix = << >>
  /\ bugStep = 0

\* Figure 8, just before the bad commit. n1..n3 hold term 2; n4..n5 hold term 3.
InitBug ==
  /\ Cardinality(Server) >= 5
  /\ LET a == Named("n1")
         b == Named("n2")
         c == Named("n3")
     IN /\ currentTerm = [s \in Server |-> 4]
        /\ votedFor = [s \in Server |-> Nil]
        /\ log = [s \in Server |-> IF s \in {a, b, c} THEN <<2>> ELSE <<3>>]
        /\ commitIndex = [s \in Server |-> 0]
        /\ role = [s \in Server |-> IF s = a THEN Leader ELSE Follower]
        /\ votesGranted = [s \in Server |-> {}]
        /\ forcedIndex = [s \in Server |-> 1]
        /\ preCand = [s \in Server |-> FALSE]
        /\ preVotes = [s \in Server |-> {}]
        /\ restartGate = [s \in Server |-> FALSE]
        /\ contact = [s \in Server |-> FALSE]
        /\ acked = [s \in Server |-> IF s = a THEN {a, b, c} ELSE {}]
        /\ readPrefix = << >>
        /\ bugStep = 0

\* Election deadline. Does not increment the term; BeginPreVote does not either.
Timeout(s, step) ==
  /\ role[s] \in {Follower, Candidate}
  /\ ~restartGate[s]
  /\ currentTerm[s] < MaxTerm
  /\ \/ ~preCand[s]
     \/ preVotes[s] # {s}
     \/ contact[s]
  /\ preCand' = [preCand EXCEPT ![s] = TRUE]
  /\ preVotes' = [preVotes EXCEPT ![s] = {s}]
  /\ contact' = [contact EXCEPT ![s] = FALSE]
  /\ bugStep' = step
  /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, role, votesGranted,
                 forcedIndex, restartGate, acked, readPrefix>>

PreVote(from, to, step) ==
  /\ from # to
  /\ preCand[from]
  /\ role[from] # Leader
  /\ role[to] # Leader
  /\ ~VoteBlocked(to)
  /\ currentTerm[from] < MaxTerm
  /\ currentTerm[from] + 1 >= currentTerm[to]
  /\ LogUpToDate(from, to)
  /\ ~ (preCand[to] /\ Rank(from) >= Rank(to) /\ ~LogStrictlyNewer(from, to))
  /\ to \notin preVotes[from]
  /\ preVotes' = [p \in Server |->
                    IF p = from THEN preVotes[from] \union {to}
                    ELSE IF p = to THEN {}
                    ELSE preVotes[p]]
  /\ preCand' = [preCand EXCEPT ![to] = FALSE]
  /\ bugStep' = step
  /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, role, votesGranted,
                 forcedIndex, restartGate, contact, acked, readPrefix>>

BeginElection(s, step) ==
  /\ preCand[s]
  /\ Cardinality(preVotes[s]) >= Majority
  /\ currentTerm[s] < MaxTerm
  /\ currentTerm' = [currentTerm EXCEPT ![s] = @ + 1]
  /\ role' = [role EXCEPT ![s] = Candidate]
  /\ votedFor' = [votedFor EXCEPT ![s] = s]
  /\ votesGranted' = [votesGranted EXCEPT ![s] = {s}]
  /\ preCand' = [preCand EXCEPT ![s] = FALSE]
  /\ preVotes' = [preVotes EXCEPT ![s] = {}]
  /\ bugStep' = step
  /\ UNCHANGED <<log, commitIndex, forcedIndex, restartGate,
                 contact, acked, readPrefix>>

RequestVote(from, to, step) ==
  /\ from # to
  /\ role[from] = Candidate
  /\ ~preCand[from]
  /\ \/ /\ currentTerm[to] > currentTerm[from]
        /\ currentTerm' = [currentTerm EXCEPT ![from] = currentTerm[to]]
        /\ role' = [role EXCEPT ![from] = Follower]
        /\ votedFor' = [votedFor EXCEPT ![from] = Nil]
        /\ votesGranted' = [votesGranted EXCEPT ![from] = {}]
        /\ preCand' = [preCand EXCEPT ![from] = FALSE]
        /\ preVotes' = [preVotes EXCEPT ![from] = {}]
        /\ contact' = [contact EXCEPT ![from] = FALSE]
        /\ acked' = [acked EXCEPT ![from] = {}]
        /\ bugStep' = step
        /\ UNCHANGED <<log, commitIndex, forcedIndex, restartGate, readPrefix>>
     \/ /\ currentTerm[from] >= currentTerm[to]
        /\ ~VoteBlocked(to)
        /\ LET higher == currentTerm[from] > currentTerm[to]
               logOk == LogUpToDate(from, to)
               grant == logOk /\ (higher \/ votedFor[to] \in {Nil, from})
           IN /\ grant \/ higher
              /\ \/ higher
                 \/ votedFor[to] # from
                 \/ to \notin votesGranted[from]
              /\ currentTerm' = [currentTerm EXCEPT ![to] = currentTerm[from]]
              /\ votedFor' = [votedFor EXCEPT ![to] =
                                IF higher THEN (IF logOk THEN from ELSE Nil)
                                ELSE from]
              /\ role' = [role EXCEPT ![to] = IF higher THEN Follower ELSE role[to]]
              /\ contact' = [contact EXCEPT ![to] = IF higher THEN FALSE ELSE contact[to]]
              /\ preCand' = [preCand EXCEPT ![to] = IF higher THEN FALSE ELSE preCand[to]]
              /\ preVotes' = [preVotes EXCEPT ![to] = IF higher THEN {} ELSE preVotes[to]]
              /\ votesGranted' = [votesGranted EXCEPT
                                    ![to] = IF higher THEN {} ELSE votesGranted[to],
                                    ![from] = IF grant THEN votesGranted[from] \union {to}
                                              ELSE votesGranted[from]]
              /\ acked' = [acked EXCEPT ![to] =
                             IF higher /\ role[to] = Leader THEN {} ELSE acked[to]]
              /\ bugStep' = step
              /\ UNCHANGED <<log, commitIndex, forcedIndex, restartGate, readPrefix>>

BecomeLeader(s, step) ==
  /\ role[s] = Candidate
  /\ ~preCand[s]
  /\ Cardinality(votesGranted[s]) >= Majority
  /\ Len(log[s]) < MaxLogLen
  /\ LET newLog == Append(log[s], currentTerm[s])
     IN /\ role' = [role EXCEPT ![s] = Leader]
        /\ log' = [log EXCEPT ![s] = newLog]
        /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
        /\ acked' = [acked EXCEPT ![s] = {s}]
        /\ preCand' = [preCand EXCEPT ![s] = FALSE]
        /\ preVotes' = [preVotes EXCEPT ![s] = {}]
        /\ bugStep' = step
        /\ UNCHANGED <<currentTerm, votedFor, commitIndex, forcedIndex,
                       restartGate, contact, readPrefix>>

ClientAppend(s, step) ==
  /\ role[s] = Leader
  /\ currentTerm[s] \in 1..MaxTerm
  /\ Len(log[s]) < MaxLogLen
  /\ log' = [log EXCEPT ![s] = Append(@, currentTerm[s])]
  /\ bugStep' = step
  /\ UNCHANGED <<currentTerm, votedFor, commitIndex, role, votesGranted,
                 forcedIndex, preCand, preVotes, restartGate,
                 contact, acked, readPrefix>>

Force(s, step) ==
  /\ forcedIndex[s] < Len(log[s])
  /\ forcedIndex' = [forcedIndex EXCEPT ![s] = Len(log[s])]
  /\ bugStep' = step
  /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, role, votesGranted,
                 preCand, preVotes, restartGate, contact, acked, readPrefix>>

AdvanceCommit(s, step) ==
  /\ role[s] = Leader
  /\ LET ns == {n \in (commitIndex[s] + 1)..Len(log[s]) : CanCommit(s, n)}
     IN /\ ns # {}
        /\ commitIndex' = [commitIndex EXCEPT ![s] =
                             CHOOSE n \in ns : \A m \in ns : m <= n]
  /\ bugStep' = step
  /\ UNCHANGED <<currentTerm, votedFor, log, role, votesGranted,
                 forcedIndex, preCand, preVotes, restartGate, contact, acked, readPrefix>>

\* Follower log is a term-by-term prefix of the leader log.
IsPrefix(a, b) ==
  /\ Len(a) <= Len(b)
  /\ \A i \in 1..Len(a) : a[i] = b[i]

AppendEntries(s, t, step) ==
  /\ s # t
  /\ role[s] = Leader
  /\ \/ /\ currentTerm[s] < currentTerm[t]
        /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[t]]
        /\ role' = [role EXCEPT ![s] = Follower]
        /\ votedFor' = [votedFor EXCEPT ![s] = Nil]
        /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
        /\ preCand' = [preCand EXCEPT ![s] = FALSE]
        /\ preVotes' = [preVotes EXCEPT ![s] = {}]
        /\ contact' = [contact EXCEPT ![s] = FALSE]
        /\ acked' = [acked EXCEPT ![s] = {}]
        /\ bugStep' = step
        /\ UNCHANGED <<log, commitIndex, forcedIndex, restartGate, readPrefix>>
     \/ /\ currentTerm[s] >= currentTerm[t]
        /\ \/ role[t] # Leader
           \/ currentTerm[s] > currentTerm[t]
        /\ LET flog == log[t]
               llog == log[s]
               termUp == currentTerm[t] < currentTerm[s]
               newRole == IF termUp \/ role[t] = Candidate THEN Follower ELSE role[t]
               conflict == ~IsPrefix(flog, llog) /\ Len(flog) > 0
               extend == IsPrefix(flog, llog) /\ Len(flog) < Len(llog)
               newLog == IF conflict THEN SubSeq(flog, 1, Len(flog) - 1)
                         ELSE IF extend THEN Append(flog, llog[Len(flog) + 1])
                         ELSE flog
               newCommit == IF conflict THEN MinNat(commitIndex[t], Len(newLog))
                            ELSE MaxNat(commitIndex[t], MinNat(commitIndex[s], Len(newLog)))
               newForced == IF conflict THEN MinNat(forcedIndex[t], Len(newLog))
                            ELSE IF extend THEN Len(newLog)
                            ELSE forcedIndex[t]
           IN /\ Len(newLog) <= MaxLogLen
              /\ \/ conflict
                 \/ extend
                 \/ termUp
                 \/ role[t] # newRole
                 \/ ~contact[t]
                 \/ preCand[t]
                 \/ t \notin acked[s]
                 \/ newCommit # commitIndex[t]
                 \/ newForced # forcedIndex[t]
              /\ log' = [log EXCEPT ![t] = newLog]
              /\ commitIndex' = [commitIndex EXCEPT ![t] = newCommit]
              /\ forcedIndex' = [forcedIndex EXCEPT ![t] = newForced]
              /\ currentTerm' = [currentTerm EXCEPT ![t] = currentTerm[s]]
              /\ role' = [role EXCEPT ![t] = newRole]
              /\ votedFor' = IF termUp THEN [votedFor EXCEPT ![t] = Nil] ELSE votedFor
              /\ votesGranted' = IF newRole # role[t]
                                 THEN [votesGranted EXCEPT ![t] = {}]
                                 ELSE votesGranted
              /\ preCand' = [preCand EXCEPT ![t] = FALSE]
              /\ preVotes' = [preVotes EXCEPT ![t] = {}]
              /\ contact' = [contact EXCEPT ![t] = TRUE]
              /\ acked' = [acked EXCEPT ![s] = acked[s] \union {t}]
              /\ bugStep' = step
              /\ UNCHANGED <<restartGate, readPrefix>>

CheckQuorum(s, step) ==
  /\ role[s] = Leader
  /\ Cardinality(acked[s]) < Majority
  /\ role' = [role EXCEPT ![s] = Follower]
  /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
  /\ preCand' = [preCand EXCEPT ![s] = FALSE]
  /\ preVotes' = [preVotes EXCEPT ![s] = {}]
  /\ contact' = [contact EXCEPT ![s] = FALSE]
  /\ acked' = [acked EXCEPT ![s] = {}]
  /\ bugStep' = step
  /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, forcedIndex,
                 restartGate, readPrefix>>

\* Crash. Durable term, vote, and forced prefix stay. The unforced suffix is dropped.
\* Start then sets leader contact and the restart vote window.
Restart(s, step) ==
  /\ currentTerm[s] > 0
  /\ LET kept == IF forcedIndex[s] = 0 THEN << >>
                 ELSE SubSeq(log[s], 1, forcedIndex[s])
     IN /\ \/ role[s] # Follower
           \/ preCand[s]
           \/ ~contact[s]
           \/ votesGranted[s] # {}
           \/ acked[s] # {}
           \/ ~restartGate[s]
           \/ preVotes[s] # {}
           \/ log[s] # kept
           \/ commitIndex[s] > forcedIndex[s]
        /\ log' = [log EXCEPT ![s] = kept]
        /\ commitIndex' = [commitIndex EXCEPT ![s] = MinNat(commitIndex[s], forcedIndex[s])]
        /\ role' = [role EXCEPT ![s] = Follower]
        /\ restartGate' = [restartGate EXCEPT ![s] = TRUE]
        /\ contact' = [contact EXCEPT ![s] = TRUE]
        /\ preCand' = [preCand EXCEPT ![s] = FALSE]
        /\ preVotes' = [preVotes EXCEPT ![s] = {}]
        /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
        /\ acked' = [acked EXCEPT ![s] = {}]
        /\ bugStep' = step
        /\ UNCHANGED <<currentTerm, votedFor, forcedIndex, readPrefix>>

ExpireRestartGate(s, step) ==
  /\ restartGate[s]
  /\ restartGate' = [restartGate EXCEPT ![s] = FALSE]
  /\ bugStep' = step
  /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, role, votesGranted,
                 forcedIndex, preCand, preVotes, contact, acked, readPrefix>>

\* Lease read. Observes the committed prefix once a current-term entry is
\* committed and a quorum of append acks is fresh. Apply lag is not separate.
Read(s, step) ==
  /\ role[s] = Leader
  /\ Cardinality(acked[s]) >= Majority
  /\ IF commitIndex[s] < 1 THEN FALSE
     ELSE log[s][commitIndex[s]] = currentTerm[s]
  /\ LET prefix == SubSeq(log[s], 1, commitIndex[s])
     IN /\ Len(prefix) >= Len(readPrefix)
        /\ prefix # readPrefix
        /\ readPrefix' = prefix
  /\ bugStep' = step
  /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, role, votesGranted,
                 forcedIndex, preCand, preVotes, restartGate, contact, acked>>

FullNext ==
  \/ \E s \in Server : Timeout(s, bugStep)
  \/ \E from, to \in Server : PreVote(from, to, bugStep)
  \/ \E s \in Server : BeginElection(s, bugStep)
  \/ \E from, to \in Server : RequestVote(from, to, bugStep)
  \/ \E s \in Server : BecomeLeader(s, bugStep)
  \/ \E s \in Server : ClientAppend(s, bugStep)
  \/ \E s \in Server : Force(s, bugStep)
  \/ \E s \in Server : AdvanceCommit(s, bugStep)
  \/ \E s, t \in Server : AppendEntries(s, t, bugStep)
  \/ \E s \in Server : CheckQuorum(s, bugStep)
  \/ \E s \in Server : Restart(s, bugStep)
  \/ \E s \in Server : ExpireRestartGate(s, bugStep)
  \/ \E s \in Server : Read(s, bugStep)

\* One path: bad commit, then the term-3 log wins the election and appends.
BugNext ==
  IF Cardinality(Server) < 5 THEN FALSE
  ELSE LET a == Named("n1")
           b == Named("n2")
           d == Named("n4")
           e == Named("n5")
       IN \/ /\ bugStep = 0
             /\ AdvanceCommit(a, 1)
          \/ /\ bugStep = 1
             /\ Timeout(d, 2)
          \/ /\ bugStep = 2
             /\ PreVote(d, e, 3)
          \/ /\ bugStep = 3
             /\ PreVote(d, b, 4)
          \/ /\ bugStep = 4
             /\ BeginElection(d, 5)
          \/ /\ bugStep = 5
             /\ RequestVote(d, e, 6)
          \/ /\ bugStep = 6
             /\ RequestVote(d, b, 7)
          \/ /\ bugStep = 7
             /\ BecomeLeader(d, 8)

\* A follower's vote set and ack set do not affect the next state.
\* BeginElection and BecomeLeader overwrite them.
View == <<currentTerm, votedFor, log, commitIndex, role,
          [s \in Server |-> IF role[s] = Candidate THEN votesGranted[s] ELSE {}],
          forcedIndex, preCand,
          [s \in Server |-> IF preCand[s] THEN preVotes[s] ELSE {}],
          restartGate, contact,
          [s \in Server |-> IF role[s] = Leader THEN acked[s] ELSE {}],
          readPrefix, bugStep>>

Next == IF CommitPriorTerms THEN BugNext ELSE FullNext

Spec == Init /\ [][Next]_vars
BugSpec == InitBug /\ [][BugNext]_vars

====
