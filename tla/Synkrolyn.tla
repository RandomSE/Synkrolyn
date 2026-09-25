---- MODULE Synkrolyn ----
\* Bounded TLA+ model of Synkrolyn election and log replication.
\* Static 3-server cluster. Not a line-level model of the C# code and not a proof.
\* Not modeled: membership, snapshots, sockets, FileRaftLog bytes, ReadIndex,
\* leadership-transfer completion, KV commands.
\* AppendEntries rejection may set nextIndex from XLen / XTerm / XIndex.
\* CanCommit counts the leader only when forcedIndex has reached n.
\* A successful AppendEntries sets the follower forcedIndex to the new length,
\* because the implementation acks only after fsync. The leader's own append
\* stays volatile until Force.

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Server, Nil, MaxTerm, MaxLogLen, Majority

ASSUME /\ Server # {}
       /\ Nil \notin Server
       /\ MaxTerm \in Nat \ {0}
       /\ MaxLogLen \in Nat \ {0}
       /\ Majority \in Nat \ {0}

Follower == "Follower"
Candidate == "Candidate"
Leader == "Leader"
Roles == {Follower, Candidate, Leader}

VARIABLES currentTerm, votedFor, log, commitIndex, role,
          votesGranted, nextIndex, matchIndex, forcedIndex

vars == <<currentTerm, votedFor, log, commitIndex, role,
          votesGranted, nextIndex, matchIndex, forcedIndex>>

MinNat(a, b) == IF a <= b THEN a ELSE b
MaxNat(a, b) == IF a >= b THEN a ELSE b

LastIndex(s) == Len(log[s])
LastTerm(s) == IF log[s] = << >> THEN 0 ELSE log[s][Len(log[s])]

LogUpToDate(cand, voter) ==
  \/ LastTerm(cand) > LastTerm(voter)
  \/ /\ LastTerm(cand) = LastTerm(voter)
     /\ LastIndex(cand) >= LastIndex(voter)

\* Paper 5.4.2. The leader counts itself only after Force has covered n.
CanCommit(s, n) ==
  /\ role[s] = Leader
  /\ n \in 1..Len(log[s])
  /\ log[s][n] = currentTerm[s]
  /\ Cardinality({t \in Server :
                    /\ Len(log[t]) >= n
                    /\ SubSeq(log[t], 1, n) = SubSeq(log[s], 1, n)
                    /\ (t # s \/ forcedIndex[s] >= n)}) >= Majority

TypeOK ==
  /\ currentTerm \in [Server -> 0..MaxTerm]
  /\ votedFor \in [Server -> Server \union {Nil}]
  /\ log \in [Server -> Seq(1..MaxTerm)]
  /\ \A s \in Server: Len(log[s]) <= MaxLogLen
  /\ commitIndex \in [Server -> 0..MaxLogLen]
  /\ \A s \in Server: commitIndex[s] <= Len(log[s])
  /\ role \in [Server -> Roles]
  /\ votesGranted \in [Server -> SUBSET Server]
  /\ nextIndex \in [Server -> [Server -> 1..(MaxLogLen + 1)]]
  /\ matchIndex \in [Server -> [Server -> 0..MaxLogLen]]
  /\ forcedIndex \in [Server -> 0..MaxLogLen]
  /\ \A s \in Server: forcedIndex[s] <= Len(log[s])

ElectionSafety ==
  \A t \in 0..MaxTerm:
    Cardinality({s \in Server : role[s] = Leader /\ currentTerm[s] = t}) <= 1

LogMatching ==
  \A s, t \in Server:
    \A i \in 1..MinNat(Len(log[s]), Len(log[t])):
      log[s][i] = log[t][i] => SubSeq(log[s], 1, i) = SubSeq(log[t], 1, i)

CommittedPrefixSafety ==
  \A s, t \in Server:
    \A i \in 1..MinNat(commitIndex[s], commitIndex[t]):
      /\ i <= Len(log[s])
      /\ i <= Len(log[t])
      /\ log[s][i] = log[t][i]

LeaderAppendOnly ==
  \A s \in Server:
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
  /\ nextIndex = [s \in Server |-> [t \in Server |-> 1]]
  /\ matchIndex = [s \in Server |-> [t \in Server |-> 0]]
  /\ forcedIndex = [s \in Server |-> 0]

Timeout(s) ==
  /\ role[s] \in {Follower, Candidate}
  /\ currentTerm[s] < MaxTerm
  /\ currentTerm' = [currentTerm EXCEPT ![s] = @ + 1]
  /\ role' = [role EXCEPT ![s] = Candidate]
  /\ votedFor' = [votedFor EXCEPT ![s] = s]
  /\ votesGranted' = [votesGranted EXCEPT ![s] = {s}]
  /\ UNCHANGED <<log, commitIndex, nextIndex, matchIndex, forcedIndex>>

BecomeLeader(s) ==
  /\ role[s] = Candidate
  /\ Cardinality(votesGranted[s]) >= Majority
  /\ Len(log[s]) < MaxLogLen
  /\ LET newLog == Append(log[s], currentTerm[s])
     IN /\ role' = [role EXCEPT ![s] = Leader]
        /\ log' = [log EXCEPT ![s] = newLog]
        /\ nextIndex' = [nextIndex EXCEPT ![s] = [t \in Server |-> Len(newLog) + 1]]
        /\ matchIndex' = [matchIndex EXCEPT ![s] = [t \in Server |-> 0]]
        /\ UNCHANGED <<currentTerm, votedFor, commitIndex, votesGranted, forcedIndex>>

ClientAppend(s) ==
  /\ role[s] = Leader
  /\ currentTerm[s] \in 1..MaxTerm
  /\ Len(log[s]) < MaxLogLen
  /\ log' = [log EXCEPT ![s] = Append(@, currentTerm[s])]
  /\ UNCHANGED <<currentTerm, votedFor, commitIndex, role,
                 votesGranted, nextIndex, matchIndex, forcedIndex>>

\* Group-commit barrier. Does not append.
Force(s) ==
  /\ forcedIndex[s] < Len(log[s])
  /\ forcedIndex' = [forcedIndex EXCEPT ![s] = Len(log[s])]
  /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, role,
                 votesGranted, nextIndex, matchIndex>>

AdvanceCommit(s) ==
  /\ role[s] = Leader
  /\ LET ns == {n \in (commitIndex[s] + 1)..Len(log[s]) : CanCommit(s, n)}
     IN  /\ ns # {}
         /\ commitIndex' = [commitIndex EXCEPT ![s] = CHOOSE n \in ns :
                              \A m \in ns : m <= n]
  /\ UNCHANGED <<currentTerm, votedFor, log, role,
                 votesGranted, nextIndex, matchIndex, forcedIndex>>

RequestVote(from, to) ==
  /\ from # to
  /\ role[from] = Candidate
  /\ \/ /\ currentTerm[from] < currentTerm[to]
        /\ currentTerm' = [currentTerm EXCEPT ![from] = currentTerm[to]]
        /\ role' = [role EXCEPT ![from] = Follower]
        /\ votedFor' = [votedFor EXCEPT ![from] = Nil]
        /\ votesGranted' = [votesGranted EXCEPT ![from] = {}]
        /\ UNCHANGED <<log, commitIndex, nextIndex, matchIndex, forcedIndex>>
     \/ /\ currentTerm[from] >= currentTerm[to]
        /\ LogUpToDate(from, to)
        /\ \/ currentTerm[from] > currentTerm[to]
           \/ votedFor[to] \in {Nil, from}
        /\ currentTerm' = [currentTerm EXCEPT ![to] = currentTerm[from]]
        /\ role' = [role EXCEPT ![to] =
                      IF currentTerm[from] > currentTerm[to]
                      THEN Follower
                      ELSE role[to]]
        /\ votedFor' = [votedFor EXCEPT ![to] = from]
        /\ votesGranted' = [votesGranted EXCEPT ![from] = @ \union {to}]
        /\ UNCHANGED <<log, commitIndex, nextIndex, matchIndex, forcedIndex>>
     \/ /\ currentTerm[from] >= currentTerm[to]
        /\ \/ ~LogUpToDate(from, to)
           \/ /\ currentTerm[from] = currentTerm[to]
              /\ votedFor[to] \notin {Nil, from}
        /\ currentTerm' = [currentTerm EXCEPT ![to] =
                             MaxNat(currentTerm[to], currentTerm[from])]
        /\ role' = [role EXCEPT ![to] =
                      IF currentTerm[from] > currentTerm[to]
                      THEN Follower
                      ELSE role[to]]
        /\ votedFor' = IF currentTerm[from] > currentTerm[to]
                       THEN [votedFor EXCEPT ![to] = Nil]
                       ELSE votedFor
        /\ UNCHANGED <<log, commitIndex, votesGranted, nextIndex, matchIndex, forcedIndex>>

FastBackupNext(s, t, prev) ==
  LET short == prev > Len(log[t])
      xLen == IF short THEN Len(log[t]) + 1 ELSE 0
      xTerm == IF short \/ prev < 1 \/ prev > Len(log[t]) THEN 0 ELSE log[t][prev]
      xIndex == IF xTerm = 0 THEN 0
                ELSE CHOOSE i \in 1..Len(log[t]) :
                       /\ log[t][i] = xTerm
                       /\ \A j \in 1..(i - 1) : log[t][j] # xTerm
      leaderIdx == {i \in 1..Len(log[s]) : log[s][i] = xTerm}
      hasTerm == leaderIdx # {}
      lastWith == IF hasTerm
                  THEN CHOOSE i \in leaderIdx : \A j \in leaderIdx : j <= i
                  ELSE 0
      current == nextIndex[s][t]
      raw == IF xTerm = 0
             THEN IF xLen = 0 THEN current - 1 ELSE xLen
             ELSE IF hasTerm THEN MinNat(lastWith + 1, current - 1)
                  ELSE IF xIndex = 0 THEN current - 1 ELSE xIndex
      stepped == IF raw < 1 THEN 1 ELSE raw
      capped == IF stepped >= current THEN MaxNat(1, current - 1) ELSE stepped
  IN MinNat(MaxLogLen + 1, MaxNat(1, capped))

AppendEntries(s, t) ==
  /\ s # t
  /\ role[s] = Leader
  /\ \/ /\ currentTerm[s] < currentTerm[t]
        /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[t]]
        /\ role' = [role EXCEPT ![s] = Follower]
        /\ votedFor' = [votedFor EXCEPT ![s] = Nil]
        /\ UNCHANGED <<log, commitIndex, votesGranted, nextIndex, matchIndex, forcedIndex>>
     \/ /\ currentTerm[s] >= currentTerm[t]
        /\ LET prev == nextIndex[s][t] - 1
               prevOk ==
                 prev = 0
                 \/ (prev <= Len(log[s])
                     /\ Len(log[t]) >= prev
                     /\ log[t][prev] = log[s][prev])
           IN IF ~prevOk
              THEN /\ nextIndex' = [nextIndex EXCEPT ![s] =
                                      [nextIndex[s] EXCEPT ![t] =
                                         FastBackupNext(s, t, prev)]]
                   /\ currentTerm' = [currentTerm EXCEPT ![t] = currentTerm[s]]
                   /\ role' = [role EXCEPT ![t] = Follower]
                   /\ votedFor' = IF currentTerm[t] < currentTerm[s]
                                  THEN [votedFor EXCEPT ![t] = Nil]
                                  ELSE votedFor
                   /\ UNCHANGED <<log, commitIndex, votesGranted, matchIndex, forcedIndex>>
              ELSE LET entries == SubSeq(log[s], prev + 1, Len(log[s]))
                       newLog == IF entries = << >>
                                 THEN log[t]
                                 ELSE SubSeq(log[t], 1, prev) \o entries
                       matchIdx == IF entries = << >> THEN prev ELSE Len(log[s])
                       newCommit == MaxNat(commitIndex[t],
                                           MinNat(commitIndex[s], Len(newLog)))
                   IN /\ log' = [log EXCEPT ![t] = newLog]
                      /\ commitIndex' = [commitIndex EXCEPT ![t] = newCommit]
                      /\ matchIndex' = [matchIndex EXCEPT ![s] =
                                          [matchIndex[s] EXCEPT ![t] = matchIdx]]
                      /\ nextIndex' = [nextIndex EXCEPT ![s] =
                                         [nextIndex[s] EXCEPT ![t] = matchIdx + 1]]
                      /\ forcedIndex' = [forcedIndex EXCEPT ![t] = Len(newLog)]
                      /\ currentTerm' = [currentTerm EXCEPT ![t] = currentTerm[s]]
                      /\ role' = [role EXCEPT ![t] = Follower]
                      /\ votedFor' = IF currentTerm[t] < currentTerm[s]
                                     THEN [votedFor EXCEPT ![t] = Nil]
                                     ELSE votedFor
                      /\ UNCHANGED votesGranted

Next ==
  \/ \E s \in Server: Timeout(s)
  \/ \E s \in Server: BecomeLeader(s)
  \/ \E s \in Server: ClientAppend(s)
  \/ \E s \in Server: Force(s)
  \/ \E s \in Server: AdvanceCommit(s)
  \/ \E f, t \in Server: RequestVote(f, t)
  \/ \E s, t \in Server: AppendEntries(s, t)

Spec == Init /\ [][Next]_vars

====
