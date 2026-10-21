export type Role = "follower" | "candidate" | "leader";

export type FeatureFlags = {
  preVote: boolean;
  checkQuorum: boolean;
  leases: boolean;
  readIndex: boolean;
  synkrolynImprovements: boolean;
  durableAck: boolean;
  sessions: boolean;
  snapshots: boolean;
  voteGate: boolean;
  leaseGrace: boolean;
};

export const defaultFlags: FeatureFlags = {
  preVote: false,
  checkQuorum: false,
  leases: false,
  readIndex: false,
  synkrolynImprovements: false,
  durableAck: false,
  sessions: false,
  snapshots: false,
  voteGate: false,
  leaseGrace: false,
};

export type LogEntry = {
  term: number;
  command: string;
  storedAt?: number;
  client?: string;
  serial?: number;
};

export type MessageKind =
  | "requestVote"
  | "voteReply"
  | "preVote"
  | "preVoteReply"
  | "appendEntries"
  | "appendReply"
  | "installSnapshot";

export type ReadStatus = "pending" | "ok" | "refused";

export type ReadResult = {
  id: number;
  leaderId: number;
  status: ReadStatus;
  value: string | null;
  roundTrip: boolean;
  stale: boolean;
  issuedAt: number;
  term: number;
  acks: number[];
};

export type Message = {
  id: number;
  from: number;
  to: number;
  kind: MessageKind;
  term: number;
  travel: number;
  totalTravel: number;
  lastLogIndex: number;
  lastLogTerm: number;
  granted: boolean;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
  success: boolean;
  matchIndex: number;
  sentAt: number;
};

export type NodeState = {
  id: number;
  role: Role;
  term: number;
  votedFor: number | null;
  log: LogEntry[];
  commitIndex: number;
  crashed: boolean;
  electionTimeout: number;
  electionElapsed: number;
  heartbeatElapsed: number;
  votes: number[];
  nextIndex: number[];
  matchIndex: number[];
  preVoting: boolean;
  preVotes: number[];
  peerContact: number[];
  quorumUntil: number;
  leaseUntil: number;
  durableIndex: number;
  snapshotIndex: number;
  snapshotTerm: number;
  voteGateUntil: number;
};

export type PendingWrite = {
  command: string;
  leaderId: number | null;
};

export type Sim = {
  seed: number;
  rng: number;
  tickCount: number;
  nodes: NodeState[];
  messages: Message[];
  nextMessageId: number;
  partitions: number[][] | null;
  flags: FeatureFlags;
  queue: PendingWrite[];
  travelTicks: number;
  elections: number;
  reads: ReadResult[];
  nextReadId: number;
  banner: string;
  tally: number;
  appliedCommit: number;
  appliedSerials: Record<string, number>;
  ackedCommand: string | null;
  mark: number;
};

export type CreateOptions = {
  nodeCount?: number;
  seed?: number;
  flags?: Partial<FeatureFlags>;
};

const HEARTBEAT_TICKS = 3;
const LEADER_FRESH_TICKS = 6;
const QUORUM_WINDOW = 10;
const LEASE_TICKS = 8;
const READ_TIMEOUT = 16;
const DISK_LAG = 6;
const CLOCK_DRIFT = 0.01;

export function cloneSim(sim: Sim): Sim {
  return {
    seed: sim.seed,
    rng: sim.rng,
    tickCount: sim.tickCount,
    nodes: sim.nodes.map((node) => ({
      ...node,
      log: node.log.map((entry) => ({ ...entry })),
      votes: node.votes.slice(),
      nextIndex: node.nextIndex.slice(),
      matchIndex: node.matchIndex.slice(),
      preVotes: node.preVotes.slice(),
      peerContact: node.peerContact.slice(),
    })),
    messages: sim.messages.map((message) => ({
      ...message,
      entries: message.entries.map((entry) => ({ ...entry })),
    })),
    nextMessageId: sim.nextMessageId,
    partitions: sim.partitions ? sim.partitions.map((group) => group.slice()) : null,
    flags: { ...sim.flags },
    queue: sim.queue.map((item) => ({ ...item })),
    travelTicks: sim.travelTicks,
    elections: sim.elections,
    reads: sim.reads.map((read) => ({ ...read, acks: read.acks.slice() })),
    nextReadId: sim.nextReadId,
    banner: sim.banner,
    tally: sim.tally,
    appliedCommit: sim.appliedCommit,
    appliedSerials: { ...sim.appliedSerials },
    ackedCommand: sim.ackedCommand,
    mark: sim.mark,
  };
}

export function createSim(options: CreateOptions = {}): Sim {
  const nodeCount = options.nodeCount ?? 5;
  const seed = options.seed ?? 1;
  const sim: Sim = {
    seed,
    rng: seed >>> 0,
    tickCount: 0,
    nodes: [],
    messages: [],
    nextMessageId: 1,
    partitions: null,
    flags: { ...defaultFlags, ...options.flags },
    queue: [],
    travelTicks: 2,
    elections: 0,
    reads: [],
    nextReadId: 1,
    banner: "",
    tally: 0,
    appliedCommit: 0,
    appliedSerials: {},
    ackedCommand: null,
    mark: 0,
  };
  for (let id = 0; id < nodeCount; id += 1) {
    sim.nodes.push({
      id,
      role: "follower",
      term: 0,
      votedFor: null,
      log: [],
      commitIndex: 0,
      crashed: false,
      electionTimeout: rollTimeout(sim, id),
      electionElapsed: 0,
      heartbeatElapsed: 0,
      votes: [],
      nextIndex: [],
      matchIndex: [],
      preVoting: false,
      preVotes: [],
      peerContact: [],
      quorumUntil: 0,
      leaseUntil: 0,
      durableIndex: 0,
      snapshotIndex: 0,
      snapshotTerm: 0,
      voteGateUntil: 0,
    });
  }
  return sim;
}

export function tick(sim: Sim): Sim {
  noteFeatureFlags(sim);
  sim.tickCount += 1;
  rememberDisk(sim);
  deliver(sim);
  settleReads(sim);
  applyTally(sim);
  for (const node of sim.nodes) {
    if (node.crashed) continue;
    if (node.role === "leader" && sim.flags.checkQuorum && sim.tickCount > node.quorumUntil) {
      stepDown(node, node.term, sim);
    }
    if (node.role === "leader") {
      flushQueue(sim, node);
      node.heartbeatElapsed += 1;
      const heartbeatDue = node.heartbeatElapsed >= HEARTBEAT_TICKS;
      if (heartbeatDue) node.heartbeatElapsed = 0;
      replicate(sim, node, heartbeatDue);
    } else {
      node.electionElapsed += 1;
      if (node.electionElapsed >= node.electionTimeout) {
        if (sim.flags.preVote) beginPreVote(sim, node);
        else beginElection(sim, node);
      }
    }
  }
  return sim;
}

export function runUntil(sim: Sim, predicate: (sim: Sim) => boolean, maxTicks: number): boolean {
  for (let i = 0; i < maxTicks; i += 1) {
    if (predicate(sim)) return true;
    tick(sim);
  }
  return predicate(sim);
}

export function leaderId(sim: Sim): number | null {
  const leaders = sim.nodes.filter((node) => node.role === "leader" && !node.crashed);
  if (leaders.length === 0) return null;
  leaders.sort((a, b) => b.term - a.term || a.id - b.id);
  return leaders[0]!.id;
}

export function acknowledged(sim: Sim): string | null {
  return sim.ackedCommand;
}

export function powerLoss(sim: Sim, id: number): void {
  const node = sim.nodes[id];
  if (!node) return;
  rememberNode(sim, node);
  node.log = node.log.filter((entry) => sim.tickCount - (entry.storedAt ?? 0) >= DISK_LAG);
  node.commitIndex = Math.min(node.commitIndex, node.durableIndex);
  node.role = "follower";
  node.votes = [];
  node.crashed = false;
  sim.messages = sim.messages.filter((message) => message.from !== id && message.to !== id);
}

export function submit(sim: Sim, client: string, serial: number, command: string, preferredLeader?: number): void {
  const target = preferredLeader ?? leaderId(sim);
  const node = target === null ? undefined : sim.nodes[target];
  if (!node || node.crashed || node.role !== "leader") return;
  if (sim.flags.sessions) {
    const seen = sim.appliedSerials[client] ?? 0;
    if (serial <= seen) return;
    if (node.log.some((entry) => entry.client === client && entry.serial === serial)) return;
  }
  appendLocal(sim, node, command, { client, serial });
}

export function compactPrefix(sim: Sim, leaderId: number): void {
  const leader = sim.nodes[leaderId];
  if (!leader) return;
  const index = leader.commitIndex;
  const term = logEntry(leader, index)?.term ?? leader.term;
  leader.log = [];
  if (sim.flags.snapshots) {
    leader.snapshotIndex = index;
    leader.snapshotTerm = term;
  }
  leader.matchIndex = sim.nodes.map(() => 0);
  leader.matchIndex[leader.id] = sim.flags.snapshots ? index : 0;
  leader.nextIndex = sim.nodes.map(() => (sim.flags.snapshots ? index + 1 : 1));
}

export function campaign(sim: Sim, id: number): void {
  const node = sim.nodes[id];
  if (!node || node.crashed) return;
  const term = node.term + 1;
  for (const peer of sim.nodes) {
    if (peer.id === id || peer.role === "leader") continue;
    send(sim, emptyMessage(id, peer.id, "requestVote", term, {
      lastLogIndex: lastIndex(node),
      lastLogTerm: lastTerm(node),
    }));
  }
}

export function propose(sim: Sim, command: string, preferredLeader?: number): void {
  const target = preferredLeader ?? leaderId(sim);
  const node = target === null ? undefined : sim.nodes[target];
  if (!node || node.crashed || node.role !== "leader") {
    sim.queue.push({ command, leaderId: target });
    return;
  }
  appendLocal(sim, node, command);
}

export function crash(sim: Sim, id: number): void {
  const node = sim.nodes[id];
  if (!node) return;
  node.crashed = true;
  node.role = "follower";
  node.votes = [];
  sim.messages = sim.messages.filter((message) => message.from !== id && message.to !== id);
}

export function restart(sim: Sim, id: number): void {
  const node = sim.nodes[id];
  if (!node) return;
  node.crashed = false;
  node.role = "follower";
  node.votes = [];
  node.electionElapsed = 0;
  node.electionTimeout = rollTimeout(sim, id);
  sim.messages = sim.messages.filter((message) => message.from !== id && message.to !== id);
}

export function setPartitions(sim: Sim, groups: number[][] | null): void {
  sim.partitions = groups;
  sim.messages = sim.messages.filter((message) => reachable(sim, message.from, message.to));
}

function noteFeatureFlags(sim: Sim): void {
  // The Synkrolyn chapter turns this on later. Extended Raft does not read it.
  void sim.flags.synkrolynImprovements;
}

export function requestRead(sim: Sim, nodeId: number): ReadResult {
  const node = sim.nodes[nodeId];
  const read: ReadResult = {
    id: sim.nextReadId,
    leaderId: nodeId,
    status: "refused",
    value: null,
    roundTrip: false,
    stale: false,
    issuedAt: sim.tickCount,
    term: node?.term ?? 0,
    acks: [],
  };
  sim.nextReadId += 1;
  if (!node || node.crashed || node.role !== "leader") {
    sim.reads.push(read);
    return read;
  }
  const value = committedValue(node);
  const leaseOpen = sim.flags.leases && node.leaseUntil > 0 && sim.tickCount <= node.leaseUntil;
  if (leaseOpen) {
    read.status = "ok";
    read.value = value;
    read.stale = markStale(sim, value);
    sim.reads.push(read);
    return read;
  }
  if (sim.flags.leases && !sim.flags.readIndex) {
    sim.reads.push(read);
    return read;
  }
  if (sim.flags.readIndex) {
    read.status = "pending";
    read.roundTrip = true;
    sim.reads.push(read);
    return read;
  }
  read.status = "ok";
  read.value = value;
  read.stale = markStale(sim, value);
  sim.reads.push(read);
  return read;
}

function committedValue(node: NodeState): string | null {
  if (node.commitIndex <= 0) return null;
  return node.log[node.commitIndex - 1]?.command ?? null;
}

function newestCommitted(sim: Sim): string | null {
  let bestIndex = 0;
  let best: string | null = null;
  for (const node of sim.nodes) {
    if (node.commitIndex > bestIndex) {
      bestIndex = node.commitIndex;
      best = node.log[node.commitIndex - 1]?.command ?? null;
    }
  }
  return best;
}

function markStale(sim: Sim, value: string | null): boolean {
  const newest = newestCommitted(sim);
  return newest !== null && value !== newest;
}

function settleReads(sim: Sim): void {
  for (const read of sim.reads) {
    if (read.status !== "pending") continue;
    const leader = sim.nodes[read.leaderId];
    if (!leader || leader.crashed || leader.role !== "leader" || leader.term !== read.term) {
      read.status = "refused";
      continue;
    }
    if (sim.tickCount - read.issuedAt >= READ_TIMEOUT) read.status = "refused";
  }
}

function noteReadAck(sim: Sim, leader: NodeState, from: number, sentAt: number): void {
  for (const read of sim.reads) {
    if (read.status !== "pending" || read.leaderId !== leader.id || read.term !== leader.term) continue;
    if (sentAt <= read.issuedAt) continue;
    if (!read.acks.includes(from)) read.acks.push(from);
    if (read.acks.length + 1 >= majority(sim)) {
      read.status = "ok";
      read.value = committedValue(leader);
      read.stale = markStale(sim, read.value);
      read.roundTrip = true;
    }
  }
}

function nextUnit(sim: Sim): number {
  let state = sim.rng | 0;
  state = (state + 0x6d2b79f5) | 0;
  let mixed = Math.imul(state ^ (state >>> 15), 1 | state);
  mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed;
  sim.rng = state >>> 0;
  return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296;
}

function rollTimeout(sim: Sim, id: number): number {
  const jitter = Math.floor(nextUnit(sim) * 3);
  return 8 + id * 5 + jitter;
}

function majority(sim: Sim): number {
  return Math.floor(sim.nodes.length / 2) + 1;
}

function lastIndex(node: NodeState): number {
  return node.snapshotIndex + node.log.length;
}

function lastTerm(node: NodeState): number {
  if (node.log.length === 0) return node.snapshotTerm;
  return node.log[node.log.length - 1]!.term;
}

function logEntry(node: NodeState, index: number): LogEntry | undefined {
  if (index <= node.snapshotIndex || index <= 0) return undefined;
  return node.log[index - node.snapshotIndex - 1];
}

function leaseSpan(sim: Sim): number {
  if (!sim.flags.leaseGrace) return LEASE_TICKS;
  return Math.floor(LEASE_TICKS * (1 - CLOCK_DRIFT));
}

function rememberNode(sim: Sim, node: NodeState): void {
  let kept = 0;
  for (const entry of node.log) {
    if (sim.tickCount - (entry.storedAt ?? 0) >= DISK_LAG) kept += 1;
  }
  node.durableIndex = node.snapshotIndex + kept;
}

function rememberDisk(sim: Sim): void {
  for (const node of sim.nodes) {
    if (node.crashed) continue;
    rememberNode(sim, node);
    if (node.role === "leader" && sim.flags.durableAck) {
      node.matchIndex[node.id] = node.durableIndex;
    }
  }
}

function applyTally(sim: Sim): void {
  const leader = sim.nodes.find((node) => node.role === "leader" && !node.crashed);
  if (!leader) return;
  while (sim.appliedCommit < leader.commitIndex) {
    const entry = logEntry(leader, sim.appliedCommit + 1);
    if (!entry) break;
    sim.appliedCommit += 1;
    if (entry.command !== "add") continue;
    if (sim.flags.sessions && entry.client !== undefined && entry.serial !== undefined) {
      const seen = sim.appliedSerials[entry.client] ?? 0;
      if (entry.serial <= seen) continue;
      sim.appliedSerials[entry.client] = entry.serial;
    }
    sim.tally += 1;
  }
}

function onInstallSnapshot(sim: Sim, follower: NodeState, message: Message): void {
  if (message.term > follower.term) stepDown(follower, message.term, sim);
  if (message.term < follower.term) {
    send(sim, emptyMessage(follower.id, message.from, "appendReply", follower.term, { success: false }));
    return;
  }
  follower.snapshotIndex = message.lastLogIndex;
  follower.snapshotTerm = message.lastLogTerm;
  follower.log = [];
  follower.commitIndex = Math.max(follower.commitIndex, message.lastLogIndex);
  follower.durableIndex = follower.snapshotIndex;
  follower.role = "follower";
  follower.electionElapsed = 0;
  send(sim, emptyMessage(follower.id, message.from, "appendReply", follower.term, {
    success: true,
    matchIndex: follower.snapshotIndex,
    sentAt: message.sentAt,
  }));
}

function logUpToDate(lastLogIndex: number, lastLogTerm: number, voter: NodeState): boolean {
  const voterTerm = lastTerm(voter);
  if (lastLogTerm !== voterTerm) return lastLogTerm > voterTerm;
  return lastLogIndex >= lastIndex(voter);
}

function reachable(sim: Sim, from: number, to: number): boolean {
  if (!sim.partitions) return true;
  return sim.partitions.some((group) => group.includes(from) && group.includes(to));
}

function send(sim: Sim, message: Omit<Message, "id" | "travel" | "totalTravel">): void {
  const from = sim.nodes[message.from];
  const to = sim.nodes[message.to];
  if (!from || !to || from.crashed || to.crashed) return;
  if (!reachable(sim, message.from, message.to)) return;
  sim.messages.push({
    ...message,
    id: sim.nextMessageId,
    travel: sim.travelTicks,
    totalTravel: sim.travelTicks,
  });
  sim.nextMessageId += 1;
}

function appendLocal(sim: Sim, node: NodeState, command: string, extra: Partial<LogEntry> = {}): void {
  node.log.push({ term: node.term, command, storedAt: sim.tickCount, ...extra });
  if (node.matchIndex.length === 0) {
    node.matchIndex = node.nextIndex.map(() => 0);
  }
  node.matchIndex[node.id] = sim.flags.durableAck ? node.durableIndex : lastIndex(node);
}

function flushQueue(sim: Sim, leader: NodeState): void {
  const remaining: PendingWrite[] = [];
  for (const item of sim.queue) {
    if (item.leaderId === null || item.leaderId === leader.id) {
      appendLocal(sim, leader, item.command);
    } else {
      remaining.push(item);
    }
  }
  sim.queue = remaining;
}

function beginPreVote(sim: Sim, node: NodeState): void {
  node.preVoting = true;
  node.preVotes = [node.id];
  node.role = "follower";
  node.electionElapsed = 0;
  node.electionTimeout = rollTimeout(sim, node.id);
  const prospective = node.term + 1;
  for (const peer of sim.nodes) {
    if (peer.id === node.id) continue;
    send(sim, emptyMessage(node.id, peer.id, "preVote", prospective, {
      lastLogIndex: lastIndex(node),
      lastLogTerm: lastTerm(node),
    }));
  }
}

function beginElection(sim: Sim, node: NodeState): void {
  node.preVoting = false;
  node.role = "candidate";
  node.term += 1;
  node.votedFor = node.id;
  node.votes = [node.id];
  node.electionElapsed = 0;
  node.electionTimeout = rollTimeout(sim, node.id);
  sim.elections += 1;
  for (const peer of sim.nodes) {
    if (peer.id === node.id) continue;
    send(sim, emptyMessage(node.id, peer.id, "requestVote", node.term, {
      lastLogIndex: lastIndex(node),
      lastLogTerm: lastTerm(node),
    }));
  }
  if (node.votes.length >= majority(sim)) becomeLeader(sim, node);
}

function becomeLeader(sim: Sim, node: NodeState): void {
  node.role = "leader";
  node.votes = [];
  node.nextIndex = sim.nodes.map(() => lastIndex(node) + 1);
  node.matchIndex = sim.nodes.map(() => 0);
  node.matchIndex[node.id] = lastIndex(node);
  node.heartbeatElapsed = 0;
  node.peerContact = sim.nodes.map(() => 0);
  node.quorumUntil = sim.tickCount + QUORUM_WINDOW;
  node.leaseUntil = 0;
  flushQueue(sim, node);
  replicate(sim, node, true);
}

function noteQuorum(sim: Sim, leader: NodeState): void {
  let fresh = 1;
  for (const peer of sim.nodes) {
    if (peer.id === leader.id) continue;
    const seen = leader.peerContact[peer.id] ?? 0;
    if (seen > 0 && sim.tickCount - seen <= QUORUM_WINDOW) fresh += 1;
  }
  if (fresh >= majority(sim)) {
    leader.quorumUntil = sim.tickCount + QUORUM_WINDOW;
    if (sim.flags.leases) leader.leaseUntil = sim.tickCount + leaseSpan(sim);
  }
}

function stepDown(node: NodeState, term: number, sim: Sim): void {
  node.term = term;
  node.role = "follower";
  node.votedFor = null;
  node.votes = [];
  node.preVoting = false;
  node.preVotes = [];
  node.leaseUntil = 0;
  node.quorumUntil = 0;
  node.electionElapsed = 0;
  node.electionTimeout = rollTimeout(sim, node.id);
}

function deliver(sim: Sim): void {
  const due: Message[] = [];
  const pending: Message[] = [];
  for (const message of sim.messages) {
    message.travel -= 1;
    if (message.travel <= 0) due.push(message);
    else pending.push(message);
  }
  sim.messages = pending;
  for (const message of due) {
    const to = sim.nodes[message.to];
    const from = sim.nodes[message.from];
    if (!to || !from || to.crashed || from.crashed) continue;
    if (!reachable(sim, message.from, message.to)) continue;
    if (message.kind === "requestVote") onRequestVote(sim, to, message);
    else if (message.kind === "voteReply") onVoteReply(sim, to, message);
    else if (message.kind === "preVote") onPreVote(sim, to, message);
    else if (message.kind === "preVoteReply") onPreVoteReply(sim, to, message);
    else if (message.kind === "appendEntries") onAppendEntries(sim, to, message);
    else if (message.kind === "installSnapshot") onInstallSnapshot(sim, to, message);
    else onAppendReply(sim, to, message);
  }
}

function voteGated(sim: Sim, voter: NodeState): boolean {
  return sim.flags.voteGate && sim.tickCount < voter.voteGateUntil;
}

function onPreVote(sim: Sim, voter: NodeState, message: Message): void {
  if (voteGated(sim, voter)) {
    send(sim, emptyMessage(voter.id, message.from, "preVoteReply", voter.term, { granted: false }));
    return;
  }
  const heardLeaderRecently = voter.role === "leader" || voter.electionElapsed < LEADER_FRESH_TICKS;
  const granted =
    message.term > voter.term &&
    !heardLeaderRecently &&
    logUpToDate(message.lastLogIndex, message.lastLogTerm, voter);
  send(sim, emptyMessage(voter.id, message.from, "preVoteReply", voter.term, { granted }));
}

function onPreVoteReply(sim: Sim, candidate: NodeState, message: Message): void {
  if (message.term > candidate.term) {
    candidate.preVoting = false;
    stepDown(candidate, message.term, sim);
    return;
  }
  if (!candidate.preVoting || !message.granted) return;
  if (!candidate.preVotes.includes(message.from)) candidate.preVotes.push(message.from);
  if (candidate.preVotes.length >= majority(sim)) {
    candidate.preVoting = false;
    beginElection(sim, candidate);
  }
}

function onRequestVote(sim: Sim, voter: NodeState, message: Message): void {
  if (voteGated(sim, voter)) {
    send(sim, emptyMessage(voter.id, message.from, "voteReply", voter.term, { granted: false }));
    return;
  }
  if (message.term > voter.term) stepDown(voter, message.term, sim);
  let granted = false;
  if (
    message.term === voter.term &&
    (voter.votedFor === null || voter.votedFor === message.from) &&
    logUpToDate(message.lastLogIndex, message.lastLogTerm, voter)
  ) {
    granted = true;
    voter.votedFor = message.from;
    voter.role = "follower";
    voter.electionElapsed = 0;
  }
  send(sim, emptyMessage(voter.id, message.from, "voteReply", voter.term, { granted }));
}

function onVoteReply(sim: Sim, candidate: NodeState, message: Message): void {
  if (message.term > candidate.term) {
    stepDown(candidate, message.term, sim);
    return;
  }
  if (candidate.role !== "candidate" || message.term !== candidate.term || !message.granted) return;
  if (!candidate.votes.includes(message.from)) candidate.votes.push(message.from);
  if (candidate.votes.length >= majority(sim)) becomeLeader(sim, candidate);
}

function onAppendEntries(sim: Sim, follower: NodeState, message: Message): void {
  if (message.term > follower.term) stepDown(follower, message.term, sim);
  if (message.term < follower.term) {
    send(sim, emptyMessage(follower.id, message.from, "appendReply", follower.term, { success: false }));
    return;
  }
  follower.role = "follower";
  follower.votes = [];
  follower.preVoting = false;
  follower.electionElapsed = 0;
  if (sim.flags.voteGate) follower.voteGateUntil = sim.tickCount + follower.electionTimeout;
  if (message.prevLogIndex > 0) {
    const previous = follower.log[message.prevLogIndex - 1];
    if (!previous || previous.term !== message.prevLogTerm) {
      send(sim, emptyMessage(follower.id, message.from, "appendReply", follower.term, { success: false }));
      return;
    }
  }
  let index = message.prevLogIndex;
  for (const entry of message.entries) {
    index += 1;
    const existing = follower.log[index - 1];
    if (!existing) {
      follower.log.push({ ...entry, storedAt: sim.tickCount });
    } else if (existing.term !== entry.term) {
      follower.log.length = index - 1;
      follower.log.push({ ...entry, storedAt: sim.tickCount });
    }
  }
  if (follower.commitIndex > follower.log.length) follower.commitIndex = follower.log.length;
  if (message.leaderCommit > follower.commitIndex) {
    follower.commitIndex = Math.min(message.leaderCommit, follower.log.length);
  }
  send(
    sim,
    emptyMessage(follower.id, message.from, "appendReply", follower.term, {
      success: true,
      matchIndex: sim.flags.durableAck ? follower.durableIndex : lastIndex(follower),
      sentAt: message.sentAt,
    }),
  );
}

function onAppendReply(sim: Sim, leader: NodeState, message: Message): void {
  if (message.term > leader.term) {
    stepDown(leader, message.term, sim);
    return;
  }
  if (leader.role !== "leader" || message.term !== leader.term) return;
  if (!message.success) {
    const next = leader.nextIndex[message.from] ?? 1;
    leader.nextIndex[message.from] = Math.max(1, next - 1);
    return;
  }
  leader.matchIndex[message.from] = Math.max(leader.matchIndex[message.from] ?? 0, message.matchIndex);
  leader.nextIndex[message.from] = message.matchIndex + 1;
  leader.peerContact[message.from] = sim.tickCount;
  noteQuorum(sim, leader);
  noteReadAck(sim, leader, message.from, message.sentAt);
  advanceCommit(sim, leader);
}

function advanceCommit(sim: Sim, leader: NodeState): void {
  const needed = majority(sim);
  for (let index = lastIndex(leader); index > leader.commitIndex; index -= 1) {
    const entry = logEntry(leader, index);
    if (!entry || entry.term !== leader.term) continue;
    const selfStored = !sim.flags.durableAck || leader.durableIndex >= index;
    let copies = selfStored ? 1 : 0;
    for (const peer of sim.nodes) {
      if (peer.id === leader.id) continue;
      if ((leader.matchIndex[peer.id] ?? 0) >= index) copies += 1;
    }
    if (copies >= needed) {
      leader.commitIndex = index;
      const acked = logEntry(leader, index);
      if (acked) sim.ackedCommand = acked.command;
      return;
    }
  }
}

function replicate(sim: Sim, leader: NodeState, heartbeatDue: boolean): void {
  for (const peer of sim.nodes) {
    if (peer.id === leader.id) continue;
    if (leader.snapshotIndex > 0 && (leader.matchIndex[peer.id] ?? 0) < leader.snapshotIndex) {
      send(sim, emptyMessage(leader.id, peer.id, "installSnapshot", leader.term, {
        lastLogIndex: leader.snapshotIndex,
        lastLogTerm: leader.snapshotTerm,
        leaderCommit: leader.commitIndex,
        sentAt: sim.tickCount,
      }));
      continue;
    }
    const next = leader.nextIndex[peer.id] ?? lastIndex(leader) + 1;
    const behind = lastIndex(leader) >= next;
    if (!heartbeatDue && !behind) continue;
    const prevLogIndex = next - 1;
    const prevLogTerm = prevLogIndex === 0 ? 0 : (leader.log[prevLogIndex - 1]?.term ?? 0);
    const entries = behind ? leader.log.slice(prevLogIndex) : [];
    send(
      sim,
      emptyMessage(leader.id, peer.id, "appendEntries", leader.term, {
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit: leader.commitIndex,
        sentAt: sim.tickCount,
      }),
    );
  }
}

function emptyMessage(
  from: number,
  to: number,
  kind: MessageKind,
  term: number,
  extra: Partial<Message> = {},
): Omit<Message, "id" | "travel" | "totalTravel"> {
  return {
    from,
    to,
    kind,
    term,
    lastLogIndex: 0,
    lastLogTerm: 0,
    granted: false,
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [],
    leaderCommit: 0,
    success: false,
    matchIndex: 0,
    sentAt: 0,
    ...extra,
  };
}
