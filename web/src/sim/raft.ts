export type Role = "follower" | "candidate" | "leader";

export type FeatureFlags = {
  preVote: boolean;
  checkQuorum: boolean;
  leases: boolean;
  readIndex: boolean;
  synkrolynImprovements: boolean;
};

export const defaultFlags: FeatureFlags = {
  preVote: false,
  checkQuorum: false,
  leases: false,
  readIndex: false,
  synkrolynImprovements: false,
};

export type LogEntry = {
  term: number;
  command: string;
};

export type MessageKind = "requestVote" | "voteReply" | "appendEntries" | "appendReply";

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
};

export type CreateOptions = {
  nodeCount?: number;
  seed?: number;
  flags?: Partial<FeatureFlags>;
};

const HEARTBEAT_TICKS = 3;

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
    });
  }
  return sim;
}

export function tick(sim: Sim): Sim {
  noteFeatureFlags(sim);
  sim.tickCount += 1;
  deliver(sim);
  for (const node of sim.nodes) {
    if (node.crashed) continue;
    if (node.role === "leader") {
      flushQueue(sim, node);
      node.heartbeatElapsed += 1;
      const heartbeatDue = node.heartbeatElapsed >= HEARTBEAT_TICKS;
      if (heartbeatDue) node.heartbeatElapsed = 0;
      replicate(sim, node, heartbeatDue);
    } else {
      node.electionElapsed += 1;
      if (node.electionElapsed >= node.electionTimeout) beginElection(sim, node);
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

export function propose(sim: Sim, command: string, preferredLeader?: number): void {
  const target = preferredLeader ?? leaderId(sim);
  const node = target === null ? undefined : sim.nodes[target];
  if (!node || node.crashed || node.role !== "leader") {
    sim.queue.push({ command, leaderId: target });
    return;
  }
  appendLocal(node, command);
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
  // Later chapters turn these on. Base Raft does not read them.
  void sim.flags.preVote;
  void sim.flags.checkQuorum;
  void sim.flags.leases;
  void sim.flags.readIndex;
  void sim.flags.synkrolynImprovements;
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
  return node.log.length;
}

function lastTerm(node: NodeState): number {
  if (node.log.length === 0) return 0;
  return node.log[node.log.length - 1]!.term;
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

function appendLocal(node: NodeState, command: string): void {
  node.log.push({ term: node.term, command });
  if (node.matchIndex.length === 0) {
    node.matchIndex = node.nextIndex.map(() => 0);
  }
  node.matchIndex[node.id] = node.log.length;
}

function flushQueue(sim: Sim, leader: NodeState): void {
  const remaining: PendingWrite[] = [];
  for (const item of sim.queue) {
    if (item.leaderId === null || item.leaderId === leader.id) {
      appendLocal(leader, item.command);
    } else {
      remaining.push(item);
    }
  }
  sim.queue = remaining;
}

function beginElection(sim: Sim, node: NodeState): void {
  node.role = "candidate";
  node.term += 1;
  node.votedFor = node.id;
  node.votes = [node.id];
  node.electionElapsed = 0;
  node.electionTimeout = rollTimeout(sim, node.id);
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
  flushQueue(sim, node);
  replicate(sim, node, true);
}

function stepDown(node: NodeState, term: number, sim: Sim): void {
  node.term = term;
  node.role = "follower";
  node.votedFor = null;
  node.votes = [];
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
    else if (message.kind === "appendEntries") onAppendEntries(sim, to, message);
    else onAppendReply(sim, to, message);
  }
}

function onRequestVote(sim: Sim, voter: NodeState, message: Message): void {
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
  follower.electionElapsed = 0;
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
      follower.log.push(entry);
    } else if (existing.term !== entry.term) {
      follower.log.length = index - 1;
      follower.log.push(entry);
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
      matchIndex: follower.log.length,
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
  advanceCommit(sim, leader);
}

function advanceCommit(sim: Sim, leader: NodeState): void {
  const needed = majority(sim);
  for (let index = leader.log.length; index > leader.commitIndex; index -= 1) {
    if (leader.log[index - 1]!.term !== leader.term) continue;
    let copies = 1;
    for (const peer of sim.nodes) {
      if (peer.id === leader.id) continue;
      if ((leader.matchIndex[peer.id] ?? 0) >= index) copies += 1;
    }
    if (copies >= needed) {
      leader.commitIndex = index;
      return;
    }
  }
}

function replicate(sim: Sim, leader: NodeState, heartbeatDue: boolean): void {
  for (const peer of sim.nodes) {
    if (peer.id === leader.id) continue;
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
    ...extra,
  };
}
