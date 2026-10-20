import {
  crash,
  createSim,
  leaderId,
  propose,
  runUntil,
  setPartitions,
  type Sim,
} from "../sim/raft";

export type ChapterLink = {
  id: "base" | "extended" | "synkrolyn";
  title: string;
  available: boolean;
};

export const chapterLinks: ChapterLink[] = [
  { id: "base", title: "Base Raft", available: true },
  { id: "extended", title: "Extended Raft", available: false },
  { id: "synkrolyn", title: "Synkrolyn", available: false },
];

export type GlossaryLine = {
  word: string;
  meaning: string;
};

export type ChapterStep = {
  id: string;
  title: string;
  kicker: string;
  body: string;
  glossary: GlossaryLine[];
  setup: () => Sim;
  script: { atTick: number; run: (sim: Sim) => void }[];
  pauseWhen: (sim: Sim) => boolean;
};

const NOTE = "hello";
const SIDE_NOTE = "side-note";

function fresh(): Sim {
  return createSim({ nodeCount: 5, seed: 1 });
}

function withLeader(): Sim {
  const sim = fresh();
  runUntil(sim, (state) => leaderId(state) !== null, 200);
  sim.messages = [];
  return sim;
}

function noteOnLeader(sim: Sim): { leader: number; index: number } | null {
  const leader = leaderId(sim);
  if (leader === null) return null;
  const index = sim.nodes[leader]!.log.findIndex((entry) => entry.command === NOTE);
  if (index < 0) return null;
  return { leader, index };
}

export const baseSteps: ChapterStep[] = [
  {
    id: "elect",
    title: "Someone steps up",
    kicker: "Choosing one leader",
    body: "Five servers start as equals. Each waits a different amount of time, so they do not all speak at once. The first to wake asks the others for support. When more than half say yes, that server becomes the leader: the only one allowed to take new writes.",
    glossary: [
      { word: "Leader", meaning: "The server that accepts new writes." },
      { word: "Follower", meaning: "A server that copies the leader." },
      { word: "Majority", meaning: "More than half of the servers." },
    ],
    setup: fresh,
    script: [],
    pauseWhen: (sim) => leaderId(sim) !== null,
  },
  {
    id: "replicate",
    title: "A note takes a trip",
    kicker: "Copying a write",
    body: "You hand the leader a short note. It writes the note in its own list, then sends a copy toward the others. Pale squares are still a draft. A draft can still be replaced.",
    glossary: [
      { word: "Log", meaning: "The ordered list of notes on a server." },
      { word: "Uncommitted", meaning: "A copy that is not permanent yet." },
    ],
    setup: withLeader,
    script: [
      {
        atTick: 1,
        run: (sim) => {
          const leader = leaderId(sim);
          if (leader !== null) propose(sim, NOTE, leader);
        },
      },
    ],
    pauseWhen: (sim) => {
      const found = noteOnLeader(sim);
      if (!found) return false;
      const copied = sim.nodes.some(
        (node) => node.id !== found.leader && node.log[found.index]?.command === NOTE,
      );
      return copied && sim.nodes[found.leader]!.commitIndex < found.index + 1;
    },
  },
  {
    id: "commit",
    title: "Most of the group agrees",
    kicker: "Making a write permanent",
    body: "The note becomes permanent once a majority of servers hold it. Watch the squares turn solid. One slow server can still be catching up. The group does not wait for every machine.",
    glossary: [
      { word: "Committed", meaning: "Safe to treat as permanent, because a majority stored it." },
    ],
    setup: () => {
      const sim = withLeader();
      const leader = leaderId(sim);
      if (leader !== null) propose(sim, NOTE, leader);
      runUntil(sim, (state) => {
        const found = noteOnLeader(state);
        if (!found) return false;
        const copied = state.nodes.some(
          (node) => node.id !== found.leader && node.log[found.index]?.command === NOTE,
        );
        return copied && state.nodes[found.leader]!.commitIndex < found.index + 1;
      }, 80);
      return sim;
    },
    script: [],
    pauseWhen: (sim) => sim.nodes.every((node) => node.commitIndex >= 1 && node.log[0]?.command === NOTE),
  },
  {
    id: "crash",
    title: "The leader goes quiet",
    kicker: "Picking a new leader",
    body: "The leader stops. The others notice the silence, then pick someone new. The solid note is still there. A permanent write is not thrown away just because the leader left. You can click any circle yourself to stop or start that server.",
    glossary: [
      { word: "Crash", meaning: "A server stops answering. Its saved notes stay on disk." },
    ],
    setup: () => {
      const sim = withLeader();
      const leader = leaderId(sim);
      if (leader !== null) propose(sim, NOTE, leader);
      runUntil(
        sim,
        (state) => state.nodes.every((node) => node.commitIndex >= 1 && node.log[0]?.command === NOTE),
        200,
      );
      sim.messages = [];
      return sim;
    },
    script: [
      {
        atTick: 6,
        run: (sim) => {
          const leader = leaderId(sim);
          if (leader !== null) crash(sim, leader);
        },
      },
    ],
    pauseWhen: (sim) => {
      if (!sim.nodes.some((node) => node.crashed)) return false;
      const leader = leaderId(sim);
      if (leader === null) return false;
      const node = sim.nodes[leader]!;
      return node.commitIndex >= 1 && node.log[0]?.command === NOTE;
    },
  },
  {
    id: "partition",
    title: "A cut in the network",
    kicker: "A small side cannot finish",
    body: "A network cut leaves the leader with only one partner. That pair can still pass notes, but it cannot make them permanent. The other three are a majority, so they can choose their own leader. The new note on the small side stays pale.",
    glossary: [
      { word: "Partition", meaning: "A cut that stops some servers from hearing the others." },
    ],
    setup: () => {
      const sim = withLeader();
      const leader = leaderId(sim);
      if (leader === null) return sim;
      propose(sim, NOTE, leader);
      runUntil(
        sim,
        (state) => state.nodes.every((node) => node.commitIndex >= 1 && node.log[0]?.command === NOTE),
        200,
      );
      sim.messages = [];
      const others = sim.nodes.map((node) => node.id).filter((id) => id !== leader);
      const partner = others[0]!;
      setPartitions(sim, [[leader, partner], others.slice(1)]);
      return sim;
    },
    script: [
      {
        atTick: 2,
        run: (sim) => {
          const minority = sim.partitions?.[0]?.[0];
          if (minority !== undefined) propose(sim, SIDE_NOTE, minority);
        },
      },
    ],
    pauseWhen: (sim) => {
      const holder = sim.nodes.find((node) => node.log.some((entry) => entry.command === SIDE_NOTE));
      if (!holder) return false;
      const index = holder.log.findIndex((entry) => entry.command === SIDE_NOTE);
      const stillDraft = holder.commitIndex < index + 1;
      const otherLeader = sim.nodes.some(
        (node) =>
          node.role === "leader" &&
          !node.crashed &&
          !node.log.some((entry) => entry.command === SIDE_NOTE),
      );
      return stillDraft && otherLeader;
    },
  },
];

export function minoritySplit(sim: Sim): number[][] {
  const leader = leaderId(sim);
  const ids = sim.nodes.map((node) => node.id);
  const head = leader ?? ids[0]!;
  const rest = ids.filter((id) => id !== head);
  const partner = rest[0] ?? head;
  return [[head, partner], rest.filter((id) => id !== partner)];
}
