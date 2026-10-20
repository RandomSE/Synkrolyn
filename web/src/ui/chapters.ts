import {
  crash,
  createSim,
  leaderId,
  propose,
  requestRead,
  runUntil,
  setPartitions,
  tick,
  type Sim,
} from "../sim/raft";

export type ChapterLink = {
  id: "base" | "extended" | "synkrolyn";
  title: string;
  available: boolean;
};

export const chapterLinks: ChapterLink[] = [
  { id: "base", title: "Base Raft", available: true },
  { id: "extended", title: "Extended Raft", available: true },
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
  setup: (enabled?: boolean) => Sim;
  script: { atTick: number; run: (sim: Sim) => void }[];
  onTick?: (sim: Sim, relative: number) => void;
  pauseWhen: (sim: Sim) => boolean;
  extension?: {
    flag: "preVote" | "checkQuorum" | "readIndex" | "leases";
    name: string;
    counter: string;
    measure: (sim: Sim) => number;
  };
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

function leaderReady(flags: { preVote?: boolean; checkQuorum?: boolean; readIndex?: boolean; leases?: boolean }): Sim {
  const sim = createSim({ nodeCount: 5, seed: 1, flags });
  runUntil(sim, (state) => leaderId(state) !== null, 200);
  sim.messages = [];
  return sim;
}

function othersOf(sim: Sim, id: number): number[] {
  return sim.nodes.map((node) => node.id).filter((nodeId) => nodeId !== id);
}

export const extendedSteps: ChapterStep[] = [
  {
    id: "prevote",
    title: "A server that will not sit still",
    kicker: "Pointless elections",
    body: "One server keeps dropping off and popping back. While it is alone it shouts for a new vote, and each shout starts a higher round. When it returns, that higher round knocks out the real leader and the group has to vote again. Turn the fix on and it asks quietly first. If the others are happy with the leader, it does not start a new round. Press 4x if you want this story to hurry.",
    glossary: [
      { word: "Round", meaning: "One attempt to choose a leader. A higher round replaces the old one." },
      { word: "PreVote", meaning: "A trial question. A real election starts only if a majority would say yes." },
    ],
    extension: {
      flag: "preVote",
      name: "PreVote",
      counter: "Elections",
      measure: (sim) => sim.elections,
    },
    setup: (enabled = false) => createSim({ nodeCount: 5, seed: 1, flags: { preVote: enabled } }),
    script: [
      {
        atTick: 24,
        run: (sim) => {
          const leader = leaderId(sim);
          if (leader === null) return;
          const flaky = othersOf(sim, leader).at(-1)!;
          setPartitions(sim, [othersOf(sim, flaky), [flaky]]);
        },
      },
      { atTick: 74, run: (sim) => setPartitions(sim, null) },
      {
        atTick: 94,
        run: (sim) => {
          const leader = leaderId(sim);
          if (leader === null) return;
          const flaky = othersOf(sim, leader).at(-1)!;
          setPartitions(sim, [othersOf(sim, flaky), [flaky]]);
        },
      },
      { atTick: 144, run: (sim) => setPartitions(sim, null) },
      {
        atTick: 170,
        run: (sim) => {
          sim.banner = sim.flags.preVote
            ? "The noisy server asked, and the group kept its leader."
            : "The noisy server forced extra elections.";
        },
      },
    ],
    pauseWhen: (sim) => sim.banner.length > 0,
  },
  {
    id: "check-quorum",
    title: "The leader who cannot hear",
    kicker: "Stepping down when cut off",
    body: "A cut leaves the leader with only one partner. It still wears the leader badge, even though new notes on that side can never become permanent. The fix is a regular check: a leader must keep hearing from a majority. If it cannot, it steps down.",
    glossary: [
      { word: "CheckQuorum", meaning: "The leader checks that a majority can still answer. If not, it steps down." },
    ],
    extension: {
      flag: "checkQuorum",
      name: "CheckQuorum",
      counter: "Cut-off servers still in charge",
      measure: (sim) => {
        const group = sim.partitions?.[0] ?? [];
        return group.filter((id) => sim.nodes[id]?.role === "leader" && !sim.nodes[id]?.crashed).length;
      },
    },
    setup: (enabled = false) => leaderReady({ checkQuorum: enabled }),
    script: [],
    onTick: (sim, relative) => {
      if (sim.banner) return;
      const leader = leaderId(sim);
      if (!sim.partitions && leader !== null && relative >= 2) {
        const others = othersOf(sim, leader);
        setPartitions(sim, [[leader, others[0]!], others.slice(1)]);
        return;
      }
      if (sim.partitions && relative >= 36) {
        const claiming = (sim.partitions[0] ?? []).some((id) => sim.nodes[id]?.role === "leader");
        sim.banner = claiming
          ? "The cut-off server still calls itself the leader."
          : "The cut-off server stepped down.";
      }
    },
    pauseWhen: (sim) => sim.banner.length > 0,
  },
  {
    id: "read-index",
    title: "An answer from yesterday",
    kicker: "Checking before you answer",
    body: "The old leader is cut off, and the rest of the group saves a newer note. Someone asks the old leader what the note says. It answers with the old one, because it never checked that it was still in charge. The fix makes it ask the group before answering. Cut off, it stays quiet instead of guessing.",
    glossary: [
      { word: "ReadIndex", meaning: "Confirm you are still the leader before answering a read." },
      { word: "Stale", meaning: "An answer that misses a newer note the rest of the group already kept." },
    ],
    extension: {
      flag: "readIndex",
      name: "ReadIndex",
      counter: "Stale answers",
      measure: (sim) => sim.reads.filter((read) => read.stale).length,
    },
    setup: (enabled = false) => {
      const sim = leaderReady({ readIndex: enabled });
      const leader = leaderId(sim);
      if (leader !== null) propose(sim, "morning", leader);
      runUntil(
        sim,
        (state) => state.nodes.every((node) => node.commitIndex >= 1 && node.log[0]?.command === "morning"),
        200,
      );
      sim.messages = [];
      return sim;
    },
    script: [],
    onTick: (sim, relative) => {
      if (sim.banner.startsWith("Answered") || sim.banner.startsWith("Stayed")) return;
      if (!sim.partitions && relative >= 2) {
        const leader = leaderId(sim);
        if (leader === null) return;
        const others = othersOf(sim, leader);
        setPartitions(sim, [[leader, others[0]!], others.slice(1)]);
        sim.banner = "cut";
        return;
      }
      if (sim.banner === "cut") {
        const majority = sim.partitions?.[1] ?? [];
        const next = majority.find((id) => sim.nodes[id]?.role === "leader" && !sim.nodes[id]?.crashed);
        if (next === undefined) return;
        const node = sim.nodes[next]!;
        if (!node.log.some((entry) => entry.command === "evening")) {
          propose(sim, "evening", next);
          return;
        }
        const index = node.log.findIndex((entry) => entry.command === "evening");
        if (node.commitIndex < index + 1) return;
        const old = sim.partitions?.[0]?.[0];
        if (old === undefined) return;
        requestRead(sim, old);
        sim.banner = "asked";
        return;
      }
      if (sim.banner === "asked") {
        const read = sim.reads.at(-1);
        if (!read || read.status === "pending") return;
        sim.banner = read.status === "ok" ? `Answered: ${read.value}` : "Stayed quiet instead of guessing.";
      }
    },
    pauseWhen: (sim) => sim.banner.startsWith("Answered") || sim.banner.startsWith("Stayed"),
  },
  {
    id: "leases",
    title: "A quick answer, while it is safe",
    kicker: "A promise that expires",
    body: "Once a majority has just answered the leader, it may answer reads on its own for a short time. That skip is safe because nobody else can take over until the promise runs out. The promise is shorter than the time needed to pick a new leader. When the time is up, the leader must not keep guessing.",
    glossary: [
      { word: "Lease", meaning: "A short promise. Followers will not pick a new leader until it expires." },
      { word: "Clock bound", meaning: "The promise lasts less time than a new election would take." },
    ],
    extension: {
      flag: "leases",
      name: "Leases",
      counter: "Answers after the safe window",
      measure: (sim) => {
        const last = sim.reads.at(-1);
        if (!last || sim.reads.length < 2) return 0;
        return last.status === "ok" ? 1 : 0;
      },
    },
    setup: (enabled = false) => {
      const sim = leaderReady({ leases: enabled });
      const leader = leaderId(sim);
      if (leader !== null) propose(sim, "today", leader);
      runUntil(
        sim,
        (state) => state.nodes.every((node) => node.commitIndex >= 1 && node.log[0]?.command === "today"),
        200,
      );
      for (let i = 0; i < 20; i += 1) tick(sim);
      sim.messages = [];
      return sim;
    },
    script: [],
    onTick: (sim, relative) => {
      if (sim.banner.includes("After the window")) return;
      const leaderNow = leaderId(sim);
      if (relative === 2 && sim.reads.length === 0 && leaderNow !== null) {
        requestRead(sim, leaderNow);
        return;
      }
      if (relative === 5 && !sim.partitions && leaderNow !== null) {
        setPartitions(sim, [[leaderNow], othersOf(sim, leaderNow)]);
        return;
      }
      if (relative === 50 && sim.partitions && sim.reads.length >= 1) {
        const cut = sim.partitions[0]?.[0];
        if (cut === undefined) return;
        const second = requestRead(sim, cut);
        const late = second.status === "ok" ? `it still answers "${second.value}"` : "it stays quiet";
        const quick = sim.flags.leases ? "While the promise held, it answered at once. " : "";
        sim.banner = `${quick}After the window, ${late}.`;
      }
    },
    pauseWhen: (sim) => sim.banner.includes("After the window"),
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
