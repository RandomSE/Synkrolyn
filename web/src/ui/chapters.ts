import {
  acknowledged,
  campaign,
  compactPrefix,
  crash,
  createSim,
  leaderId,
  powerLoss,
  propose,
  requestRead,
  restart,
  runUntil,
  setPartitions,
  submit,
  tick,
  type FeatureFlags,
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
  { id: "synkrolyn", title: "Synkrolyn", available: true },
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
    flag: string;
    name: string;
    counter: string;
    measure: (sim: Sim) => number;
    better?: "lower" | "higher";
  };
  pair?: boolean;
  screen?: "numbers" | "finale";
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

function electWith(flags: Partial<FeatureFlags>): Sim {
  const sim = createSim({ nodeCount: 5, seed: 1, flags });
  runUntil(sim, (state) => leaderId(state) !== null, 200);
  sim.messages = [];
  return sim;
}

function othersExcept(sim: Sim, id: number): number[] {
  return sim.nodes.map((node) => node.id).filter((nodeId) => nodeId !== id);
}

export const synkrolynSteps: ChapterStep[] = [
  {
    id: "durable-ack",
    title: "Told yes before it was saved",
    kicker: "Waiting for the disk",
    body: "The leader can say a note is safe as soon as most servers hold it in memory. Pull the power then, and those copies vanish, so the yes was too early. Synkrolyn waits until the note is on disk on a majority before it says yes.",
    glossary: [
      { word: "Durable", meaning: "Written to disk, so a power cut does not erase it." },
      { word: "Acknowledge", meaning: "The moment the group says the write is safe." },
    ],
    pair: true,
    extension: {
      flag: "durableAck",
      name: "Durable ack",
      counter: "Copies still on disk",
      better: "higher",
      measure: (sim) => sim.nodes.filter((node) => node.log.some((entry) => entry.command === "save")).length,
    },
    setup: (enabled = false) => electWith({ durableAck: enabled }),
    script: [],
    onTick: (sim, relative) => {
      if (sim.banner) return;
      if (relative === 2) {
        const leader = leaderId(sim);
        if (leader !== null) propose(sim, "save", leader);
        return;
      }
      if (acknowledged(sim) === "save") {
        for (const node of sim.nodes) powerLoss(sim, node.id);
        const kept = sim.nodes.filter((node) => node.log.some((entry) => entry.command === "save")).length;
        sim.banner = kept >= 3
          ? "The yes waited until the note was on disk."
          : "Said yes, then a power loss erased it.";
      }
    },
    pauseWhen: (sim) => sim.banner.length > 0,
  },
  {
    id: "sessions",
    title: "The same request, twice",
    kicker: "A retry should not count twice",
    body: "A client sends add, hears nothing, and sends that same request again. With no memory of the first one, the group counts it twice. Synkrolyn remembers the client and the request number, so the repeat does not count a second time.",
    glossary: [
      { word: "Session", meaning: "A memory of which requests from one client already counted." },
      { word: "Serial", meaning: "The client's own number for one request." },
    ],
    pair: true,
    extension: {
      flag: "sessions",
      name: "Sessions",
      counter: "Times the add counted",
      better: "lower",
      measure: (sim) => sim.tally,
    },
    setup: (enabled = false) => electWith({ sessions: enabled }),
    script: [],
    onTick: (sim, relative) => {
      if (sim.banner) return;
      const leader = leaderId(sim);
      if (relative === 2 && leader !== null) {
        submit(sim, "ada", 1, "add", leader);
        return;
      }
      if (relative === 30 && leader !== null) {
        submit(sim, "ada", 1, "add", leader);
        return;
      }
      if (relative >= 50) {
        sim.banner = sim.tally > 1
          ? "The repeat counted a second time."
          : "The repeat did not count a second time.";
      }
    },
    pauseWhen: (sim) => sim.banner.length > 0,
  },
  {
    id: "snapshots",
    title: "A blank server and a packed history",
    kicker: "Catching up after old notes are gone",
    body: "The leader throws away notes it has already made permanent. A server that was off the whole time comes back empty. Without a packed copy of that history, it never catches up. Synkrolyn sends the packed history, and the blank server jumps forward.",
    glossary: [
      { word: "Snapshot", meaning: "A packed copy of everything already made permanent." },
      { word: "Blank server", meaning: "A server that missed the notes and starts empty." },
    ],
    pair: true,
    extension: {
      flag: "snapshots",
      name: "Snapshots",
      counter: "Notes the blank server holds",
      better: "higher",
      measure: (sim) => {
        const node = sim.nodes[sim.mark];
        if (!node) return 0;
        return Math.max(node.commitIndex, node.snapshotIndex);
      },
    },
    setup: (enabled = false) => {
      const sim = electWith({ snapshots: enabled });
      const leader = leaderId(sim);
      if (leader === null) return sim;
      const behind = othersExcept(sim, leader).at(-1);
      if (behind === undefined) return sim;
      sim.mark = behind;
      crash(sim, behind);
      for (const command of ["a", "b", "c"]) {
        propose(sim, command, leader);
        runUntil(sim, (state) => {
          const node = state.nodes[leader]!;
          const index = node.log.findIndex((entry) => entry.command === command);
          return index >= 0 && node.commitIndex >= index + 1;
        }, 200);
      }
      compactPrefix(sim, leader);
      restart(sim, behind);
      sim.messages = [];
      return sim;
    },
    script: [],
    onTick: (sim, relative) => {
      if (sim.banner) return;
      const node = sim.nodes[sim.mark];
      const held = node ? Math.max(node.commitIndex, node.snapshotIndex) : 0;
      if (held >= 3 || relative >= 40) {
        sim.banner = held >= 3
          ? "The blank server received the packed history."
          : "The old notes were discarded, and the blank server never caught up.";
      }
    },
    pauseWhen: (sim) => sim.banner.length > 0,
  },
  {
    id: "vote-gate",
    title: "A vote while the leader is fine",
    kicker: "Ignoring a challenger too soon",
    body: "A server that has just heard from the leader should not hand its vote to someone else. Extended Raft still does, which can stir up a new election for no reason. Synkrolyn keeps a short window after each heartbeat. A challenge inside that window is refused, and the leader stays.",
    glossary: [
      { word: "Vote gate", meaning: "A short time after hearing the leader when a server will not vote for anyone else." },
      { word: "Challenger", meaning: "A server that asks for votes before it should." },
    ],
    pair: true,
    extension: {
      flag: "voteGate",
      name: "Vote gate",
      counter: "Votes given to the challenger",
      better: "lower",
      measure: (sim) => sim.nodes.filter((node) => node.id !== sim.mark && node.votedFor === sim.mark).length,
    },
    setup: (enabled = false) => {
      const sim = electWith({ voteGate: enabled });
      for (let i = 0; i < 10; i += 1) tick(sim);
      const leader = leaderId(sim);
      sim.mark = leader === null ? 0 : othersExcept(sim, leader)[0] ?? 0;
      sim.messages = [];
      return sim;
    },
    script: [],
    onTick: (sim, relative) => {
      if (sim.banner) return;
      if (relative === 2) {
        campaign(sim, sim.mark);
        return;
      }
      if (relative >= 12) {
        const grants = sim.nodes.filter((node) => node.id !== sim.mark && node.votedFor === sim.mark).length;
        sim.banner = grants > 0
          ? "Servers voted for a challenger while the leader was fine."
          : "The challenge was refused, and the leader stayed.";
      }
    },
    pauseWhen: (sim) => sim.banner.length > 0,
  },
  {
    id: "lease-grace",
    title: "A promise a little shorter than the clock",
    kicker: "Leaving room for a slow clock",
    body: "The leader may answer a read on its own while a short promise holds. Clocks do not agree perfectly, so Synkrolyn ends that promise a little early. At the same moment, the longer promise still answers and the shorter one has already stopped.",
    glossary: [
      { word: "Lease", meaning: "A short promise. The leader may answer reads alone until it ends." },
      { word: "Clock bound", meaning: "The promise is shortened by a small allowance for clocks that disagree." },
    ],
    pair: true,
    extension: {
      flag: "leaseGrace",
      name: "Lease grace",
      counter: "Answers past the shorter window",
      better: "lower",
      measure: (sim) => (sim.reads.at(-1)?.status === "ok" ? 1 : 0),
    },
    setup: (enabled = false) => {
      const sim = electWith({ leases: true, leaseGrace: enabled });
      const leader = leaderId(sim);
      if (leader !== null) propose(sim, "v1", leader);
      runUntil(sim, (state) => state.nodes.every((node) => node.commitIndex >= 1), 200);
      for (let i = 0; i < 20; i += 1) tick(sim);
      sim.messages = [];
      return sim;
    },
    script: [],
    onTick: (sim, relative) => {
      if (sim.banner) return;
      const leader = leaderId(sim);
      if (relative === 2 && leader !== null && !sim.partitions) {
        setPartitions(sim, [[leader], othersExcept(sim, leader)]);
        sim.mark = sim.nodes[leader]!.leaseUntil;
        return;
      }
      const due = sim.flags.leaseGrace ? sim.mark + 1 : sim.mark;
      if (!sim.partitions || sim.mark <= 0 || sim.tickCount < due || sim.reads.length > 0) return;
      const cut = sim.partitions[0]?.[0];
      if (cut === undefined) return;
      const read = requestRead(sim, cut);
      sim.banner = read.status === "ok"
        ? "It still answered after the shorter window."
        : "It stopped at the clock bound.";
    },
    pauseWhen: (sim) => sim.banner.length > 0,
  },
  {
    id: "numbers",
    title: "By the numbers",
    kicker: "One loopback run",
    body: "Three figures from the benchmark notes, drawn as bars. They come from one loopback run on one machine. They are not a promise about every run.",
    glossary: [
      { word: "Baseline", meaning: "The earlier commit those notes compare against." },
      { word: "This branch", meaning: "The Synkrolyn tree those notes measured." },
    ],
    screen: "numbers",
    setup: fresh,
    script: [],
    pauseWhen: () => true,
  },
  {
    id: "finale",
    title: "Three pictures of the same idea",
    kicker: "What the tour covered",
    body: "Base Raft showed a group picking one leader, copying a note, and making it permanent only with a majority. Extended Raft showed four ways that story frays, and the switch that calms each one. Synkrolyn showed five more promises: the yes waits for disk, a retry counts once, a blank server can catch up, a challenge during a healthy leader is ignored, and the fast answer stops a little early for slow clocks.",
    glossary: [
      { word: "Base Raft", meaning: "A leader, copies of a note, and a majority." },
      { word: "Extended Raft", meaning: "PreVote, CheckQuorum, ReadIndex, and leases." },
      { word: "Synkrolyn", meaning: "Disk, sessions, snapshots, the vote window, and the shorter lease." },
    ],
    screen: "finale",
    setup: fresh,
    script: [],
    pauseWhen: () => true,
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
