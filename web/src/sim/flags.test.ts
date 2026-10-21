import { describe, expect, it } from "vitest";
import {
  createSim,
  leaderId,
  propose,
  requestRead,
  runUntil,
  setPartitions,
  tick,
  type ReadResult,
  type Sim,
} from "./raft";

function awaitRead(sim: Sim, nodeId: number): ReadResult {
  const first = requestRead(sim, nodeId);
  if (first.status !== "pending") return first;
  for (let i = 0; i < 40; i += 1) {
    tick(sim);
    const current = sim.reads.find((read) => read.id === first.id);
    if (current && current.status !== "pending") return current;
  }
  return sim.reads.find((read) => read.id === first.id) ?? first;
}

function elect(sim: Sim): number {
  expect(runUntil(sim, (state) => leaderId(state) !== null, 200)).toBe(true);
  return leaderId(sim)!;
}

function commitOnAll(sim: Sim, leader: number, command: string): void {
  propose(sim, command, leader);
  expect(
    runUntil(
      sim,
      (state) => state.nodes.every((node) => node.commitIndex >= 1 && node.log[0]?.command === command),
      200,
    ),
  ).toBe(true);
}

describe("extended raft flags", () => {
  it("preVote stops a flaky node from inflating the term", () => {
    const flap = (preVote: boolean) => {
      const sim = createSim({ nodeCount: 5, seed: 1, flags: { preVote } });
      const leader = elect(sim);
      const flaky = sim.nodes.map((node) => node.id).filter((id) => id !== leader).at(-1)!;
      const termBefore = sim.nodes[leader]!.term;
      const electionsBefore = sim.elections;
      const rest = sim.nodes.map((node) => node.id).filter((id) => id !== flaky);
      for (let round = 0; round < 3; round += 1) {
        setPartitions(sim, [rest, [flaky]]);
        for (let i = 0; i < 50; i += 1) tick(sim);
        setPartitions(sim, null);
        for (let i = 0; i < 20; i += 1) tick(sim);
      }
      return {
        term: Math.max(...sim.nodes.map((node) => node.term)),
        elections: sim.elections - electionsBefore,
        termBefore,
      };
    };

    const broken = flap(false);
    const fixed = flap(true);
    expect(broken.term).toBeGreaterThan(broken.termBefore);
    expect(broken.elections).toBeGreaterThan(0);
    expect(fixed.term).toBe(fixed.termBefore);
    expect(fixed.elections).toBe(0);
  });

  it("checkQuorum makes a cut-off leader step down", () => {
    const claiming = (checkQuorum: boolean) => {
      const sim = createSim({ nodeCount: 5, seed: 1, flags: { checkQuorum } });
      const leader = elect(sim);
      const others = sim.nodes.map((node) => node.id).filter((id) => id !== leader);
      setPartitions(sim, [[leader, others[0]!], others.slice(1)]);
      for (let i = 0; i < 40; i += 1) tick(sim);
      return sim.nodes[leader]!.role === "leader";
    };

    expect(claiming(false)).toBe(true);
    expect(claiming(true)).toBe(false);
  });

  it("readIndex refuses a stale answer from a cut-off leader", () => {
    const answer = (readIndex: boolean) => {
      const sim = createSim({ nodeCount: 5, seed: 1, flags: { readIndex } });
      const leader = elect(sim);
      commitOnAll(sim, leader, "old");
      const others = sim.nodes.map((node) => node.id).filter((id) => id !== leader);
      const majority = others.slice(1);
      setPartitions(sim, [[leader, others[0]!], majority]);
      expect(
        runUntil(sim, (state) => majority.some((id) => state.nodes[id]!.role === "leader"), 200),
      ).toBe(true);
      const next = majority.find((id) => sim.nodes[id]!.role === "leader")!;
      propose(sim, "new", next);
      expect(
        runUntil(sim, (state) => state.nodes[next]!.log.some((entry) => entry.command === "new") && state.nodes[next]!.commitIndex >= 2, 200),
      ).toBe(true);
      return awaitRead(sim, leader);
    };

    const stale = answer(false);
    const checked = answer(true);
    expect(stale.status).toBe("ok");
    expect(stale.value).toBe("old");
    expect(checked.status).not.toBe("ok");
    expect(checked.value).not.toBe("old");
  });

  it("leases answer inside the clock bound and refuse after it", () => {
    const prepared = (leases: boolean) => {
      const sim = createSim({ nodeCount: 5, seed: 1, flags: { leases } });
      const leader = elect(sim);
      commitOnAll(sim, leader, "v1");
      for (let i = 0; i < 20; i += 1) tick(sim);
      return { sim, leader };
    };

    const fast = prepared(true);
    const immediate = requestRead(fast.sim, fast.leader);
    expect(immediate.status).toBe("ok");
    expect(immediate.value).toBe("v1");
    expect(immediate.roundTrip).toBe(false);

    const afterBound = (leases: boolean) => {
      const { sim, leader } = prepared(leases);
      const others = sim.nodes.map((node) => node.id).filter((id) => id !== leader);
      setPartitions(sim, [[leader], others]);
      for (let i = 0; i < 40; i += 1) tick(sim);
      return requestRead(sim, leader);
    };

    const unsafe = afterBound(false);
    const safe = afterBound(true);
    expect(unsafe.status).toBe("ok");
    expect(unsafe.value).toBe("v1");
    expect(safe.status).not.toBe("ok");
  });
});
