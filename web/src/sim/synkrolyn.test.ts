import { describe, expect, it } from "vitest";
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
  type Sim,
} from "./raft";

function elect(sim: Sim): number {
  expect(runUntil(sim, (state) => leaderId(state) !== null, 200)).toBe(true);
  return leaderId(sim)!;
}

describe("synkrolyn flags", () => {
  it("durableAck does not acknowledge a write that disk has not kept", () => {
    const wiped = (durableAck: boolean) => {
      const sim = createSim({ nodeCount: 5, seed: 1, flags: { durableAck } });
      const leader = elect(sim);
      propose(sim, "save", leader);
      expect(runUntil(sim, (state) => acknowledged(state) === "save", 200)).toBe(true);
      for (const node of sim.nodes) powerLoss(sim, node.id);
      const kept = sim.nodes.filter((node) => node.log.some((entry) => entry.command === "save")).length;
      return kept < 3;
    };

    expect(wiped(false)).toBe(true);
    expect(wiped(true)).toBe(false);
  });

  it("powerLoss drops copies still on the wire", () => {
    const sim = createSim({ nodeCount: 5, seed: 1 });
    const leader = elect(sim);
    propose(sim, "save", leader);
    expect(runUntil(sim, (state) => acknowledged(state) === "save", 200)).toBe(true);
    for (const node of sim.nodes) powerLoss(sim, node.id);
    for (let i = 0; i < 20; i += 1) tick(sim);
    const kept = sim.nodes.filter((node) => node.log.some((entry) => entry.command === "save")).length;
    expect(kept).toBeLessThan(3);
  });

  it("sessions apply one client serial only once", () => {
    const tally = (sessions: boolean) => {
      const sim = createSim({ nodeCount: 5, seed: 1, flags: { sessions } });
      const leader = elect(sim);
      submit(sim, "ada", 1, "add", leader);
      expect(runUntil(sim, (state) => state.tally === 1, 200)).toBe(true);
      submit(sim, "ada", 1, "add", leader);
      for (let i = 0; i < 40; i += 1) tick(sim);
      return sim.tally;
    };

    expect(tally(false)).toBe(2);
    expect(tally(true)).toBe(1);
  });

  it("snapshots bring a blank follower forward after the old log is gone", () => {
    const caughtUp = (snapshots: boolean) => {
      const sim = createSim({ nodeCount: 5, seed: 1, flags: { snapshots } });
      const leader = elect(sim);
      const behind = sim.nodes.map((node) => node.id).filter((id) => id !== leader).at(-1)!;
      crash(sim, behind);
      for (const command of ["a", "b", "c"]) {
        propose(sim, command, leader);
        expect(
          runUntil(sim, (state) => state.nodes[leader]!.log.some((entry) => entry.command === command) && state.nodes[leader]!.commitIndex >= state.nodes[leader]!.log.findIndex((entry) => entry.command === command) + 1, 200),
        ).toBe(true);
      }
      compactPrefix(sim, leader);
      restart(sim, behind);
      for (let i = 0; i < 40; i += 1) tick(sim);
      const node = sim.nodes[behind]!;
      return node.commitIndex >= 3 || node.snapshotIndex >= 3;
    };

    expect(caughtUp(false)).toBe(false);
    expect(caughtUp(true)).toBe(true);
  });

  it("voteGate refuses a challenger inside the leader window", () => {
    const grants = (voteGate: boolean) => {
      const sim = createSim({ nodeCount: 5, seed: 1, flags: { voteGate } });
      const leader = elect(sim);
      for (let i = 0; i < 10; i += 1) tick(sim);
      const challenger = sim.nodes.map((node) => node.id).filter((id) => id !== leader)[0]!;
      const term = sim.nodes[leader]!.term;
      campaign(sim, challenger);
      for (let i = 0; i < 8; i += 1) tick(sim);
      return {
        grants: sim.nodes.filter((node) => node.id !== challenger && node.votedFor === challenger).length,
        term,
        leaderTerm: sim.nodes[leader]!.term,
      };
    };

    const open = grants(false);
    const gated = grants(true);
    expect(open.grants).toBeGreaterThan(0);
    expect(gated.grants).toBe(0);
    expect(gated.leaderTerm).toBe(gated.term);
  });

  it("leaseGrace ends the fast read before the full window", () => {
    const prepared = (leaseGrace: boolean) => {
      const sim = createSim({ nodeCount: 5, seed: 1, flags: { leases: true, leaseGrace } });
      const leader = elect(sim);
      propose(sim, "v1", leader);
      expect(runUntil(sim, (state) => state.nodes.every((node) => node.commitIndex >= 1), 200)).toBe(true);
      for (let i = 0; i < 20; i += 1) tick(sim);
      return { sim, leader };
    };

    const full = prepared(false);
    const shrunk = prepared(true);
    expect(shrunk.sim.nodes[shrunk.leader]!.leaseUntil).toBeLessThan(full.sim.nodes[full.leader]!.leaseUntil);

    const servedAt = (sample: { sim: Sim; leader: number }, at: number) => {
      const others = sample.sim.nodes.map((node) => node.id).filter((id) => id !== sample.leader);
      setPartitions(sample.sim, [[sample.leader], others]);
      while (sample.sim.tickCount < at) tick(sample.sim);
      return requestRead(sample.sim, sample.leader).status === "ok";
    };

    const edge = shrunk.sim.nodes[shrunk.leader]!.leaseUntil + 1;
    expect(edge).toBeLessThanOrEqual(full.sim.nodes[full.leader]!.leaseUntil);
    expect(servedAt(full, edge)).toBe(true);
    expect(servedAt(shrunk, edge)).toBe(false);
  });
});
