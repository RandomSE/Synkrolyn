import { describe, expect, it } from "vitest";
import {
  cloneSim,
  crash,
  createSim,
  leaderId,
  propose,
  runUntil,
  setPartitions,
  tick,
} from "./raft";

describe("base raft teaching sim", () => {
  it("elects a leader", () => {
    const sim = createSim({ nodeCount: 5, seed: 1 });
    expect(runUntil(sim, (state) => leaderId(state) !== null, 200)).toBe(true);
    const id = leaderId(sim);
    expect(id).not.toBeNull();
    expect(sim.nodes[id!].role).toBe("leader");
    expect(sim.nodes[id!].crashed).toBe(false);
    expect(sim.nodes.filter((node) => node.role === "leader" && !node.crashed)).toHaveLength(1);
    expect(sim.nodes[id!].term).toBeGreaterThan(0);
  });

  it("keeps a committed entry after the leader crashes", () => {
    const sim = createSim({ nodeCount: 5, seed: 1 });
    expect(runUntil(sim, (state) => leaderId(state) !== null, 200)).toBe(true);
    const lead = leaderId(sim)!;
    propose(sim, "save-note", lead);
    expect(
      runUntil(
        sim,
        (state) => state.nodes.every((node) => node.commitIndex >= 1 && node.log[0]?.command === "save-note"),
        200,
      ),
    ).toBe(true);

    crash(sim, lead);
    expect(
      runUntil(sim, (state) => {
        const id = leaderId(state);
        return id !== null && id !== lead;
      }, 400),
    ).toBe(true);

    const next = leaderId(sim)!;
    expect(sim.nodes[next].log[0]?.command).toBe("save-note");
    expect(sim.nodes[next].commitIndex).toBeGreaterThanOrEqual(1);
    for (const node of sim.nodes) {
      if (node.commitIndex >= 1) {
        expect(node.log[0]?.command).toBe("save-note");
      }
    }
  });

  it("does not commit a write inside a minority partition", () => {
    const sim = createSim({ nodeCount: 5, seed: 1 });
    expect(runUntil(sim, (state) => leaderId(state) !== null, 200)).toBe(true);
    const lead = leaderId(sim)!;
    const others = sim.nodes.map((node) => node.id).filter((id) => id !== lead);
    const partner = others[0]!;
    const majority = others.slice(1);
    setPartitions(sim, [[lead, partner], majority]);
    const before = sim.nodes[lead].commitIndex;

    propose(sim, "must-not-stick", lead);
    for (let i = 0; i < 300; i += 1) {
      tick(sim);
    }

    expect(sim.nodes[lead].log.some((entry) => entry.command === "must-not-stick")).toBe(true);
    expect(sim.nodes[lead].commitIndex).toBe(before);
    for (const node of sim.nodes) {
      const index = node.log.findIndex((entry) => entry.command === "must-not-stick");
      if (index >= 0) {
        expect(node.commitIndex).toBeLessThan(index + 1);
      }
    }
  });

  it("clones a sim without sharing the log or the partition", () => {
    const sim = createSim({ nodeCount: 3, seed: 2 });
    setPartitions(sim, [[0, 1], [2]]);
    sim.nodes[0]!.log.push({ term: 1, command: "keep" });
    const copy = cloneSim(sim);
    copy.nodes[0]!.log.push({ term: 1, command: "only-copy" });
    setPartitions(copy, null);
    expect(sim.nodes[0]!.log.map((entry) => entry.command)).toEqual(["keep"]);
    expect(sim.partitions).toEqual([[0, 1], [2]]);
  });
});
