import { describe, expect, it } from "vitest";
import { tick } from "../sim/raft";
import { baseSteps } from "./chapters";

describe("base raft chapter", () => {
  it("reaches the pictured moment of every step", () => {
    for (const step of baseSteps) {
      const sim = step.setup();
      const origin = sim.tickCount;
      const fired = new Set<number>();
      let pictured = false;
      for (let i = 0; i < 500; i += 1) {
        tick(sim);
        const relative = sim.tickCount - origin;
        for (const event of step.script) {
          if (event.atTick === relative && !fired.has(event.atTick)) {
            event.run(sim);
            fired.add(event.atTick);
          }
        }
        if (step.pauseWhen(sim)) {
          pictured = true;
          break;
        }
      }
      expect(pictured, step.id).toBe(true);
    }
  });
});
