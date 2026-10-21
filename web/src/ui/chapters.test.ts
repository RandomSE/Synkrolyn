import { describe, expect, it } from "vitest";
import { tick } from "../sim/raft";
import { baseSteps, extendedSteps, synkrolynSteps, type ChapterStep } from "./chapters";

function play(step: ChapterStep, enabled = false) {
  const sim = step.setup(enabled);
  const origin = sim.tickCount;
  const fired = new Set<number>();
  for (let i = 0; i < 500; i += 1) {
    tick(sim);
    const relative = sim.tickCount - origin;
    for (const event of step.script) {
      if (event.atTick === relative && !fired.has(event.atTick)) {
        event.run(sim);
        fired.add(event.atTick);
      }
    }
    step.onTick?.(sim, relative);
    if (step.pauseWhen(sim)) return sim;
  }
  return null;
}

describe("chapters", () => {
  it("reaches the pictured moment of every base step", () => {
    for (const step of baseSteps) {
      expect(play(step), step.id).not.toBeNull();
    }
  });

  it("shows the problem with the fix off and the calmer result with it on", () => {
    for (const step of extendedSteps) {
      const off = play(step, false);
      const on = play(step, true);
      expect(off, step.id).not.toBeNull();
      expect(on, step.id).not.toBeNull();
      expect(step.extension!.measure(on!)).toBeLessThan(step.extension!.measure(off!));
    }
  });

  it("shows Synkrolyn ahead of extended Raft on every paired run", () => {
    const pairs = synkrolynSteps.filter((step) => step.pair);
    expect(pairs.length).toBeGreaterThanOrEqual(3);
    expect(pairs.length).toBeLessThanOrEqual(5);
    for (const step of pairs) {
      const off = play(step, false);
      const on = play(step, true);
      expect(off, step.id).not.toBeNull();
      expect(on, step.id).not.toBeNull();
      const left = step.extension!.measure(off!);
      const right = step.extension!.measure(on!);
      if (step.extension!.better === "higher") expect(right, step.id).toBeGreaterThan(left);
      else expect(right, step.id).toBeLessThan(left);
    }
  });

  it("ends on the numbers and a recap of the three chapters", () => {
    expect(synkrolynSteps.at(-2)?.screen).toBe("numbers");
    expect(synkrolynSteps.at(-1)?.screen).toBe("finale");
  });
});
