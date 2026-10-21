import { describe, expect, it } from "vitest";
import { benchmarkBars } from "./benchmarks";

describe("benchmark bars", () => {
  it("uses only the stable loopback figures", () => {
    expect(benchmarkBars.map((bar) => [bar.baseline, bar.current])).toEqual([
      [1487.2, 5776.7],
      [237.9, 170.0],
      [213363, 32307],
    ]);
  });
});
