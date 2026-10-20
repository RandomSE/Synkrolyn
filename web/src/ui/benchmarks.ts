export type BenchmarkBar = {
  label: string;
  hint: string;
  unit: string;
  baseline: number;
  current: number;
};

export const benchmarkBars: BenchmarkBar[] = [
  {
    label: "Writes per second",
    hint: "More notes per second.",
    unit: "ops/s",
    baseline: 1487.2,
    current: 5776.7,
  },
  {
    label: "Typical write time",
    hint: "A typical write finishes sooner.",
    unit: "us",
    baseline: 237.9,
    current: 170.0,
  },
  {
    label: "Memory per batched write",
    hint: "Each batched write uses less memory.",
    unit: "bytes",
    baseline: 213363,
    current: 32307,
  },
];
