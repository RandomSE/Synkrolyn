import { useEffect, useReducer } from "react";
import { cloneSim, crash, restart, setPartitions, tick, type Sim } from "./sim/raft";
import { Finale } from "./ui/Finale";
import { Numbers } from "./ui/Numbers";
import { Stage } from "./ui/Stage";
import {
  baseSteps,
  chapterLinks,
  extendedSteps,
  minoritySplit,
  synkrolynSteps,
  type ChapterStep,
} from "./ui/chapters";

type Speed = 1 | 2 | 4;
type ChapterId = "base" | "extended" | "synkrolyn";
type Side = "left" | "right";
type Compare = { off: number | null; on: number | null };

type State = {
  chapter: ChapterId;
  stepIndex: number;
  extensionOn: boolean;
  compare: Compare;
  sim: Sim;
  originTick: number;
  fired: number[];
  right: Sim | null;
  rightOrigin: number;
  rightFired: number[];
  playing: boolean;
  speed: Speed;
  frame: number;
};

type Action =
  | { type: "play" }
  | { type: "pause" }
  | { type: "step" }
  | { type: "tick" }
  | { type: "reset" }
  | { type: "speed"; speed: Speed }
  | { type: "goto"; index: number }
  | { type: "chapter"; id: ChapterId }
  | { type: "toggle-extension" }
  | { type: "toggle-node"; id: number; side: Side }
  | { type: "toggle-partition" };

function stepsFor(chapter: ChapterId): ChapterStep[] {
  if (chapter === "extended") return extendedSteps;
  if (chapter === "synkrolyn") return synkrolynSteps;
  return baseSteps;
}

function chapterNumber(chapter: ChapterId): number {
  if (chapter === "extended") return 2;
  if (chapter === "synkrolyn") return 3;
  return 1;
}

function runMoment(sim: Sim, step: ChapterStep, origin: number, fired: number[]): number[] {
  const relative = sim.tickCount - origin;
  const next = fired.slice();
  for (const event of step.script) {
    if (event.atTick === relative && !next.includes(event.atTick)) {
      event.run(sim);
      next.push(event.atTick);
    }
  }
  step.onTick?.(sim, relative);
  return next;
}

function loadStep(
  chapter: ChapterId,
  index: number,
  speed: Speed = 1,
  extensionOn = false,
  compare: Compare = { off: null, on: null },
): State {
  const steps = stepsFor(chapter);
  const step = steps[index] ?? steps[0]!;
  const pair = Boolean(step.pair);
  const enabled = !pair && chapter === "extended" && extensionOn;
  const sim = step.setup(pair ? false : enabled);
  const right = pair ? step.setup(true) : null;
  return {
    chapter,
    stepIndex: index,
    extensionOn: enabled,
    compare,
    sim,
    originTick: sim.tickCount,
    fired: [],
    right,
    rightOrigin: right?.tickCount ?? 0,
    rightFired: [],
    playing: false,
    speed,
    frame: 0,
  };
}

function advance(state: State, step: ChapterStep): State {
  let sim = state.sim;
  let fired = state.fired;
  if (!step.pauseWhen(sim)) {
    sim = tick(cloneSim(sim));
    fired = runMoment(sim, step, state.originTick, fired);
  }
  let right = state.right;
  let rightFired = state.rightFired;
  if (right && step.pair && !step.pauseWhen(right)) {
    right = tick(cloneSim(right));
    rightFired = runMoment(right, step, state.rightOrigin, rightFired);
  }
  let playing = state.playing;
  let compare = state.compare;
  const leftDone = step.pauseWhen(sim);
  const rightDone = !step.pair || right === null || step.pauseWhen(right);
  if (leftDone && rightDone) {
    playing = false;
    if (step.extension && step.pair && right) {
      compare = { off: step.extension.measure(sim), on: step.extension.measure(right) };
    } else if (step.extension) {
      const value = step.extension.measure(sim);
      compare = state.extensionOn ? { ...state.compare, on: value } : { ...state.compare, off: value };
    }
  }
  return { ...state, sim, right, fired, rightFired, playing, compare, frame: state.frame + 1 };
}

function reducer(state: State, action: Action): State {
  const steps = stepsFor(state.chapter);
  const step = steps[state.stepIndex] ?? steps[0]!;
  switch (action.type) {
    case "play":
      return { ...state, playing: true };
    case "pause":
      return { ...state, playing: false };
    case "speed":
      return { ...state, speed: action.speed };
    case "goto":
      return loadStep(state.chapter, action.index, state.speed);
    case "chapter":
      return loadStep(action.id, 0, state.speed);
    case "toggle-extension": {
      const next = loadStep(state.chapter, state.stepIndex, state.speed, !state.extensionOn, state.compare);
      return { ...next, playing: true };
    }
    case "reset":
      return loadStep(state.chapter, state.stepIndex, state.speed, state.extensionOn);
    case "tick":
      return advance(state, step);
    case "step":
      return { ...advance({ ...state, playing: false }, step), playing: false };
    case "toggle-node": {
      const source = action.side === "right" ? state.right : state.sim;
      if (!source) return state;
      const current = source.nodes[action.id];
      if (!current) return state;
      const next = cloneSim(source);
      if (current.crashed) restart(next, action.id);
      else crash(next, action.id);
      if (action.side === "right") return { ...state, right: next, frame: state.frame + 1 };
      return { ...state, sim: next, frame: state.frame + 1 };
    }
    case "toggle-partition": {
      const sim = cloneSim(state.sim);
      setPartitions(sim, state.sim.partitions ? null : minoritySplit(state.sim));
      let right = state.right;
      if (right && step.pair) {
        right = cloneSim(right);
        setPartitions(right, state.right?.partitions ? null : minoritySplit(right));
      }
      return { ...state, sim, right, frame: state.frame + 1 };
    }
    default:
      return state;
  }
}

function laterLine(chapter: ChapterId): string {
  if (chapter === "synkrolyn") return "Base Raft and Extended Raft stay in the chapters above.";
  if (chapter === "extended") return "Chapter 3 runs Extended Raft and Synkrolyn side by side.";
  return "Chapter 2 turns each extension on and off. Chapter 3 runs both sides at once.";
}

function showBanner(text: string): boolean {
  return text.length > 0 && text !== "cut" && text !== "asked";
}

export function App() {
  const [state, dispatch] = useReducer(reducer, undefined, () => loadStep("base", 0));
  const steps = stepsFor(state.chapter);
  const step = steps[state.stepIndex] ?? steps[0]!;
  const tickMs = Math.round(480 / state.speed);
  const pair = Boolean(step.pair && state.right);

  useEffect(() => {
    if (!state.playing) return;
    const id = window.setInterval(() => dispatch({ type: "tick" }), tickMs);
    return () => window.clearInterval(id);
  }, [state.playing, tickMs]);

  return (
    <div className="page">
      <header className="top">
        <div>
          <p className="brand">Synkrolyn</p>
          <h1>Raft, in plain sight</h1>
          <p className="lede">
            A small live picture of how a group of servers agrees on one story. Nothing here talks to the
            real engine. It is a guided tour.
          </p>
        </div>
        <nav className="chapters" aria-label="Chapters">
          {chapterLinks.map((link) => (
            <button
              key={link.id}
              type="button"
              className={
                !link.available ? "chapter soon" : link.id === state.chapter ? "chapter current" : "chapter"
              }
              disabled={!link.available}
              onClick={() => {
                if (link.available) dispatch({ type: "chapter", id: link.id });
              }}
            >
              {link.title}
              {!link.available && <span>coming soon</span>}
            </button>
          ))}
        </nav>
      </header>

      <main className="layout">
        <section className="story" aria-live="polite">
          <p className="kicker">
            Chapter {chapterNumber(state.chapter)}, step {state.stepIndex + 1} of {steps.length}
          </p>
          <h2>{step.title}</h2>
          <p className="kicker-line">{step.kicker}</p>
          <p className="body">{step.body}</p>
          <ul className="glossary">
            {step.glossary.map((line) => (
              <li key={line.word}>
                <strong>{line.word}.</strong> {line.meaning}
              </li>
            ))}
          </ul>
          {step.extension && !step.pair && (
            <div className="extension">
              <button
                type="button"
                className={state.extensionOn ? "on" : ""}
                aria-pressed={state.extensionOn}
                onClick={() => dispatch({ type: "toggle-extension" })}
              >
                {state.extensionOn ? `${step.extension.name} on` : `${step.extension.name} off`}
              </button>
              <div className="compare" aria-label={step.extension.counter}>
                <p>{step.extension.counter}</p>
                <div>
                  <span>Fix off</span>
                  <strong>{state.compare.off ?? "..."}</strong>
                </div>
                <div>
                  <span>Fix on</span>
                  <strong>{state.compare.on ?? "..."}</strong>
                </div>
              </div>
              <p className="live">This run: {step.extension.measure(state.sim)}</p>
            </div>
          )}
          {step.pair && step.extension && (
            <div className="compare" aria-label={step.extension.counter}>
              <p>{step.extension.counter}</p>
              <div>
                <span>Extended Raft</span>
                <strong>{step.extension.measure(state.sim)}</strong>
              </div>
              <div>
                <span>Synkrolyn</span>
                <strong>{state.right ? step.extension.measure(state.right) : "..."}</strong>
              </div>
            </div>
          )}
          <div className="pager">
            <button
              type="button"
              onClick={() => dispatch({ type: "goto", index: state.stepIndex - 1 })}
              disabled={state.stepIndex === 0}
            >
              Back
            </button>
            <button
              type="button"
              onClick={() => dispatch({ type: "goto", index: state.stepIndex + 1 })}
              disabled={state.stepIndex >= steps.length - 1}
            >
              Next
            </button>
          </div>
          <p className="later">{laterLine(state.chapter)}</p>
        </section>

        <section className="exhibit">
          <div className="controls">
            <button type="button" onClick={() => dispatch({ type: "play" })} aria-pressed={state.playing} disabled={state.playing || Boolean(step.screen)}>
              Play
            </button>
            <button type="button" onClick={() => dispatch({ type: "pause" })} disabled={!state.playing}>
              Pause
            </button>
            <button type="button" onClick={() => dispatch({ type: "step" })} disabled={Boolean(step.screen)}>
              Step
            </button>
            <button type="button" onClick={() => dispatch({ type: "reset" })}>
              Reset
            </button>
            <div className="speeds" role="group" aria-label="Speed">
              {([1, 2, 4] as const).map((value) => (
                <button
                  key={value}
                  type="button"
                  aria-pressed={state.speed === value}
                  className={state.speed === value ? "on" : ""}
                  onClick={() => dispatch({ type: "speed", speed: value })}
                >
                  {value}x
                </button>
              ))}
            </div>
            {!step.screen && (
              <button
                type="button"
                onClick={() => dispatch({ type: "toggle-partition" })}
                aria-pressed={state.sim.partitions !== null}
                className={state.sim.partitions ? "on" : ""}
              >
                {state.sim.partitions ? "Heal the cut" : "Cut the network"}
              </button>
            )}
            {!step.screen && (
              <span className="tick-readout" title="One tick is one small moment in the tour.">
                Tick {state.sim.tickCount}
              </span>
            )}
          </div>
          {step.screen === "numbers" && <Numbers />}
          {step.screen === "finale" && <Finale />}
          {!step.screen && pair && state.right && (
            <div className="pair">
              <div>
                <p className="pair-label">Extended Raft</p>
                {showBanner(state.sim.banner) && <p className="banner">{state.sim.banner}</p>}
                <Stage sim={state.sim} tickMs={tickMs} onToggleNode={(id) => dispatch({ type: "toggle-node", id, side: "left" })} />
              </div>
              <div>
                <p className="pair-label">Synkrolyn</p>
                {showBanner(state.right.banner) && <p className="banner">{state.right.banner}</p>}
                <Stage sim={state.right} tickMs={tickMs} onToggleNode={(id) => dispatch({ type: "toggle-node", id, side: "right" })} />
              </div>
            </div>
          )}
          {!step.screen && !pair && (
            <>
              {showBanner(state.sim.banner) && <p className="banner">{state.sim.banner}</p>}
              <Stage sim={state.sim} tickMs={tickMs} onToggleNode={(id) => dispatch({ type: "toggle-node", id, side: "left" })} />
            </>
          )}
        </section>
      </main>
    </div>
  );
}
