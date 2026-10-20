import { useEffect, useReducer } from "react";
import { cloneSim, crash, restart, setPartitions, tick, type Sim } from "./sim/raft";
import { baseSteps, chapterLinks, extendedSteps, minoritySplit, type ChapterStep } from "./ui/chapters";
import { Stage } from "./ui/Stage";

type Speed = 1 | 2 | 4;
type ChapterId = "base" | "extended";
type Compare = { off: number | null; on: number | null };

type State = {
  chapter: ChapterId;
  stepIndex: number;
  extensionOn: boolean;
  compare: Compare;
  sim: Sim;
  originTick: number;
  fired: number[];
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
  | { type: "toggle-node"; id: number }
  | { type: "toggle-partition" };

function stepsFor(chapter: ChapterId): ChapterStep[] {
  return chapter === "extended" ? extendedSteps : baseSteps;
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
  const enabled = chapter === "extended" && extensionOn;
  const sim = step.setup(enabled);
  return {
    chapter,
    stepIndex: index,
    extensionOn: enabled,
    compare,
    sim,
    originTick: sim.tickCount,
    fired: [],
    playing: false,
    speed,
    frame: 0,
  };
}

function advance(state: State, step: ChapterStep): State {
  const sim = tick(cloneSim(state.sim));
  const relative = sim.tickCount - state.originTick;
  const fired = state.fired.slice();
  for (const event of step.script) {
    if (event.atTick === relative && !fired.includes(event.atTick)) {
      event.run(sim);
      fired.push(event.atTick);
    }
  }
  step.onTick?.(sim, relative);
  let playing = state.playing;
  let compare = state.compare;
  if (step.pauseWhen(sim)) {
    playing = false;
    if (step.extension) {
      const value = step.extension.measure(sim);
      compare = state.extensionOn ? { ...state.compare, on: value } : { ...state.compare, off: value };
    }
  }
  return { ...state, sim, fired, playing, compare, frame: state.frame + 1 };
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
      const current = state.sim.nodes[action.id];
      if (!current) return state;
      const sim = cloneSim(state.sim);
      if (current.crashed) restart(sim, action.id);
      else crash(sim, action.id);
      return { ...state, sim, frame: state.frame + 1 };
    }
    case "toggle-partition": {
      const sim = cloneSim(state.sim);
      if (state.sim.partitions) setPartitions(sim, null);
      else setPartitions(sim, minoritySplit(state.sim));
      return { ...state, sim, frame: state.frame + 1 };
    }
    default:
      return state;
  }
}

export function App() {
  const [state, dispatch] = useReducer(reducer, undefined, () => loadStep("base", 0));
  const steps = stepsFor(state.chapter);
  const step = steps[state.stepIndex] ?? steps[0]!;
  const tickMs = Math.round(480 / state.speed);

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
                if (link.id === "base" || link.id === "extended") dispatch({ type: "chapter", id: link.id });
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
            Chapter {state.chapter === "extended" ? 2 : 1}, step {state.stepIndex + 1} of {steps.length}
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
          {step.extension && (
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
              <p className="live">
                This run: {step.extension.measure(state.sim)}
              </p>
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
          <p className="later">
            {state.chapter === "extended"
              ? "Synkrolyn, the last chapter, is still to come."
              : "Chapter 2 turns each extension on and off. Synkrolyn is still to come."}
          </p>
        </section>

        <section className="exhibit">
          <div className="controls">
            <button type="button" onClick={() => dispatch({ type: "play" })} aria-pressed={state.playing} disabled={state.playing}>
              Play
            </button>
            <button type="button" onClick={() => dispatch({ type: "pause" })} disabled={!state.playing}>
              Pause
            </button>
            <button type="button" onClick={() => dispatch({ type: "step" })}>
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
            <button
              type="button"
              onClick={() => dispatch({ type: "toggle-partition" })}
              aria-pressed={state.sim.partitions !== null}
              className={state.sim.partitions ? "on" : ""}
            >
              {state.sim.partitions ? "Heal the cut" : "Cut the network"}
            </button>
            <span className="tick-readout" title="One tick is one small moment in the tour.">
              Tick {state.sim.tickCount}
            </span>
          </div>
          {state.sim.banner && state.sim.banner !== "cut" && state.sim.banner !== "asked" && (
            <p className="banner">{state.sim.banner}</p>
          )}
          <Stage sim={state.sim} tickMs={tickMs} onToggleNode={(id) => dispatch({ type: "toggle-node", id })} />
        </section>
      </main>
    </div>
  );
}
