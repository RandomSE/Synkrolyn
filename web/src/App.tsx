import { useEffect, useReducer } from "react";
import { cloneSim, crash, restart, setPartitions, tick, type Sim } from "./sim/raft";
import { baseSteps, chapterLinks, minoritySplit, type ChapterStep } from "./ui/chapters";
import { Stage } from "./ui/Stage";

type Speed = 1 | 2 | 4;

type State = {
  stepIndex: number;
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
  | { type: "toggle-node"; id: number }
  | { type: "toggle-partition" };

function loadStep(index: number, speed: Speed = 1): State {
  const step = baseSteps[index] ?? baseSteps[0]!;
  const sim = step.setup();
  return {
    stepIndex: index,
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
  const playing = step.pauseWhen(sim) ? false : state.playing;
  return { ...state, sim, fired, playing, frame: state.frame + 1 };
}

function reducer(state: State, action: Action): State {
  const step = baseSteps[state.stepIndex] ?? baseSteps[0]!;
  switch (action.type) {
    case "play":
      return { ...state, playing: true };
    case "pause":
      return { ...state, playing: false };
    case "speed":
      return { ...state, speed: action.speed };
    case "goto":
      return loadStep(action.index, state.speed);
    case "reset":
      return loadStep(state.stepIndex, state.speed);
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
  const [state, dispatch] = useReducer(reducer, undefined, () => loadStep(0));
  const step = baseSteps[state.stepIndex] ?? baseSteps[0]!;
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
              className={link.available ? "chapter current" : "chapter soon"}
              disabled={!link.available}
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
            Chapter 1, step {state.stepIndex + 1} of {baseSteps.length}
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
              disabled={state.stepIndex >= baseSteps.length - 1}
            >
              Next
            </button>
          </div>
          <p className="later">
            Later chapters add PreVote, CheckQuorum, leases, ReadIndex, and the Synkrolyn improvements.
            Those switches are off for this tour.
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
          <Stage sim={state.sim} tickMs={tickMs} onToggleNode={(id) => dispatch({ type: "toggle-node", id })} />
        </section>
      </main>
    </div>
  );
}
