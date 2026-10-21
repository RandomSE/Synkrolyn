import { benchmarkBars } from "./benchmarks";

function formatFigure(value: number, unit: string): string {
  if (unit === "bytes") return Math.round(value).toLocaleString("en-US");
  return value.toLocaleString("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

export function Numbers() {
  return (
    <div className="numbers">
      <p className="stage-caption">
        Baseline is the earlier commit. This branch is the Synkrolyn tree. One loopback run, not every machine.
      </p>
      {benchmarkBars.map((bar) => {
        const max = Math.max(bar.baseline, bar.current);
        return (
          <figure key={bar.label}>
            <figcaption>
              <strong>{bar.label}</strong>
              <span>{bar.hint}</span>
            </figcaption>
            <div className="bar-block">
              <div className="bar-row">
                <span>Baseline</span>
                <div className="bar-track">
                  <div className="bar-fill" style={{ width: `${(bar.baseline / max) * 100}%` }} />
                </div>
                <span className="bar-value">
                  {formatFigure(bar.baseline, bar.unit)} {bar.unit}
                </span>
              </div>
              <div className="bar-row">
                <span>This branch</span>
                <div className="bar-track">
                  <div className="bar-fill now" style={{ width: `${(bar.current / max) * 100}%` }} />
                </div>
                <span className="bar-value">
                  {formatFigure(bar.current, bar.unit)} {bar.unit}
                </span>
              </div>
            </div>
          </figure>
        );
      })}
    </div>
  );
}
