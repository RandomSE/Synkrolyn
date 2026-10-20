import type { Message, NodeState, Sim } from "../sim/raft";

type StageProps = {
  sim: Sim;
  tickMs: number;
  onToggleNode: (id: number) => void;
};

const roleLabel: Record<NodeState["role"], string> = {
  follower: "follower",
  candidate: "candidate",
  leader: "leader",
};

export function Stage({ sim, tickMs, onToggleNode }: StageProps) {
  const leaders = sim.nodes.filter((node) => node.role === "leader" && !node.crashed);

  return (
    <div className="stage-wrap">
      <div className="stage" style={{ ["--tick-ms" as string]: `${tickMs}ms` }}>
        <svg className="wires" viewBox="0 0 100 100" aria-hidden="true">
          {sim.partitions && <PartitionCut sim={sim} />}
        </svg>
        {sim.messages.map((message) => (
          <MessageDot key={message.id} message={message} count={sim.nodes.length} />
        ))}
        {sim.nodes.map((node) => {
          const point = nodePosition(node.id, sim.nodes.length);
          const status = node.crashed ? "down" : node.role;
          return (
            <button
              key={node.id}
              type="button"
              className={`node ${status}`}
              style={{ left: `${point.x}%`, top: `${point.y}%` }}
              onClick={() => onToggleNode(node.id)}
              aria-label={
                node.crashed
                  ? `Server ${node.id + 1} is down. Click to start it.`
                  : `Server ${node.id + 1} is a ${roleLabel[node.role]}. Click to stop it.`
              }
            >
              <span className="node-id">S{node.id + 1}</span>
              <span className="node-role">{node.crashed ? "down" : roleLabel[node.role]}</span>
              <span className="log-strip" aria-hidden="true">
                {node.log.length === 0 && <span className="log-empty">no notes</span>}
                {node.log.map((entry, index) => (
                  <span
                    key={`${entry.term}-${entry.command}-${index}`}
                    className={index < node.commitIndex ? "cell committed" : "cell draft"}
                    title={entry.command}
                  />
                ))}
              </span>
            </button>
          );
        })}
      </div>
      <p className="stage-caption">
        {leaders.length === 0 && "No leader yet. The servers are waiting or asking for support."}
        {leaders.length === 1 && `Server ${leaders[0]!.id + 1} is the leader.`}
        {leaders.length > 1 &&
          "Two leaders right now. Only a side with a majority can make a new note permanent."}
        {sim.partitions && " A network cut is in place."}
      </p>
      <ul className="legend">
        <li><i className="swatch follower" /> follower</li>
        <li><i className="swatch candidate" /> candidate</li>
        <li><i className="swatch leader" /> leader</li>
        <li><i className="swatch down" /> down</li>
        <li><i className="swatch draft" /> draft note</li>
        <li><i className="swatch committed" /> permanent note</li>
      </ul>
    </div>
  );
}

function MessageDot({ message, count }: { message: Message; count: number }) {
  const from = nodePosition(message.from, count);
  const to = nodePosition(message.to, count);
  const progress = 1 - message.travel / message.totalTravel;
  const x = from.x + (to.x - from.x) * progress;
  const y = from.y + (to.y - from.y) * progress;
  const carrying = message.entries.length > 0;
  const vote = message.kind === "requestVote" || message.kind === "voteReply";
  return (
    <span
      className={`dot ${vote ? "vote" : "append"} ${carrying ? "carrying" : ""}`}
      style={{ left: `${x}%`, top: `${y}%` }}
    />
  );
}

function PartitionCut({ sim }: { sim: Sim }) {
  const groups = sim.partitions ?? [];
  if (groups.length < 2) return null;
  const left = centroid(groups[0] ?? [], sim.nodes.length);
  const right = centroid(groups[1] ?? [], sim.nodes.length);
  const midX = (left.x + right.x) / 2;
  const midY = (left.y + right.y) / 2;
  const dx = right.x - left.x;
  const dy = right.y - left.y;
  const length = Math.hypot(dx, dy) || 1;
  const nx = -dy / length;
  const ny = dx / length;
  const span = 28;
  return (
    <line
      className="cut"
      x1={midX - nx * span}
      y1={midY - ny * span}
      x2={midX + nx * span}
      y2={midY + ny * span}
    />
  );
}

function centroid(ids: number[], count: number): { x: number; y: number } {
  if (ids.length === 0) return { x: 50, y: 50 };
  const points = ids.map((id) => nodePosition(id, count));
  const x = points.reduce((sum, point) => sum + point.x, 0) / points.length;
  const y = points.reduce((sum, point) => sum + point.y, 0) / points.length;
  return { x, y };
}

function nodePosition(index: number, count: number): { x: number; y: number } {
  const angle = -Math.PI / 2 + (index * 2 * Math.PI) / count;
  return {
    x: 50 + Math.cos(angle) * 34,
    y: 47 + Math.sin(angle) * 33,
  };
}
