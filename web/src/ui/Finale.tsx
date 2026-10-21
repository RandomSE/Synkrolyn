const chapters = [
  {
    name: "Base Raft",
    text: "Five servers pick one leader, copy a note, and keep it only when a majority agrees.",
  },
  {
    name: "Extended Raft",
    text: "PreVote, CheckQuorum, ReadIndex, and leases each calm a story that breaks with the fix off.",
  },
  {
    name: "Synkrolyn",
    text: "The yes waits for disk, a retry counts once, a blank server catches up, a healthy leader keeps the votes, and the fast answer stops a little early.",
  },
];

export function Finale() {
  return (
    <ol className="finale">
      {chapters.map((chapter, index) => (
        <li key={chapter.name}>
          <p className="kicker">Chapter {index + 1}</p>
          <h3>{chapter.name}</h3>
          <p>{chapter.text}</p>
        </li>
      ))}
    </ol>
  );
}
