# File Deletion Protocol

## File Deletion Protocol

Whole-file deletion is irreversible by default while git is read-only. Stricter than in-file cleanup.

### Pre-delete backup

1. Copy the file to `.kodaelus/trash/<ISO-timestamp>/<original-relative-path>`.
2. Create `.kodaelus/trash/` if missing.
3. Ensure `.kodaelus/` is in `.gitignore` when the project uses git.

**Hook enforcement (Main/Lite/Prepare):** Delete and shell `rm` / `del` / `Remove-Item` cannot proceed without trash backup and a manifest append (`guard-delete.mjs`). Still emit the **File Deletions** section citing the manifest.

| Field | Content |
|-------|---------|
| Path | Original relative path |
| Reason | Why it is dead or superseded |
| Confidence | NN% that deletion is safe |
| Backup | Path under `.kodaelus/trash/` |
| Timestamp | ISO-8601 |
| Entry-point check | Pass / BLOCKED |

Append each entry to `.kodaelus/deletion-manifest.json`.

### Restore

On `restore <file>` or `undo last delete`: read the manifest (not chat history), copy the backup back, and confirm. No git.

### Entry-point detection

Hard-block deletion of an entry point regardless of confidence. Check at minimum:

- `package.json` `main`, `bin`, `exports`, `scripts`
- Python `if __name__ == "__main__"`, `pyproject.toml` / `setup.py`, `__main__.py`
- `Procfile`, `Dockerfile` `CMD` / `ENTRYPOINT` / `COPY`
- CI workflows that invoke the file
- README or docs run instructions
- `Makefile`, `justfile` targets used by CI, scripts, or docs
- Bundler configs: `vite.config`, `webpack.config`, `rollup.config`
- Monorepo runners: `turbo.json`, `nx.json`
- `scripts/*.sh` invoked by CI or `package.json`
- Framework conventions (`app.py`, `main.ts`, `index.js`) when tooling references them

If blocked, record BLOCKED and propose deprecation, re-export, or a move. Do not retry a hook block.

### Confidence

Whole-file deletion requires >= 90% confidence. Below that, investigate or ask. Grep project-wide for imports, dynamic references, string paths, and config mentions before calling a file dead.

### In-file vs whole-file

In-file dead code (unused imports, unreachable branches) needs no backup. Whole-file deletes use this protocol.
