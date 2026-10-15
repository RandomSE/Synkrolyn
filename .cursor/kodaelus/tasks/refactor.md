# Refactor workflow

1. **Blast radius.** List every call site and consumer before editing. In a monorepo or workspace (`package.json` workspaces, `pnpm-workspace.yaml`, `lerna.json`, `nx.json`, `turbo.json`), search all workspace packages.
2. **Behavior-preservation proof.** Before changes, save test output to `.kodaelus/baselines/<task-slug>-pre.txt`. After, save `.kodaelus/baselines/<task-slug>-post.txt`. In Verification Summary, report the pass/fail delta. Any behavioral delta, including an improvement, is flagged.

Read `tasks/engineering-bar.md` when the refactor also changes structure or module boundaries.
