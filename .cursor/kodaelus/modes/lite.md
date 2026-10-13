# Mode Lite (4)

### Lite mode (4)

`use kodaelus lite` (also `use kodaelus fast`) is the fast path for a very small code change. It is not Delivery Tier Lite.

- Implementation is allowed. Hooks still enforce git, delete backup, entry points, scope creep, and confidence format.
- **Plan:** goal, rough file count, targeted test command.
- **Tests:** targeted tests for touched logic only, not the full suite, unless the targeted run fails or the user asks for CI parity. Emit a `## Tests` heading.
- Omit by default: Dead Code Removal, File Deletions (unless a delete was requested), Runtime Confirmation, baselines unless the task is a refactor, Follow-Up Queue.
- Keep: Implementation, Tests, Verification Summary, abbreviated Delivery Self-Check, Outcome Validation.
- **Task pack:** Read `policy-manifest.json` (`modes.lite`). Clarifying questions are in `core.md`. Do not open `modes/main.md`.
- If the ask is docs-only or one file, Mode Lite is the recommended path (`use kodaelus lite`). That recommendation does not switch mode by itself.
- If the task grows past about 3 files, or needs a delete or refactor, recommend `use kodaelus main`. Delivery Tier Standard fits a small tested change of about 2-4 files inside Main.

## Lite response structure

Short Plan, Implementation, targeted Tests, Verification Summary, abbreviated Delivery Self-Check, Outcome Validation. Stop hooks soft-check `## Tests` and `## Delivery Self-Check` (`loop_limit` on the shared stop hook). They do not require Follow-Up Queue.

<!-- kodaelus:include fragments/delivery-self-check.md#lite -->
IDE: Read fragments/delivery-self-check.md#lite when this marker is present.
