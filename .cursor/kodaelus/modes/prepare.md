# Prepare mode (6)

### Prepare mode (6)

`use kodaelus prepare` reviews the working tree against the last commit. Gate regressions, green the full suite, propose a commit message. Do not create the commit.

**Task pack:** Read `policy-manifest.json` (`modes.prepare.always`). Clarifying questions are in `core.md`. Do not open `modes/main.md`.

- May edit code and tests to fix failures introduced by the pending diff.
- Git stays read-only (`status`, `diff`, `log`). Never `git commit`, `git add`, or `gh`.
- Scope: uncommitted and staged changes versus `HEAD`.
- Fix-and-rerun soft cap is 3 cycles (cycle 0 is the first full run). After 3 fix cycles, stop, do not claim Ready, and do not propose a commit message. Hooks deny further product edits until the user replies **`prepare continue`**.
- If CI or test config changed, check coherence and run the CI-parity suite.
- Follow-Up Queue: omit when Ready; required when Not ready.
- Stop gate: Ready vs Not ready (`delivery-structure-guard.mjs`), Delivery Self-Check, and test-evidence when implementation occurred. No greenfield TDD write-order row.
- Read `.kodaelus/insights.md` and `## Preferences` in `.kodaelus/instructions.md` when present before the verdict. Append a non-obvious finding to insights. If insights exceed 100 lines, queue a prune.

**Workflow:** change inventory; regression review; CI/CD guard; full suite (`npm test`, plus `cd sdk && npm test` when SDK surface changed); fix-and-rerun up to 3; on green, propose a commit message matching `git log` style.

## Prepare response structure

1. **Understanding.** Pending change versus `HEAD`.
2. **Change inventory.** Files and risks, with confidence.
3. **CI/CD review.** Pass / Fail / N/A with evidence.
4. **Tests.** Commands, outcomes, fix-loop count (0 - 3).
5. **Delivery Self-Check.** Emit `## Delivery Self-Check` from the prepare fragment.
6. **Prepare verdict.** Ready or Not ready.
7. **Proposed commit message.** Ready only. One fenced block: subject, refined summary, extended summary.
8. **Handoff.** User copies the message and commits locally.
9. **Follow-Up Queue.** Omit when Ready; required when Not ready.

<!-- kodaelus:include fragments/delivery-self-check.md#prepare -->
IDE: Read fragments/delivery-self-check.md#prepare when this marker is present.
