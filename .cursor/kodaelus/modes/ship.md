# Ship mode (7)

### Ship mode (7)

`use kodaelus ship` extends Prepare: inventory versus HEAD, full suite, max 3 fix-rerun cycles, then commit, push, open or reuse a PR, and report CI. Prepare still never commits. Ship is the IDE path that matches the cloud/SDK permission to commit.

**Task pack:** Read `policy-manifest.json` (`modes.ship.always`). Clarifying questions are in `core.md`. Do not open `modes/main.md`.

- Same pre-commit gate as Prepare. On Ready, Ship may run the git/gh allowlist below. Other modes stay git-read-only.
- Allow: `git status`, `diff`, `log`, `branch`, `add`, `commit`, `push` (including `git push -u origin HEAD`), `git checkout -b`, `git switch -c`, `gh pr create`, `gh pr view`, `gh pr checks`, `gh pr status`, `gh run list`, `gh run view`.
- Deny: force push, `reset --hard`, `rebase -i`, `git config`, `gh pr merge`, and any push whose ref is `main` or `master`.
- If the current branch is `main` or `master`, create and switch to a new branch before commit. Never push to `main` or `master` unless the user text in this task explicitly says to.
- Fix-and-rerun soft cap is 3 cycles. After 3, stop, do not claim Ready, and do not commit. Hooks deny further product edits until **`ship continue`** or **`prepare continue`**.
- After a PR exists and CI is fail: pull the log with `gh run view` or `gh pr checks`, write a minimal fix Plan, implement under the Ship allowlist, re-run the local suite, commit and push again, and re-poll. That path allows 3 product fixes. Then stop, report Not ready, and include Follow-Up Queue until **`ship ci continue`**. `ship continue` does not unlock this cap.
- When CI is pending, poll at most 4 times. Backoff between later polls is 15s, then 30s, then 60s. After the fourth pending result, report pending and Follow-Up Queue. Do not poll forever.
- Before the inventory, state `Active mode: Ship` and, when the ask was natural language, the `recommendKodaelusIntent` and `recommendCeremony` lines. Both say they do not switch mode.
- Read `.kodaelus/insights.md` and `## Preferences` when present before the verdict.
- Secrets guard still applies. Never force-push. Do not auto-merge.

**Workflow:** change inventory; full suite (`npm test`, plus `cd sdk && npm test` when SDK surface changed); fix-and-rerun up to 3; on green, commit with the proposed message; push the current branch and set upstream if needed; `gh pr create` when no PR exists, otherwise reuse `gh pr view`; poll checks. On fail, repair within 3 product cycles, then re-poll. On pending, poll with the backoff above. Report pass, fail, or pending with evidence and put the PR URL in the delivery. Ship is not an open-ended babysitter.

## Phase boundary

Follow the phase handoff in `core.md`. Ship does not restate it.

## Ship response structure

1. **Understanding.** Pending change versus `HEAD`.
2. **Change inventory.**
3. **CI/CD review.**
4. **Tests.** Commands, outcomes, fix-loop count (0 - 3).
5. **Delivery Self-Check.** Emit `## Delivery Self-Check` from the ship fragment.
6. **Verdict.** Ready or Not ready.
7. **Commit.** Hash when Ready. Omit when Not ready.
8. **Push.** Remote and branch when Ready.
9. **PR.** URL. Create or reuse.
10. **CI.** pass, fail, or pending, with the command output and the repair or poll count.
11. **Follow-Up Queue.** Required when Not ready, CI is still fail, CI is pending past the poll limit, or the CI-repair cap is reached. Omit when Ready and CI is green. Residual flakes may go here.

<!-- kodaelus:include fragments/delivery-self-check.md#ship -->
IDE: Read fragments/delivery-self-check.md#ship when this marker is present.
