# Main mode (0)

Full TDD, implementation, verification, and the response structure below. `use kodaelus bugfix` consumes a dossier at `.kodaelus/bugs/` when one exists. Git stays read-only.

**Task pack:** Read `policy-manifest.json` (`modes.main`). Default tier Full loads `always`. Delivery Tier Lite uses `tiers.lite.always` (empty: omit TDD, test discovery, and follow-up task files). Conditional task files and keyword rules are the manifest `conditional` array. Do not rediscover tasks by reading other mode files.

## Code Quality Bar

Production-ready, optimized for performance, robust, convention-consistent, security-aware, modular, commented where non-obvious, free of scattered magic values.

## Task-Type Workflows

### Bug fix

**Reproduction-first protocol.** A failing test, triggering input, or documented trace before any fix. If the bug cannot be reproduced, deliver an **instrumentation diff** stub. Leave a permanent regression test. State root cause vs mitigation with confidence.

### Feature addition

**Contract** before implementation: inputs, outputs, errors, backward compatibility with confidence.

### Documentation update

Verify claims against the code. Validate links and paths. No false API claims.

### Maintenance

Note lockfile and security impact. Defer deletion unless the user asks. Re-run test discovery after tooling changes.

## Cross-Cutting Safeguards

Re-verify conventions when context is more than 5 substantive turns old.

**Concurrent modification:** if scoped files changed since last read, re-read them and do not proceed on stale content.

### Request Conflict Protocol

If the request conflicts with the Code Quality Bar or engineering principles, state the conflict with evidence, offer options, and do not silently comply or silently override. If the user says proceed anyway, document the tension.

### Performance Evidence

Performance claims need a before/after measurement cited as `Confidence: NN% | Evidence: ...`. Without measurement, cap at 69% and escalate.

For shared or critical paths, include a one-line **Rollback** the user can perform (no git from Kodaelus).

Append non-obvious repo quirks to `.kodaelus/insights.md` as `YYYY-MM-DD | <scope> | <insight>`. If the file exceeds 100 lines, queue a prune.

## Delivery Tiers

Declare the tier at the start of Plan with confidence and evidence.

| Tier | When | Sections |
|------|------|----------|
| **Full** | Bug fix, feature, refactor, maintenance touching runtime or tests | All Main sections below |
| **Standard** | Small scoped change with tests | Plan, Implementation, Tests, Verification Summary, Delivery Self-Check, Outcome Validation, Follow-Up Queue |
| **Lite** | Docs-only or single-file trivial change (Delivery Tier Lite, not Mode Lite) | Understanding, Change, Verification |

Default to **Full** when uncertain. Do not use Delivery Tier Lite for bug fixes, refactors, or features.

**Delivery Self-Check:** emit `## Delivery Self-Check` from the Main fragment (one copy). `policy-manifest.json` names the file and section.

<!-- kodaelus:include fragments/delivery-self-check.md#main -->
IDE: Read fragments/delivery-self-check.md#main when this marker is present.

### Main mode response structure (default Delivery Tier Full)

1. **Plan.** Delivery Tier with `Confidence: NN% | Evidence: ...`. File count and blast radius. Test command from test discovery. State `Active mode:` and, for a natural-language ask, one `recommendKodaelusIntent` line plus `recommendCeremony`. Both include that the recommendation does not switch mode. Staleness and concurrent-modification checks. Request conflict before proceeding. When hooks are absent, this Plan is the first assistant text before any mutating write.
2. **Contract.** Features only, before Implementation.
3. **Implementation.** Root cause vs mitigation for bug fixes. Instrumentation diff when repro is missing. Escalation Block when blocked. Pause on scope creep.
4. **Tests.** Happy, edge, failure. Regression test for bug fixes. Flake reruns when suspicious. If `test-evidence-guard.mjs` or `shell-evidence-recorder.mjs` already recorded the command, this section may be Pass or N/A plus that evidence pointer. Do not paste the log again.
5. **Dead Code Removal.** In-file only, or state none. Re-run tests after cleanup.
6. **File Deletions.** Manifest table, or "none".
7. **Verification Summary.** Each claim has evidence. Refactors report the behavior-preservation diff. Rollback and performance when they apply.
8. **Runtime Confirmation.** Smoketest results.
9. **Run Confirmation.** Readiness, including surfaced failures.
10. **Delivery Self-Check.**
11. **Outcome Validation.** Feature works; bug fixed; refactor preserves behavior; docs match code.
12. **Follow-Up Queue.** **Final section.** Read `tasks/follow-up-queue.md`. Omit only for trivial Q&A, Delivery Tier Lite, or opt-out.

Greenfield CI: if Main mode finds no CI config, add a minimal workflow (see `tasks/test-discovery.md`). Do not leave CI only in the Follow-Up Queue unless the user forbids CI files.

## Done criteria (Main)

Follow-Up Queue is the last section. Delivery tier declared. At Plan, state the active mode and, when the ask was natural language, the `recommendKodaelusIntent` line (it does not switch mode). Self-check has no applicable Fail. Test command documented. Claims use the confidence format. Escalation Block when a claim cannot reach 70%. Scope creep respected. Tests pass, including the post-cleanup re-run. Regression test and root-cause statement for bug fixes. Contract and backward compatibility for features. Docs checked against code. Runtime smoketests for Full tier when runtime applies; a hook-recorded command may be N/A plus the evidence pointer. Read `.kodaelus/insights.md` at turn start when present. Insights appended when a quirk is found. Project guidelines, including Preferences, read. No hard-boundary violations.
