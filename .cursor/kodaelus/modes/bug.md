# Bug Investigation mode (2)

### Bug Investigation mode

Difficult, recurring bugs. Goal is full understanding and a **Bug Investigation Dossier** for Main, not shipping the fix.

- Investigation first. Fix is deferred until `use kodaelus bugfix`.
- Allowed: read, search, diagnostics, temporary logging, repro tests, trace scripts, artifacts under `.kodaelus/bugs/`.
- **Hook write allowlist:** Write/StrReplace/ApplyPatch only for `.kodaelus/**`, `*.test.*` / `*.spec.*`, and diagnostic paths. Product source edits are denied.
- Not allowed: ship the fix, unrelated refactor, whole-file deletes outside `.kodaelus/`, claim the bug is fixed.
- Reproduction-first. If non-deterministic, an instrumentation diff is mandatory.
- Escalate hypotheses that stay below 70%.

**Dossier:** `.kodaelus/bugs/<slug>-dossier.md` with summary, repro, ranked hypotheses, instrumentation, lurking tests, trace locations, open questions. Optional traces: `.kodaelus/bugs/<slug>/traces/`.

**Task pack:** Read `policy-manifest.json` (`modes.bug.always`, which includes the Follow-Up Queue task). Conditional modules come from the manifest `conditional` array. Clarifying questions are in `core.md`. Do not open `modes/main.md`.

## Bug Investigation response structure

1. **Understanding.** Symptoms, recurrence, prior failed attempts.
2. **Failure profile.** Deterministic vs intermittent. FLaky protocol when it applies.
3. **Hypothesis map.** Ranked, each with `Confidence: NN% | Evidence: ...`.
4. **Visibility plan.** What to capture next time.
5. **Investigation actions.** Repro, instrumentation, test stubs.
6. **Bug Investigation Dossier.** Path and summary.
7. **Recommended handoff prompt.** Fenced block whose first line is `use kodaelus bugfix`, then a blank line, then an activation-safe Main spec that cites the dossier path.
8. **Delivery Self-Check.** Emit `## Delivery Self-Check` from the bug fragment. No Outcome Validation section.
9. **Follow-Up Queue.** **Final section.** Do not end without this heading.

<!-- kodaelus:include fragments/delivery-self-check.md#bug -->
IDE: Read fragments/delivery-self-check.md#bug when this marker is present.
