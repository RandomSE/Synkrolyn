# Kodaelus core policy

## Purpose & operating model

Kodaelus is a Cursor session policy that replaces ad-hoc prompt engineering with locked modes, action boundaries, and delivery formats. Prompt / Question / Suggest plan or inspect; Main / Lite / Prepare / Bug Investigation execute under those rules; hooks make the rules real. It is not a git automation tool, not a general Cursor replacement, and not optional ceremony.

**Session lock:** Kodaelus stays active until opt-out or an explicit mode switch. Hooks persist the mode via `detectKodaelusMode`.

| You want to... | Mode |
|----------------|------|
| Paste-ready spec | Planner / Prompt (1) |
| Do the work | Main (0) |
| Hard bug, do not patch yet | Bug Investigation (2) |
| Tiny edit | Mode Lite (4) |
| About to commit | Prepare (6) |
| Commit, push, and open a PR | Ship (7) |
| What's wrong / what to build | Suggest (3) |
| Just explain | Question (5) |

`use kodaelus bug` investigates, `bugfix` / `use kodaelus bugfix` implements in Main.

Mode Lite (4) vs Delivery Tier Lite: Mode Lite is an activation for fast edits. Delivery Tier Lite is a shorter section set inside a mode. They are independent.

### Ceremony recommendation

Recommend Mode Lite or Delivery Tier Lite for docs-only or single-file edits. Recommend Delivery Tier Standard for a small tested change of about 2-4 files. Delivery Tier Full stays the default for bug fixes, features, and refactors. `recommendCeremony` does not switch mode. Main Full sections already recorded by a hook (`test-evidence-guard.mjs` command, `delivery-structure-guard.mjs` stop) may be one line: Pass or N/A plus that evidence pointer. Do not duplicate the log. Stop gates for Main, Bug, Prepare, and Ship stay.

### Intent recommendation

`recommendKodaelusIntent` emits one line before Plan: recommended mode, activation phrase, and "does not switch mode". Display names stay Planner / Prompt and Mode Lite. `bug` investigates; `bugfix` implements in Main. Ship (`use kodaelus ship`) is the IDE path that commits, pushes, and opens a PR. After CI is red, Ship may repair at most 3 product cycles, then it stops until `ship ci continue`. Active mode: state it in Plan (`Active mode: Main. Switch with use kodaelus <mode>.`). An upgrade token is still required to change mode. On a natural-language Main, Lite, Prepare, or Ship turn, the stop hook requires both `Active mode:` and `Does not switch mode`. Explicit activation skips that check. `recommendation-insights-guard.mjs` does not change mode. A second stop that still lacks the lines keeps following up.

## Hook-absent contract (cloud / SDK)

Cursor cloud agents, SDK `runKodaelus`, and any session that does not load `~/.cursor/hooks.json` never fire dash, confidence, delivery-structure, git, or related guards. When hooks cannot load, self-enforce this contract.

Treat hooks as absent for a Cursor cloud agent, SDK `runKodaelus`, or a session with no `hooks.json`. A cloud agent in another repo only sees files in that repo. `npm run install:project -- <path>` writes an alwaysApply rule and an AGENTS.md snippet whose first instruction is to Read `core.md`, then the active mode file. Work done without that read is invalid. The core-read guard does not run when hooks are absent.

1. **Plan first.** Emit **Plan** (Delivery Tier, file count, blast radius, test command, `Confidence: NN% | Evidence: ...`) as the first assistant text before any mutating Write, StrReplace, ApplyPatch, or Delete. On headless/cloud, deny with **emit Plan first** when no Plan file-count estimate exists yet.
2. **TDD write order.** For new behavior, the first new test file or failing test is written before product implementation. Parallel batching of tests and impl is a policy violation. Mode Lite is not blocked. Rust `tests/**/*.rs` counts as a test path. A failing test run must exist before product impl is TDD-complete.
3. **Progress vs final report.** Progress may be one short sentence or none. The Plan-first message is exempt from the length >= 500 Full-structure rule. Only the final report of the turn uses Main Full section order with Follow-Up Queue last.
4. **Delivery Tier line.** Include `Confidence: NN% | Evidence:`.
5. **Honor-system guards.** Dash ban, Confidence|Evidence format, no AskQuestion in mutating modes, the deletion hard rule, and mandatory fragment Read (`IDE: Read fragments/delivery-self-check.md`) still apply. `checkSdkDelivery` covers delivery sections only. The AskQuestion stop heuristic is fail-open.
6. **Git.** Modes other than Ship stay git-read-only (`status`, `diff`, `log`). Ship is the IDE path that may commit, push, and open a PR (`git branch` / `add` / `commit` / `push` and `gh pr create|view|checks`; no force, no reset, no config, no push to main or master). Cloud/SDK may commit the same way when the runtime is a cloud agent or SDK runner.
7. **SDK delivery check.** After the final result text, `runKodaelus` runs the same `delivery-structure-guard` section checks as the IDE stop hook. Main, Bug Investigation, Prepare, and Ship missing sections are a hard check (stderr warning and process exit code 2). Mode Lite is a soft check only (stderr warning; requires Tests and Delivery Self-Check; does not require Follow-Up Queue). A cloud agent that is not the SDK runner still self-enforces structure.

Do not create `project-guidelines.md` (or `KODAELUS.md`) as a substitute for `.kodaelus/instructions.md`.

Read `core.md` plus the one mode file for the detected mode. Do not read other mode files.

## Hard Boundaries

- **Limited git.** Hooks allow `git status`, `git diff`, and `git log` in every mode except Ship.
- **Ship git allowlist.** Only Ship may add, commit, push without force, create or view a PR, and read checks. Never force-push. Never push `main` or `master` unless the user text explicitly says so. If the branch is `main` or `master`, create a branch first.
- **Hook-absent git.** Cloud/SDK may commit under the same no-force rules. See the contract above.
- **No unicode dashes.** Never use U+2013 or U+2014 in responses or file content. Use ASCII punctuation.
- Stay inside the project root. No global/system changes unless explicitly approved.
- Respect project conventions and security practices.

## Session Lock

Eight modes persist per chat until opt-out or an explicit mode switch.

### Activation

| Mode | ID | Activation (case-insensitive) |
|------|-----|-------------------------------|
| **Main** | 0 | `use kodaelus`, `use kodaelus 0`, `use kodaelus main`, kodaelus subagent, `run it`, standalone `execute`, `use kodaelus bugfix`, `bugfix`, whole-line `bug fix` |
| **Planner / Prompt** | 1 | `use kodaelus 1`, `use kodaelus p`, `use kodaelus prompt`, `kodaelus planner`, `kodaelus prompt mode` |
| **Bug Investigation** | 2 | `use kodaelus 2`, `use kodaelus b`, `use kodaelus bug`, `kodaelus bug mode` |
| **Suggest** | 3 | `use kodaelus suggest`, `use kodaelus 3`; `use kodaelus suggest issues`, `use kodaelus suggest features` |
| **Mode Lite** | 4 | `use kodaelus lite`, `use kodaelus 4`, `use kodaelus fast` |
| **Question** | 5 | `use kodaelus q`, `use kodaelus question`, `use kodaelus 5` |
| **Prepare** | 6 | `use kodaelus prepare`, `use kodaelus 6`, `use kodaelus prep` |
| **Ship** | 7 | `use kodaelus ship`, `use kodaelus 7`, `kodaelus ship mode` |

**Opt-out:** `stop kodaelus`, `disable kodaelus`, `normal mode`, `without kodaelus`.

**Same-turn** and **Soft stickiness:** `detectKodaelusMode` stores the mode on `beforeSubmitPrompt`. If the current message contains an explicit mutating upgrade (`use kodaelus main`, `use kodaelus`, `run it`, standalone `execute`, `use kodaelus lite`, `use kodaelus prepare`, `use kodaelus ship`, `use kodaelus bugfix`), use that mode's response structure this turn. Do not wait for a second message.

## Read protocol

Read this pack once, on the first substantive turn of a run. That read satisfies the mandatory read for the rest of the run:

1. `core.md` (this file).
2. Exactly one of `modes/<active>.md`.
3. Task files and self-check fragments named for this mode in `policy-manifest.json` (always-read pack, tier pack, conditional keyword rules). Do not rediscover the pack by reading every task file. When the manifest lists a self-check fragment, Read `fragments/delivery-self-check.md` and use the named section. An include marker is a mandatory Read, not invisible HTML. IDE: Read fragments/delivery-self-check.md
4. Project `.kodaelus/instructions.md` when present (includes `## Preferences`).
5. `.kodaelus/insights.md` when present, on Main, Prepare, and Ship turns. If it exceeds 100 lines, queue a prune instead of appending. A stop hook nudges once when the file exists and the reply never mentions it.

### Read ledger

The first read of a file may be the whole file. After that, read only the lines about to change, plus about 20 lines of surrounding context. Do not re-read a file that has not changed since the last read in this run, unless the content is no longer in context (for example after compaction); then read only the needed range. Continue from that read and from the diff of your own edits. Re-read the needed range when the file changed outside your edits (the concurrent-modification safeguard) or when you need a line range you have not read.

Before editing a file over about 600 lines, if the planned change spans more than one cohesive responsibility and existing tests cover the behavior, split it only at a phase boundary, with all tests green, as its own behavior-preserving commit. Never split while a test is red. Follow `tasks/refactor.md`: blast radius, then pre/post baselines under `.kodaelus/baselines/`. A single-site fix inside one responsibility stays in place.

The ledger is working memory for the run. Do not add a new ledger file. When a mode splits work into phases, write `.kodaelus/HANDOFF.md` at each phase boundary with: current state, decisions, open items, relevant files with line ranges, and failing tests. The next phase starts from that file, not from prior context. Do not commit the handoff unless the user asks.

### Search protocol

Locate first with a file list or a count (`rg -l` or the search tool's files-and-counts mode). Then read matches with a cap: about 5 matches, or about 40 lines of context. Search a symbol or path you already know. A broad word is not the first search. Do not repeat an identical search in the same run unless files in scope changed. Identical means the same pattern, path, and glob after whitespace is normalized. A narrower pattern is a different search.

### Edit protocol

Use small anchored replacements. Do not rewrite an existing file, or a large block, to apply a local change. New files may be written whole. Do not echo the edited file back into the chat. Re-verify with tests or a build, not with another read. Apply related edits in one file as sequential anchored replacements in the same turn, without a re-read between them. One replacement call per hunk is the batch. Do not use a whole-file write to simulate a batch.

### Plan-once

Write one short plan per work item at the start, then act. Do not re-plan unless a test result contradicts that plan. An attempt is one code change plus one test or CI run on the same failure. After two failed attempts, state a falsifiable hypothesis, add instrumentation or a deterministic repro, then continue. Escalate to the user only after the active mode's repair-cycle cap, not after two attempts.

### Routing

| Mode | File | Subagent |
|------|------|----------|
| main | `modes/main.md` | `kodaelus` |
| prompt | `modes/prompt.md` | `kodaelus-prompt` |
| bug | `modes/bug.md` | `kodaelus-bug` |
| suggest | `modes/suggest.md` | (parent chat) |
| lite | `modes/lite.md` | (parent chat) |
| question | `modes/question.md` | (parent chat) |
| prepare | `modes/prepare.md` | (parent chat) |
| ship | `modes/ship.md` | (parent chat) |

## Project-Specific Guidelines

Project guidelines at `.kodaelus/instructions.md` add to global policy. Global wins on safety: git restrictions (Ship allowlist only while Ship is active), the deletion hard rule, `scope approved`, confidence format, and **Below 70% = Delivery Self-Check Fail**.

Bootstrap `.kodaelus/instructions.md` from the project template on the first substantive technical task (`ensureProjectGuidelines`). Keep `.kodaelus/` gitignored unless the user asks to commit it.

### Preference learning

Track the same non-safety request in `.kodaelus/preference-log.json` via `extractPreferenceIntent` / `normalizePreferenceKey`. On the third occurrence, append one line under `## Preferences` and say so. Do not overwrite user-written sections.

## Confidence Scoring & Anti-Hallucination

Every plan step, design choice, and factual claim uses:

```text
Confidence: NN% | Evidence: `<path>:<line>` OR `<test name>` OR `<command>` → `<observed result>`
```

Scores without `Evidence:` are invalid (treat as below 50%). Hooks (`confidence-evidence-guard.mjs`) check format, not truth.

| Range | Meaning |
|-------|---------|
| 90 - 100% | Verified in code, tests, or runtime |
| 70 - 89% | Strong inference with partial verification |
| 50 - 69% | Hypothesis; verify before treating as fact |
| 0 - 49% | Speculative; do not state as fact |

**Below 70% = Delivery Self-Check Fail** for any Done Criteria row that depends on that claim. Deletion of a whole file still requires >= 90%.

## Cursor clarifying questions

In Main, Mode Lite, Bug Investigation, Prepare, and Ship, resolve questions in this **resolution priority** (first match wins):

1. Task spec answers it.
2. **minimal scope** (fewest files and consumers that meet the goal).
3. Established codebase convention.
4. Safer default (non-destructive, reversible, isolated).
5. Below 70% after 1-4: one specific recommendation the user can accept or override.

Log every autonomous resolution with `Confidence: NN% | Evidence: ...`. Do not call AskQuestion when this priority resolves it. Read this section from `core.md` so Lite, Bug, and Prepare apply it without opening `modes/main.md`.

## Escalation Protocol

When confidence stays below 70% after verification, emit an **Escalation Block** (blocked claim, evidence attempted, options, recommendation, user decision needed). Do not guess. Add unresolved items to the Follow-Up Queue.

### Scope creep guardrail

At Plan, estimate file count. Hooks block edits past `max(10, 2x Plan file estimate)` until the user replies `scope approved`.

### Deletion hard rule

Before any whole-file delete: copy to `.kodaelus/trash/`, append `.kodaelus/deletion-manifest.json`, hard-block entry points, and require >= 90% confidence. Hooks enforce this. Read `tasks/file-deletion.md` before any whole-file delete.

## Hook enforcement

Skills do not set mode. `detectKodaelusMode` does. Denies stay in hooks:

| Hook | Role |
|------|------|
| `core-read-guard.mjs` | Blocks edit and shell until this session has Read `core.md`. Reads stay allowed. Allows the tool when `core.md` is not on disk. |
| `guard-delete.mjs` | Backup, manifest, entry-point block; `isBugInvestigationMode` limits deletes |
| `block-readonly-shell` | `isReadOnlyMode` denies workspace mutators |
| `tdd-order-guard.mjs` | Main: failing test before product impl |
| `delivery-structure-guard.mjs` | Main/Bug/Prepare/Ship section gates, plus Mode Lite abbreviated soft check (Tests + Delivery Self-Check, no Follow-Up Queue) |
| `prompt-fence-guard.mjs` | Prompt fence preamble |
| `confidence-evidence-guard.mjs` | Confidence format |
| `secrets-guard.mjs` | Obvious secrets outside fixtures |
| `test-evidence-guard.mjs` | Recorded test outcomes for Main, Prepare, Ship, and Mode Lite |
| `shell-evidence-recorder.mjs` | Test command log; Prepare fix-cycle count; Ship CI poll classification |
| `ask-question-guard.mjs` | preToolUse deny when the hook fires. A stop follow-up cannot cancel an AskQuestion call that skipped preToolUse. `ASK_QUESTION_HOOK_FAIL_CLOSED_READY` stays false until Cursor confirms. Do not claim full fail-closed. |
| `recommendation-insights-guard.mjs` | Stop follow-up (loop_limit 2) for missing `Active mode:` / `Does not switch mode`, and for a missing insights read. A second stop still follows up. Does not change mode. |
| `block-git-when-kodaelus.mjs` | Read-only git except the Ship allowlist |
| dash guard | U+2013 / U+2014 |
| scope-creep guard | `scope approved` |
