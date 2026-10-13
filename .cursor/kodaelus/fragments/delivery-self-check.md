<!-- kodaelus:self-check main -->

## Delivery Self-Check

Before **Outcome Validation** (Full and Standard tiers), emit a mandatory **Delivery Self-Check** table mapping **Done Criteria** to evidence:

| Done Criterion | Evidence (command, file path, test name) | Result (Pass / Fail / N/A) |
|----------------|------------------------------------------|----------------------------|

**Do not claim complete if any applicable row is Fail.** A row is **Fail** when its supporting claims are below **70%** confidence or lack valid inline **Evidence:**. Rework, escalate, or obtain user acceptance before Outcome Validation.

**TDD write order:** If new behavior was implemented by batching tests and product impl in the same write step, that row is **Fail**. Parallel batching is a policy violation even when tests later pass. If the first test command was already green and no earlier failing run was recorded, that row is **Fail**.

<!-- kodaelus:self-check bug -->

## Delivery Self-Check

Emit a **Delivery Self-Check** table for the investigation. Bug Investigation has no Outcome Validation section. Leave that section out.

| Done Criterion | Evidence (command, file path, test name) | Result (Pass / Fail / N/A) |
|----------------|------------------------------------------|----------------------------|

**Do not claim the investigation complete if any applicable row is Fail.** A row is **Fail** when its supporting claims are below **70%** confidence or lack valid inline **Evidence:**.

Applicable rows: dossier path, repro or instrumentation diff, ranked hypotheses with confidence, Follow-Up Queue is the final section. A product fix is N/A.

<!-- kodaelus:self-check prepare -->

## Delivery Self-Check

Emit a **Delivery Self-Check** table for the prepare gate. There is no greenfield TDD write-order row.

| Done Criterion | Evidence (command, file path, test name) | Result (Pass / Fail / N/A) |
|----------------|------------------------------------------|----------------------------|

**Do not claim Ready if any applicable row is Fail.** A row is **Fail** when its supporting claims are below **70%** confidence or lack valid inline **Evidence:**.

Applicable rows: change inventory versus HEAD, full suite command and outcome, Ready or Not ready, proposed commit message only when Ready. Follow-Up Queue is required when Not ready and omitted when Ready.

<!-- kodaelus:self-check ship -->

## Delivery Self-Check

Emit a **Delivery Self-Check** table for the ship gate. There is no greenfield TDD write-order row.

| Done Criterion | Evidence (command, file path, test name) | Result (Pass / Fail / N/A) |
|----------------|------------------------------------------|----------------------------|

**Do not claim Ready if any applicable row is Fail.** A row is **Fail** when its supporting claims are below **70%** confidence or lack valid inline **Evidence:**.

Applicable rows: change inventory versus HEAD, full suite command and outcome, Ready or Not ready, commit, push, PR link, CI status, CI repair within 3 cycles or `ship ci continue`. Follow-Up Queue is required when Not ready, CI failed, CI pending past the poll limit, or the CI-repair cap is reached, and omitted when Ready and CI is green.

<!-- kodaelus:self-check lite -->

## Delivery Self-Check

Abbreviated table for Mode Lite. Follow-Up Queue stays omitted unless the change is non-trivial.

| Done Criterion | Evidence (command, file path, test name) | Result (Pass / Fail / N/A) |
|----------------|------------------------------------------|----------------------------|

**Do not claim complete if any applicable row is Fail.** Keep the table to two rows: targeted Tests outcome, and the change matches the request.
