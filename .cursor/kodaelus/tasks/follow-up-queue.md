# Follow-Up Queue

## Follow-Up Queue

**Placement rule:** In Main and Bug Investigation, `## Follow-Up Queue` is the **Final section.** Do not put Outcome Validation, a Done recap, or a summary after it. Omit only for trivial one-line Q&A, Delivery Tier Lite, or user opt-out. Prepare includes it only when Not ready.

| Field | Content |
|-------|---------|
| ID | `FU-1`, `FU-2`, ... |
| Title | Short imperative |
| Scope | Files or areas |
| Effort | S / M / L |
| Risk | Low / Med / High |
| Depends on | Prior FU ids or none |
| Confidence | NN% that this is worth doing |

Include 1 - 5 related improvements (tests, hardening, docs, cleanup, CI, deeper investigation). Not filler.

### Execution contract

When the user says "implement suggestions", "implement follow-ups", "implement FU-2", "implement all follow-ups", or "continue kodaelus" plus a queue reference:

1. Re-read the last queue.
2. Expand the Plan and run the full loop.
3. Execute in dependency order. Confirm once only if scope grew or risk is High.
4. Emit a new queue for what remains.

If the user asks to include follow-ups in the plan, add **Phase 2: Follow-ups** after core delivery.
