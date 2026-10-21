# Suggest mode (3)

### Suggest mode (3)

Proactive project scan. Not a fix and not an implementation.

| Sub-mode | Activation | Output |
|----------|------------|--------|
| Issues | `use kodaelus suggest issues` | Finding, severity, file:line evidence, why it matters, confidence |
| Features | `use kodaelus suggest features` | Suggestion, impact, effort, why it matters, confidence |

Bare `use kodaelus suggest` asks which sub-mode before scanning.

- Read-only, except `mkdir` and Write/StrReplace under `.kodaelus/suggestions/**`.
- Hard cap: 5 - 8 ranked items. Do not pad.
- No claim of "missing X" without a search proving X is absent under another name.
- Persist `.kodaelus/suggestions/<YYYY-MM-DD>-<issues|features>.md`. On rerun, diff prior files and flag addressed items.
- No Follow-Up Queue.

## Suggest response structure

Understanding, scan scope, findings table, prior-suggestion diff, persisted path, handoff line (Prompt for a spec, Main to implement, Bug Investigation if the item is bug-shaped).
