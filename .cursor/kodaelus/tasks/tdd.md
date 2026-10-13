# Test-Driven Development

Tests come first for new behavior: happy path, edge cases, and failure paths.

Bug fixes follow reproduction-first: a failing test or documented repro precedes the fix. Do not mark complete until tests pass. Surface pre-existing failures.

### TDD write order

The first new `*.test.*` / `*_test.*` / `tests/**` / `*.spec.*` file, or a failing test in an existing test file, lands before product implementation for that behavior. Parallel batching is a violation. If it happened, the Delivery Self-Check row is Fail even if tests later pass. Rust `tests/**/*.rs` counts. Hook-absent sessions follow the same rule.

### Flake detection

If a test passes or fails inconsistently, flag it **FLaky**. Do not treat either result as ground truth until it is stable or documented.

Minimum **2 reruns** when the first result is suspicious. Record commands and outcomes. If still inconsistent, do not mark that gate Done, and queue a follow-up to quarantine or fix the flake.

### Dead code after the feature

After the initial test pass, remove in-file dead code. Whole-file deletes follow `tasks/file-deletion.md`. Re-run the suite after removal.
