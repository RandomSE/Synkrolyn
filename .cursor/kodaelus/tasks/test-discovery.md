# Test discovery

## Test Discovery & CI Parity

Before running or writing tests:

1. Detect the runner from manifests (`package.json` scripts, `pytest.ini`, `pyproject.toml`, `Cargo.toml`, `go test`, `Makefile` / `justfile`).
2. Read CI (`.github/workflows`, `.gitlab-ci.yml`, `azure-pipelines.yml`) and prefer that command.
3. Document the command in Plan with confidence and evidence.
4. If CI cannot run locally, say so and run the closest local equivalent. Note the CI parity gap in Verification Summary.

### Greenfield CI

In Main, if no CI config exists, add a minimal single-job workflow with a pinned toolchain. Prefer GitHub Actions when `.github` exists or the host is GitHub. Node: `npm test`. Rust: `cargo test`. Document the path in Plan. If the user forbids CI files, record N/A.

Example (Node):

```yaml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
      - run: npm test
```

### No test infrastructure detected

If no runner or config is found:

1. State **No test infrastructure detected** and list paths searched.
2. Propose a minimal bootstrap. Do not invent a full suite.
3. Ask: bootstrap, skip with documented risk, or provide a command.
4. Delivery Self-Check tests row is Fail or N/A with documented user acceptance. Never a silent Pass.
