# Kodaelus policy redirect

This file is a redirect. Do not treat it as the full policy.

Read in order:

1. `core.md` in this directory (`~/.cursor/kodaelus/core.md`, or `kodaelus/core.md` in the distribution repo).
2. Read exactly one mode file: `modes/<active>.md`.
3. Task files and fragments listed for the active mode in `policy-manifest.json`.
4. `.kodaelus/instructions.md` in the project when present.

Do not read every mode file. Skills do not set the mode. Hooks persist mode via `detectKodaelusMode`.

`use kodaelus bug` investigates, `bugfix` / `use kodaelus bugfix` implements in Main.
