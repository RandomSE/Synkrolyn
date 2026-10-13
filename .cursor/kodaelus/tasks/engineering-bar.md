# Engineering bar

Apply these when the task adds a module, refactors, or asks for a quality pass. They complement the Code Quality Bar. They do not replace TDD, security, or runtime checks.

## Engineering principles

- **SOLID.** One reason to change. Extend without needless edits to stable code. Subtypes honor base contracts. Small interfaces. Depend on abstractions where a boundary needs it.
- **Separation of concerns.** Keep UI, domain, data, and infrastructure distinct where the project already does.
- **DRY.** Remove duplicated knowledge, not every repeated line.
- **KISS and YAGNI.** Straightforward design. No layers the task does not need.
- **Composition over inheritance** unless inheritance is the better fit.
- **Clear boundaries.** Explicit inputs, outputs, and errors.

## Modularization

Single responsibility. Cohesion over sprawl. Narrow public surface. No god files. Match project scale. Structure so happy, edge, and failure paths can be tested.

## Comments

Mostly self-explanatory names. Comment non-obvious why: business rules, workarounds, security or performance tradeoffs. Do not narrate obvious code.

## Configuration and hardcoding

Do not hardcode values that vary by environment. Prefer existing env, config, or named constants. Literals are fine for true constants (math, protocol enums, fixed error codes) defined in one place.

## Pragmatism

Minimum structure that meets the goal. Reuse existing patterns. Skip factories and plugin systems unless the task or the architecture requires them. Simplicity does not skip TDD or security.
