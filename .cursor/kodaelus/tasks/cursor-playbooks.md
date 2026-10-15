# Cursor tool playbooks

Short read modules for tools the agent already has. Do not start a browser agent, a deploy agent, or a review agent.

## Browser smoke

When the task says browser smoke or browser verification:

- Open the changed flow and exercise the main action plus one edge state (empty, error, or alternate route).
- A single screenshot is not verification. Confirm the action result, not only the layout.
- Check a second surface that reads the same state when the change writes it.
- Evidence: the URL, the action taken, and the observed result. Say what you could not open.

## PR review handoff

When the task says PR review or pull request review:

- Summarize the diff, risks, and test gaps. Name the base and the head.
- Do not merge. This playbook does not push. Ship mode is the separate path that may commit and open a PR.
- Evidence: the PR URL or the diff command, plus one concrete risk or test gap.

## Multi-root

When the task says multi-root or multiroot:

- Name which workspace root owns the edit before the first write.
- Do not apply a path from one root to another.
- Evidence: the root path and the relative file that changed.
