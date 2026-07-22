# CLAUDE.md — agent workflow

Conventions and review/verification gates for AI-assisted work in this repo.

## Principles

- **Architecture first.** Understand the system before changing it; surface tradeoffs before coding.
- **Minimum code.** Solve the asked problem — no speculative abstractions, configurability, or "while I'm here" cleanups.
- **Surgical edits.** Every changed line must trace to the current task. Don't refactor adjacent code.
- **Own what you ship.** Read every diff. If you can't explain a change in plain language, don't ship it.
- **Verification is non-negotiable.** "Seems right" is never sufficient — tests pass, command output captured.

## Workflow stages

Every non-trivial change moves through six stages, each with a deliverable and a **review gate** before advancing.

| Stage | Deliverable | Review gate (must pass before next stage) |
|-------|------------|---------------|
| **1. Define** | Spec in `claude-docs/specs/<feature>.md` | Objectives, scope, and success criteria unambiguous? Existing spec to extend instead? |
| **2. Plan** | Plan in `claude-docs/plans/<feature>.md` | Tasks atomic? Acceptance criteria observable? Dependencies & risks listed? Open questions named with owner? |
| **3. Build** | Code + tests, one task at a time | Each task's `Verify` command runs; changes stay within the task's scope |
| **4. Verify** | Evidence each acceptance criterion is met | "Seems right" is not sufficient — tests pass, command output captured. No green check without evidence. |
| **5. Review** | Diff + updated plan | Five-axis code review (correctness, readability, architecture, security, performance); plan reflects what shipped |
| **6. Ship** | Commit + (when asked) push/PR | One logical change per commit; message explains *why* |

**Review is not a one-shot at merge time** — review the spec before drafting a plan, the plan before writing code, and the code before committing.

## Update the plan as you go

- Mark a task `✅ DONE` only when *every* acceptance-criteria checkbox is `[x]` with evidence.
- When pre-flight questions get answered, move them out of "Open questions" into a "resolved" subsection (date + answer + impact).
- When implementation reveals new constraints, add a new task rather than silently expanding an existing one.

## Plan format

```
# Plan: <feature> (<spec link>)
**Goal**: <one sentence>     **Constraint**: <e.g. ≤ N lines, no new deps>

## Current state
| Resource | Status |   ← reflects reality, kept current as work proceeds

## Dependency graph
<ASCII showing what blocks what>

## Tasks
### Task N — <title>  <status: ready | NEXT | DONE | blocked>
**What**: …
**Verify**: <command/snippet that proves the task is done>
**Acceptance criteria**:
- [ ] checkbox per observable outcome   ← never tick without evidence

## Open questions
<numbered, with owner; move resolved ones into a "resolved" subsection>

## Done definition
<observable end state>
```

See `claude-docs/plans/subissue_A_run_s1tiling.md` for a working example.

## Project conventions

- **Python entry points**: always `uv run <cmd>` (never bare `python`/`pytest`).
- **Tests**: `tests/unit` and `tests/fixtures`. Run `uv run pytest` before declaring a task done.
- **Don't commit unless asked.** When asked, follow commit hygiene: one logical change per commit, message explains *why*.

## What to ask vs. assume

Ask before:
- writing a new spec, choosing between competing specs, or making an architectural decision the spec doesn't cover
- crossing a module boundary the plan doesn't cover
- modifying acceptance/UAT tests

Don't ask for:
- routine implementation choices already constrained by the plan
- standard refactors needed to make a task land cleanly

## Red flags — stop and fix

- A task is marked done but acceptance criteria are unchecked
- The plan no longer reflects the code (or the code drifted from the plan)
- "I'll add the test after" / "the test would be too hard"
- An open question silently ignored instead of resolved
- Changes drift outside the plan's scope without a new task being added
- A diff you can't fully explain
- A diff containing `.env`, `kubeconfig`, `eodag.yml`, or any password/token literal
