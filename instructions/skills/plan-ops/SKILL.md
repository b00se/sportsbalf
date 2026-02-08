---
name: plan-ops
description: Run common planning workflows in this repo: keep planning, execute approved plan, refresh plan docs/audit, and finalize with commit/push/PR. Trigger when user asks for repeated planning actions, plan execution loops, or post-plan audit/commit automation.
---

# Skill: plan-ops

## Purpose
Provide a consistent command-style workflow for common planning actions without repo hooks.

## Trigger Phrases
Use this skill when user messages include any of these intents:
- "keep planning"
- "execute this plan <path>"
- "approve plan <path>"
- "refresh/update plan audit"
- "commit/push/open PR for plan work"

## Commands the user can invoke
Treat these as direct workflow commands in chat.

1. `plan:keep <path>`
- Read the plan and continue refining it only (no code changes).
- Output updated decision-complete plan content.

2. `plan:execute <path>`
- Execute plan end-to-end with normal repo workflow.
- Use TDD where applicable, run validations, summarize RED/GREEN evidence.

3. `plan:audit`
- Update `docs/plans/plan-doneness-audit-2026-02-08.md` to reflect current state.
- Ensure PR/branch status and implemented/planned classifications are consistent.

4. `plan:finalize <message>`
- Stage relevant plan/docs/code files.
- Commit with provided message.
- Push branch and open/edit PR when requested.

5. `plan:approve <path>`
- Composite workflow:
  1) execute the plan at `<path>`
  2) refresh doneness audit
  3) commit with a plan-specific message
  4) push and open PR

## Repo Constraints
- Keep tests offline and deterministic.
- Use repo-local executables (`.venv/bin/...`).
- Do not touch `data/`, `models/`, `notebooks/`, `betslips/` for planning flows.
- Keep changes surgical and scoped to the requested plan.

## Validation Defaults
- Lint: `.venv/bin/ruff check .`
- Tests: `.venv/bin/pytest -q`
- For docs-only updates: skip full tests unless requested, but still run quick consistency checks (`rg`, `git diff`, status checks).

## Notes
- This skill intentionally replaces hook-based automation with explicit chat commands.
- If the user asks for global Codex behavior changes, explain that repo-local skills cannot modify product-level UI hooks.
