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
- "review plan"
- "execute this plan <path>"
- "approve plan <path>"
- "refresh/update plan audit"
- "commit/push/open PR for plan work"

## Commands the user can invoke
Treat these as direct workflow commands in chat.

1. `plan:keep <path>`
- Read the plan and continue refining it only (no code changes).
- Output updated decision-complete plan content.

2. `plan:review <path>`
- Review the plan quality and decision-completeness.
- Return concrete findings/gaps and required fixes before approval.

3. `plan:execute <path>`
- Execute plan end-to-end with normal repo workflow.
- Default to simple branch-first execution in `/Users/jbrys/sportsbalf`:
  1) create/use a feature branch from `main`,
  2) implement on that branch,
  3) run review rounds on `main...HEAD`,
  4) open PR from that same branch.
- Use TDD where applicable, run validations, summarize RED/GREEN evidence.

4. `plan:audit`
- Update `docs/plans/plan-doneness-audit-2026-02-08.md` to reflect current state.
- Ensure PR/branch status and implemented/planned classifications are consistent.

5. `plan:finalize <message>`
- Stage relevant plan/docs/code files.
- Commit with provided message.
- Push branch and open/edit PR when requested.

6. `plan:approve <path>`
- Approval workflow for your default sequence:
  1) confirm plan is in `docs/plans/planned/`
  2) if user requested commit-on-main, move to `/Users/jbrys/sportsbalf` on branch `main`
  3) pull latest main with `git pull --ff-only origin main`
  4) verify tracked tree is clean before staging
  5) commit only the approved plan doc change(s)
  6) audit plans by updating `docs/plans/plan-doneness-audit-2026-02-08.md` when requested
  7) commit audit update separately when requested
  8) verify commit landed on intended branch via `git branch --show-current` and `git log -n 1`
  9) optionally push/open PR when requested

7. `plan:review-context`
- Verify review context before running review tasks:
  1) print `pwd`, current branch, and `git status --short`
  2) verify intended diff scope is non-empty
  3) if diff is empty, stop and return exact context-switch steps instead of findings

## Repo Constraints
- Keep tests offline and deterministic.
- Use repo-local executables (`.venv/bin/...`).
- Do not touch `data/`, `models/`, `notebooks/`, `betslips/` for planning flows.
- Keep changes surgical and scoped to the requested plan.
- Do not stage `.worktrees/` in plan workflow commits.
- Do not run review/findings on empty diff scope.

## Validation Defaults
- Lint: `.venv/bin/ruff check .`
- Tests: `.venv/bin/pytest -q`
- For docs-only updates: skip full tests unless requested, but still run quick consistency checks (`rg`, `git diff`, status checks).

## Notes
- This skill intentionally replaces hook-based automation with explicit chat commands.
- If the user asks for global Codex behavior changes, explain that repo-local skills cannot modify product-level UI hooks.
