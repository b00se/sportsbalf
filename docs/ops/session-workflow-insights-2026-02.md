# Session Workflow Insights (2026-02)

Status: Operational guidance artifact

## Purpose
Capture recurring workflow friction observed in local Codex session history for this repo and document the policy updates that address it.

## Sources Analyzed
- `~/.codex/sessions` entries filtered to `cwd=/Users/jbrys/sportsbalf`
- Recent repo commit history on `main`
- Existing workflow docs:
  - `AGENTS.md`
  - `CODEX.md`
  - `instructions/codex_autonomy_runbook.md`
  - `instructions/skills/repo-autonomy/SKILL.md`
  - `instructions/skills/plan-ops/SKILL.md`

## Recurring Frictions
1. Review was sometimes run from the wrong branch/context and returned empty-diff results.
2. Plan/doc commits were requested on `main` while active context remained in a worktree.
3. Untracked `.worktrees/` appeared in status and risked accidental staging/distraction.
4. Repeated need to verify branch/path/status before major git actions.
5. Occasional stale wording in docs led to contract drift (example: outdated config fallback description).

## Policy Decisions (Now Codified)
1. Context preflight is mandatory before major git operations:
   - `pwd`
   - `git branch --show-current`
   - `git status --short`
2. Review output must be blocked when diff scope is empty; return context-switch steps instead.
3. Mainline plan-doc commits require:
   - path `/Users/jbrys/sportsbalf`
   - branch `main`
   - `git pull --ff-only origin main`
   - targeted staging only.
4. Worktree remains default for substantial implementation work.
5. `.worktrees/` is treated as non-commit noise unless explicitly requested.

## Before vs After Behavior
- Before:
  - review could be run from `main` while changes lived in a worktree branch.
  - result: no actionable findings despite existing changes.
- After:
  - review flow explicitly checks non-empty diff and context first.
  - if empty, workflow returns correction steps rather than misleading findings.

- Before:
  - plan commit tasks could be executed without explicit mainline sync checks.
- After:
  - plan workflows enforce `main` + fast-forward pull + clean tracked state before commit.

## Maintenance Notes
- Refresh this artifact when workflow issues repeat in new sessions.
- Keep policy details in operational docs/skills; keep this file as rationale and evidence summary.
