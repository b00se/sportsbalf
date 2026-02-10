# CODEX.md

Scope: Entire repository.

## Purpose
Codify the standard workflow for committing approved plan docs from a clean, up-to-date `main` branch.

## Plan Commit Workflow (Mainline)
Use this sequence when the user asks to commit a plan document (for example under `docs/plans/planned/`).

1. Operate in the primary repo path (`/Users/jbrys/sportsbalf`) for the commit, not a feature worktree.
2. Confirm branch is `main`:
   - `git branch --show-current`
3. Update local `main` before staging:
   - `git pull --ff-only origin main`
4. Verify cleanliness for tracked files:
   - `git status --short`
   - If unexpected tracked changes exist, stop and ask before proceeding.
5. Stage only intended plan/doc files (never broad-add by default):
   - `git add docs/plans/planned/<plan-file>.md`
6. Commit with a clear message:
   - `git commit -m "Add PR#<n> <topic> plan"`
7. Re-check post-commit state:
   - `git status --short`
   - `git log --oneline -n 3`

## Safety Rules
- Do not stage or commit `.worktrees/`.
- Do not include unrelated file changes in plan-doc commits.
- Prefer fast-forward pull only (`--ff-only`) for `main` synchronization.
- If `main` cannot fast-forward cleanly, stop and ask how to proceed.

## Worktree Note
Worktrees remain the default for substantial implementation changes. The mainline plan commit workflow above applies specifically when user intent is to land approved plan docs on `main`.
