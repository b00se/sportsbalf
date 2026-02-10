# CODEX.md

Scope: Entire repository.

## Purpose
Codify the standard workflows for implementation, review, and approved plan-doc commits with explicit context and safety checks.

## Workflow Matrix

### 1) Worktree Implementation Flow
Use for substantial code changes.

1. Create/use a dedicated worktree branch.
2. Implement and validate in that worktree.
3. Commit/push from that branch.
4. Open PR from that branch to `main`.

### 2) Mainline Plan Commit Flow
Use when user asks to commit approved plan/docs directly to `main`.

1. Operate in the primary repo path (`/Users/jbrys/sportsbalf`), not a feature worktree.
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

### 3) Review Flow
Use when asked to run review/findings.

1. Confirm context:
   - `pwd`
   - `git branch --show-current`
   - `git status --short`
2. Confirm comparison target and ensure non-empty diff:
   - example: `git diff <merge-base>` or `git diff main...HEAD`
3. If diff is empty:
   - report no actionable review due to empty diff,
   - provide exact branch/worktree switch command(s),
   - do not fabricate findings.
4. Run review in the branch/worktree that contains the intended commits.

## Wrong-Context Recovery
- If on the wrong branch/worktree for requested action:
  1. report current context,
  2. switch to intended path/branch,
  3. re-run context checks before continuing.
- For worktree commits mistakenly reviewed from `main`, switch to the worktree branch and re-run review.

## Safety Rules
- Do not stage or commit `.worktrees/`.
- Do not include unrelated file changes in plan-doc commits.
- Prefer fast-forward pull only (`--ff-only`) for `main` synchronization.
- If `main` cannot fast-forward cleanly, stop and ask how to proceed.
