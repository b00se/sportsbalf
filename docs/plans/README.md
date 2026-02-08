# Plans Directory

This directory is split into:

- `docs/plans/planned/`: proposals and roadmap documents that describe intended work.
- `docs/plans/implemented/`: post-implementation records for plans that have shipped.

## Lifecycle

1. Create new planning docs in `planned/`.
2. Keep docs in `planned/` while work is intended, in progress, partially complete, or blocked.
3. Move a doc to `implemented/` only when scope is fully shipped against acceptance criteria.
4. Keep filenames stable when moving so references stay intact.

## Status labels (required in each plan doc)
- `Status: Planned`
- `Status: Planned (Partially Implemented)` when substantial work is done but criteria are not fully met.
- `Status: Implemented` when scope is fully shipped.

## Optional local automation
- Install repo-managed git hooks:
  - `scripts/install_git_hooks.sh`
- Post-commit plan approval loop:
  - If a commit message contains `[plan-approved]` (or `plan approved` / `approve plan`) and touches `docs/plans/*`, the `post-commit` hook runs `scripts/audit_plans_doneness.py`.
  - The hook stages the generated `docs/plans/plan-doneness-audit-auto-YYYY-MM-DD.md` and creates a follow-up commit:
    - `Refresh auto plan doneness audit`
