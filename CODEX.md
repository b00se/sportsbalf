# CODEX.md

Scope: Entire repository.

## Purpose

Companion workflow policy for autonomous coding sessions in this repo.
`AGENTS.md` remains the fuller policy document; when conflicts exist, follow `AGENTS.md`.

## Canonical Defaults

1. Branch-first for substantial work:
- create/use feature branch from `main`
- implement, validate, review, merge

2. Worktrees are opt-in only:
- use only when explicitly requested or parallel branch work is required

3. Mainline doc-only commits are allowed when explicitly requested.

4. Always use repo-local executables (`.venv/bin/...`).

## Mandatory Context Preflight

Before major git actions (review, commit, branch switch, merge):

```bash
pwd
git branch --show-current
git status --short
```

If unexpected tracked changes are present, stop and confirm intent.

## Review Guardrail

- Verify non-empty diff scope before findings (`main...HEAD` or requested base).
- If empty diff, report context issue and provide exact switch commands.
- Do not fabricate review findings.

## Mainline Plan/Docs Commit Flow

Use only when user asks for direct `main` docs commit.

1. Be in primary checkout: `/Users/jbrys/sportsbalf`
2. Confirm branch `main`
3. Sync safely: `git pull --ff-only origin main`
4. Stage only intended files
5. Commit with specific message
6. Re-check `git status --short` and recent log

## Repo-Specific Footguns

- Do not stage `.worktrees/` unless explicitly asked.
- Do not use `cli/main.py` for canonical pipeline work.
- Do not hardcode data/model paths in source code.
- Do not introduce network-required tests.
- Do not change output schemas silently.
