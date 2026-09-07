# Implementer Prompt Template

    Implement task <TASK_ID> only.

    Read AGENTS.md, the active wave file, and the <TASK_ID> section of the
    master roadmap. Dependencies: <HASHES>. Allowed files: <FILES>.

    Confirm or write behavior-focused tests first. Implement the smallest
    complete change. Loop on failures. Do not use network access in tests,
    weaken assertions, edit outside ownership, or modify protected directories.

    Before completion run focused tests, affected broader tests, ruff on changed
    files, and git diff --check. Commit only this task.

    Return changed files, commands with exit codes, exit-criterion evidence,
    risks, and commit hash.
