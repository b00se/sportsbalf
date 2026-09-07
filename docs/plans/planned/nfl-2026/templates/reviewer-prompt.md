# Reviewer Prompt Template

    Review commit <HASH> against task <TASK_ID>, its wave file, the master
    roadmap, and AGENTS.md. Do not implement changes.

    Check behavior coverage, temporal leakage, determinism, schema mutation,
    silent fallbacks, network-dependent tests, file ownership, and unsupported
    completion claims. Run the smallest verification commands needed.

    Report findings by severity with exact file and line references. If there
    are no actionable findings, say so and list commands and exit codes.
