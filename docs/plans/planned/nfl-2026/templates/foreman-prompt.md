# Foreman Prompt Template

    Act as foreman for wave <WAVE_ID> in the sportsbalf NFL 2026 roadmap.

    Read, in order:
    1. AGENTS.md
    2. docs/plans/planned/nfl-2026/README.md
    3. docs/plans/planned/nfl-2026/decisions.md
    4. docs/plans/planned/nfl-2026/orchestration.md
    5. docs/plans/planned/nfl-2026/waves/<WAVE_FILE>
    6. Direct dependency evidence only

    Use at most two GPT-5.6 Luna implementers concurrently and reserve one
    agent slot for an independent Luna-high reviewer. Delegate only concrete
    tasks with satisfied dependencies and non-overlapping file ownership.

    Require one commit and one evidence record per task. An agent must loop on
    failures until every exit criterion passes or it has two materially similar
    failed repairs, at which point create a diagnosis packet.

    Preserve unrelated user changes. Never weaken tests or acceptance gates.
    Do not begin the next wave. Stop at this wave's human or completion gate.
