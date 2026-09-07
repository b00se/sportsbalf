# NFL 2026 Locked Decisions

## Daily rankings

- One manually downloaded UD rankings CSV is the input for each weekly slate.
- One generated ranking is manually uploaded and used for all entries on that slate.
- All picks run on Autopilot. V1 changes row order and recommends maximum position counts.
- Row order is presumed to control Autopilot, but a controlled proof is a hard launch gate.
- Default scoring is half PPR; explicit PPR overrides it. Unknown scoring fails closed.
- Standard roster: QB 1, RB 1, WR 2, FLEX 1, TE 1.
- Single-game: four SFLEX slots; implement after the standard-slate vertical slice.
- Optimize expected net tournament payout. Entry count and bankroll sizing remain user-controlled.
- Use component-stat models. Direct fantasy-point regression is a benchmark/residual candidate.
- UD ADP/projected points may be inputs or benchmarks when evidence supports their use.
- Confirmed inactive players are excluded. Uncertain availability is probability-weighted and warned.
- Rookie V1 uses conservative priors and wider uncertainty; a full transition model is deferred.

## Prediction Markets

- Rankings launch first.
- Pregame 6box only: both ML sides, both spread sides, over, under.
- Hybrid independent score model plus public consensus.
- UD all-in executable quote is authoritative and functionally taker-only.
- Read-only UD API discovery is required; manual quote entry is not normal operation.
- No order placement automation.

## Operations

- CLI plus Markdown/HTML report; no V1 dashboard.
- Weekly build, not repeated same-day refresh optimization.
- Archive starts Week 1 2026.
- Exposure CSV supports user-roster reconstruction.
- Completed-contest scraping is a later feasibility spike.
