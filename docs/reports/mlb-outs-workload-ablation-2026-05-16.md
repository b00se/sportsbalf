# MLB Outs Workload Ablation Report

Date: 2026-05-16

## Summary

Final `outs_recorded` model after leakage-safe outs workload expansion and prune:

- Champion: `hist_gradient_boosting`
- Walk-forward MAE: `2.8965`
- Walk-forward RMSE: `3.8100`
- Walk-forward R2: `0.2375`
- Feature count: `37`

Compared with the original outs baseline:

- Baseline MAE: `2.9319`
- Improvement: `0.0354` MAE

Compared with the intermediate combined feature run:

- Intermediate MAE: `2.9029`
- Improvement after pruning: `0.0065` MAE

## Final Feature Ranking

Permutation importance on a 1,500-row sample from the final champion, ranked by MAE impact.

| Rank | Feature | Importance | Std Dev |
| --- | --- | ---: | ---: |
| 1 | `rest_days` | 0.1702 | 0.0152 |
| 2 | `rolling_on_base_events_allowed_5` | 0.1374 | 0.0145 |
| 3 | `rolling_outs_recorded_5` | 0.1108 | 0.0040 |
| 4 | `umpire_sample_size` | 0.0610 | 0.0061 |
| 5 | `batters_faced` | 0.0327 | 0.0031 |
| 6 | `opponent_out_rate` | 0.0273 | 0.0023 |
| 7 | `rolling_K_avg_5` | 0.0220 | 0.0047 |
| 8 | `rolling_K_rate` | 0.0203 | 0.0024 |
| 9 | `prev_pitch_count` | 0.0195 | 0.0057 |
| 10 | `prev_outs_recorded` | 0.0133 | 0.0023 |
| 11 | `rolling_pitch_count_10` | 0.0125 | 0.0040 |
| 12 | `season_avg_pitch_count_to_date` | 0.0120 | 0.0041 |
| 13 | `rolling_pitch_count_5` | 0.0100 | 0.0015 |
| 14 | `rolling_outs_per_batter_faced_5` | 0.0096 | 0.0015 |
| 15 | `rolling_hard_contact_allowed_5` | 0.0091 | 0.0010 |

## Iteration Changelog

1. Fixed the residual bootstrap sampler to use `outs_recorded` directly instead of renaming into strikeout labels.
2. Added outs-specific workload features and verified they are leakage-safe.
3. Ran walk-forward ablations across:
   - long-window workload history
   - season/career workload priors
   - role/leash proxies
4. Combined all three feature families into the production outs pipeline.
5. Pruned two redundant ratio features:
   - `outs_per_batter_faced`
   - `prev_outs_per_batter_faced`
6. Re-ran full walk-forward backtesting and kept the pruned model because it produced the best MAE.

## Takeaways

- Outs is primarily a workload and leash problem.
- `rest_days`, prior workload, and recent outs history dominate the model.
- The ratio-style outs-per-batter features were redundant and slightly noisy.
- The final model is better than the original outs baseline, but the gain is incremental rather than dramatic.
