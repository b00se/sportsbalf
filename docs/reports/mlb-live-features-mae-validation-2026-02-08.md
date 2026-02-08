# MLB Live Features MAE Validation (2026-02-08)

## Goal
Validate the live-features acceptance gate from
`docs/plans/planned/mlb-pybaseball-live-features-plan.md`:

- gate: enriched live-feature model must show positive MAE lift vs baseline.

## Commands

```bash
PYTHONPATH=. .venv/bin/python scripts/backtest_mlb_strikeouts.py \
  --config config/mlb.yaml \
  --compare-feature-sets \
  --comparison-out runtime/mlb_strikeouts_feature_comparison.csv \
  --champion-out runtime/mlb_strikeouts_feature_comparison_summary.json
```

Additional candidate sweep (same comparison mode, different
`mlb.strikeouts.model_selection.candidates`):
- `random_forest`
- `xgboost`
- `hist_gradient_boosting`
- `elastic_net`
- `poisson`
- `random_forest,xgboost,hist_gradient_boosting`

## Results

Default config comparison:
- baseline MAE: `1.467579`
- enriched MAE: `1.491491`
- MAE improvement (`baseline - enriched`): `-0.023912`
- gate passed: `false`

Candidate sweep summary:
- `random_forest`: `-0.023912` (fail)
- `xgboost`: `-0.021907` (fail)
- `hist_gradient_boosting`: `-0.012598` (fail)
- `elastic_net`: `-0.000756` (fail)
- `poisson`: `-0.003221` (fail)
- `random_forest,xgboost,hist_gradient_boosting`: `-0.015167` (fail)

## Conclusion
The strict MAE+ acceptance gate is **not met** on current data/config.
The live-features plan remains partially complete pending either:
1. feature/model adjustments that yield positive MAE lift, or
2. an explicit policy change to the acceptance criterion.
