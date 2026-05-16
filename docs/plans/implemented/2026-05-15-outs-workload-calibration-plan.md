# MLB Outs Workload and Calibration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve MLB `outs_recorded` prediction quality by adding outs-specific workload features and a dedicated calibration/diagnostic pass, while leaving the strikeouts pipeline unchanged.

**Architecture:** Keep the current multi-stat pitcher-prop pipeline, but extend the outs branch with workload-centric features that better approximate innings pitched and plate-appearance volume. Add a small outs-only calibration/quality layer that evaluates predicted over probabilities against held-out outcomes so we can tune the outs pipeline without affecting strikeouts or other markets.

**Tech Stack:** Python 3.11, pandas, numpy, scikit-learn, pytest, ruff.

**Status:** Implemented

---

### Task 1: Add outs-specific workload features

**Files:**
- Create: `src/mlb/pitcher_props/outs_features.py`
- Modify: `src/mlb/pitcher_props/data.py`
- Modify: `src/mlb/pitcher_props/pipeline.py`
- Test: `tests/test_mlb_outs_features.py`

**Step 1: Write the failing test**

Add a regression test that builds a tiny pitcher-game frame and asserts the outs helper emits workload columns such as `plate_appearances`, `batters_faced`, `innings_pitched_proxy`, `outs_per_batter_faced`, and rolling outs features without leaking future rows.

```python
def test_add_outs_workload_features_is_leakage_safe():
    ...
    assert "rolling_outs_avg_3" in enriched.columns
    assert "batters_faced" in enriched.columns
    assert enriched.loc[0, "rolling_outs_avg_3"] == ...
```

Run:

```bash
.venv/bin/pytest -q tests/test_mlb_outs_features.py::test_add_outs_workload_features_is_leakage_safe -v
```

Expected: FAIL because the helper does not exist yet.

**Step 2: Write minimal implementation**

Implement the outs-only helper in `src/mlb/pitcher_props/outs_features.py` and wire it into the outs training/inference branch in `src/mlb/pitcher_props/pipeline.py`.

Keep the feature surface small and workload-focused:
- current game `plate_appearances`
- `batters_faced`
- `innings_pitched_proxy`
- `outs_per_batter_faced`
- `rolling_outs_avg_3`
- `rolling_outs_avg_5`
- `rolling_outs_per_pitch_5`

Update `build_pitcher_game_table()` in `src/mlb/pitcher_props/data.py` if any of those base columns need to be emitted upstream.

**Step 3: Run the test to verify it passes**

Run:

```bash
.venv/bin/pytest -q tests/test_mlb_outs_features.py::test_add_outs_workload_features_is_leakage_safe -v
```

Expected: PASS.

**Step 4: Commit**

```bash
git add src/mlb/pitcher_props/outs_features.py src/mlb/pitcher_props/data.py src/mlb/pitcher_props/pipeline.py tests/test_mlb_outs_features.py
git commit -m "feat: add outs workload features"
```

### Task 2: Add outs calibration diagnostics

**Files:**
- Create: `src/mlb/pitcher_props/calibration.py`
- Modify: `src/mlb/pitcher_props/pipeline.py`
- Test: `tests/integration/test_mlb_outs_recorded_pipeline.py`

**Step 1: Write the failing test**

Add an integration test that runs the outs pipeline on fixture data and asserts a calibration report is emitted with bin-level over-rate columns, plus a summary metric such as expected calibration error.

```python
def test_outs_pipeline_writes_calibration_report(tmp_path):
    ...
    assert calibration_path.exists()
    report = pd.read_csv(calibration_path)
    assert {"prob_bin", "mean_predicted_over", "observed_over_rate", "abs_gap"}.issubset(report.columns)
```

Run:

```bash
.venv/bin/pytest -q tests/integration/test_mlb_outs_recorded_pipeline.py::test_outs_pipeline_writes_calibration_report -v
```

Expected: FAIL because the report is not written yet.

**Step 2: Write minimal implementation**

Add an outs-only calibration helper that:
- bins held-out `prob_over`
- compares them to realized over outcomes
- writes a CSV report
- returns a summary dict with calibration error

Call it from the outs pipeline after scoring so the report is produced only for `outs_recorded`.

**Step 3: Run the test to verify it passes**

Run:

```bash
.venv/bin/pytest -q tests/integration/test_mlb_outs_recorded_pipeline.py::test_outs_pipeline_writes_calibration_report -v
```

Expected: PASS.

**Step 4: Commit**

```bash
git add src/mlb/pitcher_props/calibration.py src/mlb/pitcher_props/pipeline.py tests/integration/test_mlb_outs_recorded_pipeline.py
git commit -m "feat: add outs calibration diagnostics"
```

### Task 3: Verify outs accuracy and keep strikeouts untouched

**Files:**
- Modify: `tests/test_mlb_pitcher_prop_model_selection.py`
- Modify: `tests/integration/test_mlb_outs_recorded_pipeline.py`

**Step 1: Write the failing test**

Add a focused regression test that asserts the outs path uses outs-target features and still resolves `ResidualBootstrapper.from_games(..., target_col="outs_recorded")` rather than falling back to strikeout labels.

```python
def test_outs_pipeline_uses_outs_target_for_bootstrapper():
    ...
    assert captured["target_col"] == "outs_recorded"
```

Run:

```bash
.venv/bin/pytest -q tests/test_mlb_pitcher_prop_model_selection.py::test_outs_pipeline_uses_outs_target_for_bootstrapper -v
```

Expected: FAIL until the pipeline wiring is in place.

**Step 2: Write minimal implementation**

Confirm the outs branch never renames `outs_recorded` to `strikeouts`, and make sure any new feature columns are only surfaced for `outs_recorded`.

**Step 3: Run the full targeted suite**

Run:

```bash
.venv/bin/pytest -q tests/test_mlb_pitcher_prop_model_selection.py tests/integration/test_mlb_outs_recorded_pipeline.py -q
```

Expected: PASS.

**Step 4: Commit**

```bash
git add tests/test_mlb_pitcher_prop_model_selection.py tests/integration/test_mlb_outs_recorded_pipeline.py
git commit -m "test: lock down outs pipeline feature and calibration paths"
```
