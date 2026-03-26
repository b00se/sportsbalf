# MLB Underdog Lines and Betslips Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a shadow-mode MLB workflow that fetches live Underdog pitcher-prop lines for all supported modeled stats, scores the available props, and generates JSON betslips with mixed-stat and same-pitcher support.

**Architecture:** Reuse the existing MLB pitcher-prop pipelines as the scoring engines. Add a shared MLB Underdog ingestion layer plus a thin orchestration layer that fetches live lines, writes dated snapshots, normalizes scored outputs into a common candidate-leg schema, and generates slips with explicit validity rules.

**Tech Stack:** Python 3.11, pandas, repo-local CLI scripts, existing MLB pitcher-prop pipeline modules, existing Underdog NFL API pattern, pytest, ruff.

---

Status: Planned

Date: 2026-03-25

## Stacked Diff Strategy
Execute this plan as a series of small reviewable branches/PRs, not one large
change. Target roughly 150-400 net lines per slice when possible, and avoid
mixing ingestion, orchestration, slip logic, and docs in the same review unless
the dependency is trivial.

Recommended stack:

1. `mlb-ud-lines-pr1-contracts`
- Scope:
  - config/docs contract only
  - no live network behavior
- Expected review surface:
  - docs and config-schema updates
  - any minimal config validation tests

2. `mlb-ud-lines-pr2-ingestion`
- Scope:
  - `src/mlb/data/underdog.py`
  - payload parsing/normalization tests
- Exclude:
  - no pipeline orchestration
  - no slip-builder changes

3. `mlb-ud-lines-pr3-line-snapshots`
- Scope:
  - stat-specific line normalization
  - dated snapshot writing helpers
- Exclude:
  - no combined slate scoring
  - no betslip generation changes

4. `mlb-ud-lines-pr4-slate-orchestration`
- Scope:
  - unified multi-stat MLB slate scoring orchestration
  - run-summary behavior for skipped/failed stats
- Exclude:
  - no slip-builder generalization yet

5. `mlb-ud-lines-pr5-mixed-slips`
- Scope:
  - generic candidate-leg schema
  - mixed-stat slip generation
  - same-pitcher stacking support
  - hard 2-player / 2-team validation rule

6. `mlb-ud-lines-pr6-live-cli`
- Scope:
  - live CLI wrapper
  - JSON artifact writing
  - README updates for operator workflow

7. `mlb-ud-lines-pr7-validation-and-polish`
- Scope:
  - final config wiring
  - targeted cleanup
  - validation evidence
- Only include code if discovered during validation. Do not save unrelated
  refactors for this PR.

Rules for every slice:
- Each PR must be independently reviewable against its parent branch.
- Each PR must include the tests that justify its behavior.
- Do not defer test coverage for behavior introduced in the slice.
- Keep docs aligned with the code introduced in that slice only.
- If a slice starts growing too large, split it again before implementation.

### Task 1: Lock the provider contract and planning docs

**Files:**
- Review: `docs/plans/planned/2026-03-25-mlb-underdog-lines-betslips-design.md`
- Modify: `docs/config-schema.md`
- Modify: `docs/contracts.md`
- Modify: `docs/architecture.md`

**Step 1: Write the failing test**

Add or update a config-contract test that fails until MLB live Underdog config
keys for multi-stat ingestion are recognized.

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest -q <targeted test>`

Expected: config/schema assertion fails on new MLB live-ingestion keys.

**Step 3: Write minimal implementation**

Document the new config surface for:
- MLB Underdog stat ids by stat,
- optional output paths for dated line snapshots,
- unified betslip orchestration defaults.

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest -q <targeted test>`

Expected: config/schema test passes.

**Step 5: Commit**

```bash
git add docs/config-schema.md docs/contracts.md docs/architecture.md
git commit -m "docs: define MLB Underdog lines and betslip contract"
```

### Task 2: Add MLB Underdog line ingestion primitives

**Files:**
- Create: `src/mlb/data/underdog.py`
- Modify: `src/mlb/data/__init__.py`
- Test: `tests/test_mlb_underdog_lines.py`

**Step 1: Write the failing test**

Add fixture-based tests that:
- parse a mocked Underdog payload for one MLB stat,
- normalize player, game, odds, and line metadata,
- return an empty frame when no matching lines exist.

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest -q tests/test_mlb_underdog_lines.py`

Expected: import/helper functions do not exist yet.

**Step 3: Write minimal implementation**

Mirror the NFL Underdog helper pattern, but normalize MLB pitcher-prop rows into
a reusable schema that supports stat-specific line columns and shared odds
fields.

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest -q tests/test_mlb_underdog_lines.py`

Expected: payload parsing tests pass offline.

**Step 5: Commit**

```bash
git add src/mlb/data/underdog.py src/mlb/data/__init__.py tests/test_mlb_underdog_lines.py
git commit -m "feat: add MLB Underdog line ingestion helpers"
```

### Task 3: Support multi-stat live line normalization and snapshot writing

**Files:**
- Modify: `src/mlb/data/load_props.py`
- Create: `src/mlb/pitcher_props/live_lines.py`
- Test: `tests/test_mlb_pitcher_prop_live_lines.py`

**Step 1: Write the failing test**

Add tests that:
- map unified live rows into per-stat line files,
- preserve required columns for each MLB stat,
- write dated snapshot outputs deterministically.

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest -q tests/test_mlb_pitcher_prop_live_lines.py`

Expected: normalizer/snapshot helpers do not exist yet.

**Step 3: Write minimal implementation**

Create helpers that transform fetched Underdog rows into the stat-specific CSV
shape expected by `load_pitcher_prop_lines` and persist dated snapshots for a
target slate.

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest -q tests/test_mlb_pitcher_prop_live_lines.py`

Expected: normalized live-line tests pass.

**Step 5: Commit**

```bash
git add src/mlb/data/load_props.py src/mlb/pitcher_props/live_lines.py tests/test_mlb_pitcher_prop_live_lines.py
git commit -m "feat: normalize live MLB pitcher-prop line snapshots"
```

### Task 4: Add unified MLB slate scoring orchestration

**Files:**
- Create: `src/mlb/pitcher_props/slate.py`
- Modify: `src/mlb/pitcher_props/__init__.py`
- Test: `tests/test_mlb_pitcher_prop_slate.py`

**Step 1: Write the failing test**

Add tests that:
- run multiple mocked MLB stat scorers,
- skip unavailable stats cleanly,
- produce one combined scored candidate frame.

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest -q tests/test_mlb_pitcher_prop_slate.py`

Expected: slate orchestration module does not exist yet.

**Step 3: Write minimal implementation**

Build a thin orchestrator that:
- fetches or accepts live lines per stat,
- invokes the existing MLB pitcher-prop pipelines,
- combines successful stat outputs into one frame,
- records skipped/failed stats for run reporting.

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest -q tests/test_mlb_pitcher_prop_slate.py`

Expected: multi-stat orchestration tests pass.

**Step 5: Commit**

```bash
git add src/mlb/pitcher_props/slate.py src/mlb/pitcher_props/__init__.py tests/test_mlb_pitcher_prop_slate.py
git commit -m "feat: add unified MLB pitcher-prop slate scoring"
```

### Task 5: Generalize candidate-leg normalization for mixed MLB slips

**Files:**
- Modify: `src/mlb/slips.py`
- Test: `tests/test_slips.py`
- Create: `tests/test_mlb_mixed_prop_slips.py`

**Step 1: Write the failing test**

Add tests that:
- generate slips from mixed MLB stat rows,
- allow same-pitcher stacks,
- reject slips that do not include at least 2 distinct players and 2 distinct
  teams.

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest -q tests/test_slips.py tests/test_mlb_mixed_prop_slips.py`

Expected: current strikeouts-specific slip builder cannot satisfy the mixed-stat
schema or validity rules.

**Step 3: Write minimal implementation**

Refactor the MLB slip builder to consume a common candidate-leg schema with
generic stat fields instead of strikeouts-only columns. Add the 2-player / 2-team
 validator as a hard filter during slip generation.

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest -q tests/test_slips.py tests/test_mlb_mixed_prop_slips.py`

Expected: slip tests pass with same-pitcher stacking and rule enforcement.

**Step 5: Commit**

```bash
git add src/mlb/slips.py tests/test_slips.py tests/test_mlb_mixed_prop_slips.py
git commit -m "feat: support mixed MLB pitcher-prop slips"
```

### Task 6: Add a CLI for live fetch -> score -> JSON slips

**Files:**
- Modify: `scripts/build_betslips.py`
- Create: `scripts/build_mlb_live_betslips.py`
- Test: `tests/test_build_mlb_live_betslips.py`
- Modify: `README.md`

**Step 1: Write the failing test**

Add a CLI-level test that:
- mocks live Underdog payload fetches,
- mocks stat scoring outputs,
- verifies JSON slips are written for a target slate.

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest -q tests/test_build_mlb_live_betslips.py`

Expected: live orchestration CLI does not exist yet.

**Step 3: Write minimal implementation**

Create a dedicated CLI entrypoint for MLB live shadow runs that:
- fetches lines,
- writes dated snapshots,
- scores all available modeled pitcher props,
- emits JSON slips and a concise run summary.

Keep the older `scripts/build_betslips.py` compatible where practical, but do not
force it to own the full live multi-stat workflow if a separate script is
cleaner.

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest -q tests/test_build_mlb_live_betslips.py`

Expected: live MLB betslip CLI tests pass offline.

**Step 5: Commit**

```bash
git add scripts/build_betslips.py scripts/build_mlb_live_betslips.py tests/test_build_mlb_live_betslips.py README.md
git commit -m "feat: add MLB live betslip shadow-run CLI"
```

### Task 7: Wire config defaults and stat-id lookup behavior

**Files:**
- Modify: `config/mlb.yaml`
- Modify: `src/core/config.py`
- Test: `tests/test_core_config.py`

**Step 1: Write the failing test**

Add config-loading tests for the new MLB live-line config surface and fallback
behavior when some stat ids are absent.

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest -q tests/test_core_config.py`

Expected: config validation does not recognize the new keys or defaults.

**Step 3: Write minimal implementation**

Add config support for MLB live Underdog stat-id mappings and any required
orchestration defaults without breaking current stat-specific MLB configs.

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest -q tests/test_core_config.py`

Expected: new config tests pass.

**Step 5: Commit**

```bash
git add config/mlb.yaml src/core/config.py tests/test_core_config.py
git commit -m "feat: add MLB live lines config wiring"
```

### Task 8: Run targeted and full validation

**Files:**
- Review: `tests/test_mlb_underdog_lines.py`
- Review: `tests/test_mlb_pitcher_prop_live_lines.py`
- Review: `tests/test_mlb_pitcher_prop_slate.py`
- Review: `tests/test_slips.py`
- Review: `tests/test_mlb_mixed_prop_slips.py`
- Review: `tests/test_build_mlb_live_betslips.py`

**Step 1: Run targeted tests**

Run:
- `.venv/bin/pytest -q tests/test_mlb_underdog_lines.py`
- `.venv/bin/pytest -q tests/test_mlb_pitcher_prop_live_lines.py`
- `.venv/bin/pytest -q tests/test_mlb_pitcher_prop_slate.py`
- `.venv/bin/pytest -q tests/test_slips.py tests/test_mlb_mixed_prop_slips.py`
- `.venv/bin/pytest -q tests/test_build_mlb_live_betslips.py`

Expected: all new/modified behavior tests pass.

**Step 2: Run repo validation**

Run:
- `.venv/bin/ruff check .`
- `.venv/bin/pytest -q`

Expected: lint and full offline suite pass.

**Step 3: Manual smoke check**

Run a shadow/manual MLB live workflow against a temp output path and capture:
- which stats returned live lines,
- how many candidate legs were scored,
- how many slips were emitted,
- whether every emitted slip satisfied the 2-player / 2-team rule.

**Step 4: Commit**

```bash
git add README.md docs/config-schema.md docs/contracts.md docs/architecture.md config/mlb.yaml src tests scripts
git commit -m "chore: validate MLB live Underdog betslip workflow"
```

## Notes
- Keep all tests offline by using mocked Underdog payloads and mocked or fixture
  MLB pipeline outputs.
- Do not auto-submit slips or add operator-account logic in this plan.
- Treat live Underdog stat ids as updateable config/constants, not deep internal
  invariants.
- Preserve existing sportsbook pipeline entrypoints and output schema where
  possible.
- Prefer one commit per task and one PR per stacked-diff slice.
