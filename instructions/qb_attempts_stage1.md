# Pipeline Modification: QB Pass Attempts Dataset Builder

This document outlines the steps to build a new dataset for modeling QB pass attempts, which will be integrated into the existing betting analytics pipeline. This task involves modifying the data ingestion and feature engineering steps to handle NFL data from `nfl_data_py`, a different library from the one used for MLB.

## Handling Ambiguity and Requesting Clarification

If any step in these instructions is unclear, ambiguous, or if a required function or data source (e.g., `import_ud_lines`) is not accessible, **stop execution immediately**. Do not make assumptions. Instead, provide a clear, concise query explaining the ambiguity and requesting further instructions from the user.

**Example Query:**
"I am unable to locate or execute the `import_ud_lines` function. Please provide the implementation details for this function or specify the data source from which to pull Underdog lines."

-----

## Step 1. Git Workflow

1.  Create a new branch for this work:
    ```bash
    git checkout -b feature/qb-attempts-dataset
    ```

-----

## Step 2. Data Ingest (Small Scale Test)

1.  Import libraries:

    ```python
    import nfl_data_py as nfl
    import pandas as pd
    ```

2.  Pull data for a single test season (e.g., 2023):

    ```python
    year = 2023
    weekly = nfl.import_weekly_data([year])
    sched = nfl.import_schedules([year])
    ```

3.  Filter for QBs and select relevant fields:

    ```python
    qbs = weekly[weekly['position'] == 'QB'][[
        'season', 'week', 'game_id',
        'player_id', 'player_display_name',
        'recent_team', 'opponent_team',
        'attempts'
    ]]
    ```

4.  Trim schedule to joinable fields:

    ```python
    sched = sched[['game_id','season','week','home_team','away_team','spread_line','total_line']]
    ```

5.  **Ensure Consistent Data Types for Merge Keys**:

    ```python
    # Convert game_id columns to string to ensure a successful merge.
    qbs['game_id'] = qbs['game_id'].astype(str)
    sched['game_id'] = sched['game_id'].astype(str)
    ```

6.  Merge schedules into QB data:

    ```python
    qbs = qbs.merge(sched, on=['season','week','game_id'], how='left')
    ```

-----

## Step 3. Expand to Multiple Seasons

1.  Replace single year with a range:
    ```python
    years = list(range(2015, 2024))
    weekly = nfl.import_weekly_data(years)
    sched = nfl.import_schedules(years)
    ```
2.  Repeat the filtering, data type conversion, and merging steps from Step 2.

-----

## Step 4. Ingest UD Lines

1.  **Pull QB pass attempt lines from an external function.**

      - Assume we have an internal or external function like `import_ud_lines(market='pass_attempts', years=years)`.
      - Call this function to get UD lines.
      - Key fields to extract: `game_id`, `player_id`, `line`, `book='Underdog'`.

2.  **Merge UD lines into the main dataset.**

      - Merge the UD lines DataFrame into the `qbs` DataFrame.
      - The join should be a `left` join on the common keys: `season`, `week`, and `player_id`.

3.  **Result:** Each row in the final dataset should have both the **actual attempts** and the corresponding **UD line**.

-----

## Step 5. Normalize Team Names / Abbreviations

Team abbreviations change over time (e.g., `OAK` → `LV`, `SD` → `LAC`, `STL` → `LA`). Create a mapping dictionary and apply it to both schedule and player datasets.

```python
team_map = {
    'OAK': 'LV',
    'SD': 'LAC',
    'STL': 'LA',
    # add others as needed
}

# Apply the mapping to the relevant columns
qbs['recent_team'] = qbs['recent_team'].replace(team_map)
qbs['opponent_team'] = qbs['opponent_team'].replace(team_map)
sched['home_team'] = sched['home_team'].replace(team_map)
sched['away_team'] = sched['away_team'].replace(team_map)
```

-----

## Step 6. Derived Features

Add simple rolling features to test the pipeline:

```python
qbs = qbs.sort_values(['player_id','season','week'])
qbs['prev_attempts'] = qbs.groupby('player_id')['attempts'].shift(1)
qbs['rolling3_attempts'] = (
    qbs.groupby('player_id')['attempts']
       .rolling(3).mean()
       .reset_index(level=0, drop=True)
)
```

-----

## Step 7. Output

1.  **Rename Columns to Match Final Schema**:

    ```python
    qbs = qbs.rename(columns={
        'player_display_name': 'qb_name',
        'player_id': 'qb_id',
        'recent_team': 'team',
        'opponent_team': 'opponent',
        'attempts': 'pass_attempts',
        'spread_line': 'spread',
        'total_line': 'total',
        'line': 'ud_line'
    })
    ```

2.  **Save to Parquet:**

      - Create the `data` directory if it does not exist.
      - Save the final DataFrame to the specified path.

    <!-- end list -->

    ```bash
    mkdir -p data
    ```

    ```python
    qbs.to_parquet("data/qb_attempts_dataset.parquet", index=False)
    ```

3.  **Expected Schema:** The final DataFrame should have the following columns in any order:
    | season | week | game\_id | qb\_name | qb\_id | team | opponent | home | spread | total | pass\_attempts | ud\_line | prev\_attempts | rolling3\_attempts |

-----

## Step 8. Next Steps

  - **Validate Joins:** Check for `NaN` values in the `spread` and `ud_line` columns to ensure the merges were successful.
  - Backfill more features (plays per game, pass rate, opponent tendencies).
  - Confirm UD line coverage across seasons.
  - Push results to branch and open PR.

<!-- end list -->

```bash
git add .
git commit -m "Build QB attempts dataset with UD lines + normalization"
git push origin feature/qb-attempts-dataset
```