# Architecture & Data Pipeline

## Data flow

```
Transfermarkt  →  scrape.py  →  CSV/*.csv  →  db_setup.py  →  j1_league.duckdb
                                                                     │
                                                     ┌───────────────┴───────────────┐
                                                     ▼                               ▼
                                            train_model.py                    dashboard.py
                                       (offline, run manually)          (reads model + DB live)
                                                     │
                                                     ▼
                                       outcome_predictor.joblib
```

## Feature engineering

All features are built by `build_features_from_matches()` in
`train_model.py`, which is deliberately the **only** place this logic lives —
the dashboard's backtest imports and calls the same function rather than
reimplementing it, so the two can't drift out of sync.

Feature groups:

- **Rolling form** (last 5 matches, shifted so the match being predicted is
  never included): points, goals for/against, goal difference, matches
  played, unbeaten streak, days since last match, season-to-date points and
  goal difference — all computed separately for home and away.
- **Home/away-specific form**: a team's home form isn't the same as its
  overall form, and likewise for away — tracked separately.
- **Elo ratings**: point-in-time Elo (à la FiveThirtyEight's SPI) with a
  home-advantage adjustment and a margin-of-victory multiplier, computed
  chronologically so each match only sees ratings as they stood *before*
  kickoff.
- **Squad features**: market value, average age, and foreign-player count
  per team per season, pulled from Transfermarkt squad pages and merged in
  by team + year. Missing squad data falls back to the league median for
  that stat.

### Predicting unplayed fixtures

`build_current_state()` reuses the exact same rolling-form logic by
appending a single placeholder "next match" row per team dated `as_of_date`.
Because every rolling feature is computed with `shift(1)` before
rolling/cumsum, the placeholder's own (missing) result never leaks into its
own features — it just represents "what a team carries into their next
match," which is exactly what's needed to score an unplayed fixture.

## Model

An `XGBClassifier` (`multi:softprob`, 3 classes) tuned via
`RandomizedSearchCV` with `TimeSeriesSplit` cross-validation — a plain
random or k-fold split would let the model validate on matches that
happened *before* some of its training data, which would leak information
it wouldn't have at real prediction time.

Class imbalance (home wins are more common than away wins in most leagues)
is handled with `compute_sample_weight(class_weight="balanced")`, combined
with an exponential recency weight (3-year half-life) so recent seasons
matter more than a 2005 result.

## Backtest accuracy

The dashboard's "Backtest accuracy (last 30)" metric re-runs the exact same
feature pipeline over the most recent played matches and compares the
model's top-probability class against the actual result — a live sanity
check that the deployed model still performs the way training suggested,
computed as a single batch `predict_proba` call rather than row-by-row (see
the comment in `accuracy_tracker()` in `dashboard.py` for why that
distinction matters — pulling a single row out of a DataFrame via
`iterrows()` silently upcasts all of its columns to `object` dtype, which
XGBoost rejects).