# J-League Match & Fan Reaction Predictor

A Streamlit dashboard that predicts J1 League match outcomes — **home win /
draw / away win** — using an XGBoost model trained on historical results,
rolling team form, Elo ratings, and squad market-value data. It also has an
optional feature that uses a local LLM (via Ollama) to simulate how different
fan personas might react to a given matchup.

## What's in this project

| Component | Purpose |
|---|---|
| `scrape.py` | Prefect flow that scrapes Transfermarkt for standings, fixtures, goal-minute distribution, card stats, and squad summaries |
| `db_setup.py` | Loads the scraped CSVs into a DuckDB file and builds `played_matches` / `upcoming_matches` views |
| `train_model.py` | Point-in-time feature engineering (no future leakage) + XGBoost training |
| `dashboard.py` | The Streamlit app: backtest accuracy, matchup picker, win-probability chart, fan reactions |
| `fan_simulator.py` | Persona-based reaction generation via a local Ollama model |

## Quick links

- New to the project? Start with [Getting Started](getting-started.md).
- Want to understand the feature pipeline? See [Architecture & Data Pipeline](architecture.md).
- Curious about the LLM fan-reaction feature and why it's disabled on some deployments? See [Fan Reactions](fan-reactions.md).
- Ready to put the dashboard online? See [Deploying for Free](deployment.md).

!!! note "Single source of truth for features"
    `train_model.py`'s `build_features_from_matches()` is used by **both**
    the training script and the dashboard's backtest. This guarantees the
    features the model was trained on and the features used to score
    historical matches in the dashboard can never silently drift apart.