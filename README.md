# J-League Match & Fan Reaction Predictor

A Streamlit dashboard that predicts J1 League match outcomes (home win / draw /
away win) with an XGBoost model trained on historical results, team form, Elo
ratings, and squad market-value data — plus an optional local-LLM feature that
simulates how different fan personas might react to a given matchup.

## How it fits together

```
scrape.py        →  CSV/*.csv           Scrapes Transfermarkt (standings, fixtures,
  (Prefect flow)                        goal minutes, cards, squad summaries) for
                                         each J1 League season.

db_setup.py       →  j1_league.duckdb   Loads those CSVs into DuckDB tables and
                                         builds played_matches / upcoming_matches
                                         views (splits fixtures by whether both
                                         scores are known yet).

train_model.py     →  outcome_predictor.joblib
                                         Builds point-in-time rolling-form, Elo,
                                         and squad features (no future leakage),
                                         tunes an XGBoost classifier, and saves
                                         the model + feature columns + label map.

dashboard.py        (streamlit run)     Loads the DuckDB tables + trained model,
                                         shows a backtest accuracy table, lets you
                                         pick a matchup, and displays predicted
                                         win/draw/loss probabilities.

fan_simulator.py                        Optional: asks a local Ollama model to
                                         write short in-character reactions from
                                         home fans / away fans / a neutral pundit,
                                         given the predicted probabilities.
```

`train_model.py`'s `build_features_from_matches()` is the single source of
truth for feature engineering — both the training run and the dashboard's
backtest call it, so they can't drift out of sync.

## Running it locally

```bash
python -m venv venv && source venv/bin/activate   # or conda/mamba, your call
pip install -r requirements.txt

# 1. Scrape source data (slow — hits Transfermarkt once per club per season)
python scrape.py

# 2. Load the scraped CSVs into DuckDB
python db_setup.py

# 3. Train the model
python train_model.py

# 4. Launch the dashboard
streamlit run dashboard.py
```

Steps 1–3 only need to be re-run when you want fresh data or a retrained
model — the dashboard just reads `j1_league.duckdb` and
`outcome_predictor.joblib`.

### Optional: fan reaction simulation

This feature calls a **local** Ollama server (`http://localhost:11434`), not
a hosted API. To use it:

```bash
ollama pull llama3.1     # or edit MODEL_NAME in fan_simulator.py
ollama serve
```

If Ollama isn't running, the dashboard detects that automatically and shows
an info message instead of a broken widget — see [Fan Reactions](docs/fan-reactions.md)
for details.

## Deploying for free

The dashboard itself (predictions, backtest, charts) works fine on
[Streamlit Community Cloud](https://share.streamlit.io) with no code changes.
See [docs/deployment.md](docs/deployment.md) for the full walkthrough,
including why fan reactions are disabled automatically on hosted deployments.

## Project structure

```
.
├── scrape.py              # Transfermarkt scraper (Prefect flow) → CSV/*.csv
├── db_setup.py             # CSV → j1_league.duckdb
├── train_model.py          # Feature engineering + XGBoost training → outcome_predictor.joblib
├── dashboard.py             # Streamlit app
├── fan_simulator.py         # Local-LLM fan reaction personas (optional)
├── requirements.txt
├── mkdocs.yml
└── docs/
    ├── index.md
    ├── getting-started.md
    ├── architecture.md
    ├── fan-reactions.md
    └── deployment.md
```

## Docs

View the [documentation](https://vikasreddy85.github.io/j1-soccer-league-dashboard/) to learn more about the project.
