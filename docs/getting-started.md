# Getting Started

## 1. Install dependencies

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

XGBoost needs the OpenMP runtime on macOS:

```bash
brew install libomp
```

(If you'd rather avoid that dependency, set `USE_XGBOOST = False` at the top
of `train_model.py` to fall back to scikit-learn's
`HistGradientBoostingClassifier`, which is comparable on tabular data this
size and has no native dependency.)

## 2. Build the dataset

```bash
python scrape.py
```

This runs a Prefect flow that scrapes Transfermarkt for every J1 League
season from 2005 onward: standings, goal-minute distribution, disciplinary
stats, full fixture lists, and per-club squad summaries (market value,
average age, foreign-player count). It writes CSVs to `./CSV/`.

!!! warning "Be polite to the source site"
    The squad-summary scrape is one HTTP request per club per season and
    throttles itself with a 1-second sleep between requests. A full run
    across ~20 seasons takes a while — that's expected.

## 3. Load it into DuckDB

```bash
python db_setup.py
```

Creates `j1_league.duckdb` with `standings`, `goal_minutes`, `card_stats`,
`fixtures`, and `squad_summary` tables, plus two convenience views:

- `played_matches` — fixtures where both scores are known, with a derived `Result` column
- `upcoming_matches` — fixtures still missing a score

## 4. Train the model

```bash
python train_model.py
```

Builds rolling-form, Elo, and squad-value features, runs a time-based
train/test split (so the model is never evaluated on matches that happened
before its training window), tunes hyperparameters with
`RandomizedSearchCV` + `TimeSeriesSplit`, and saves everything the dashboard
needs to `outcome_predictor.joblib`.

## 5. Run the dashboard

```bash
streamlit run dashboard.py
```

Opens at `http://localhost:8501`. You'll see:

- Dataset size and a backtest accuracy metric (last 30 played matches)
- A matchup picker (upcoming fixture or any custom team pairing)
- Predicted win/draw/loss probabilities as a bar chart
- Optional simulated fan reactions (see [Fan Reactions](fan-reactions.md))