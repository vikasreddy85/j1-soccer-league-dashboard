"""
Live dashboard for the J-League predictor project.

Run with:
    streamlit run dashboard.py

Requires: streamlit, duckdb, pandas, joblib, plotly
    pip install streamlit duckdb pandas joblib plotly --break-system-packages
"""
import duckdb
import pandas as pd
import joblib
import streamlit as st
import plotly.graph_objects as go

from fan_simulator import simulate_reactions, check_ollama_available
from train_model import (
    build_features_from_matches,
    build_matchup_features,
    compute_elo_features,
)

DB_PATH = "./j1_league.duckdb"
MODEL_PATH = "./outcome_predictor.joblib"

st.set_page_config(page_title="J-League Predictor", layout="wide")


@st.cache_resource
def load_model():
    bundle = joblib.load(MODEL_PATH)
    return bundle["model"], bundle["feature_cols"], bundle["label_map"], bundle["squad_medians"]


@st.cache_data
def load_matches():
    con = duckdb.connect(DB_PATH, read_only=True)
    played = con.execute("SELECT * FROM played_matches ORDER BY MatchDate").df()
    upcoming = con.execute("SELECT * FROM upcoming_matches ORDER BY MatchDate").df()
    squad = con.execute("SELECT Year, Team, SquadValueEUR_M, AvgAge, ForeignPlayers FROM squad_summary").df()
    con.close()
    played["MatchDate"] = pd.to_datetime(played["MatchDate"])
    if not upcoming.empty:
        upcoming["MatchDate"] = pd.to_datetime(upcoming["MatchDate"])
    return played, upcoming, squad


def accuracy_tracker(model, feature_cols, played: pd.DataFrame, squad: pd.DataFrame, n_matches: int = 30):
    """Backtest: for the most recent already-played matches, compare the
    model's top prediction against the actual result. Uses the exact same
    feature pipeline as training, so it can't drift out of sync again."""
    features, _, _ = build_features_from_matches(played, squad)
    features = features.dropna(subset=feature_cols).sort_values("MatchDate").tail(n_matches)

    label_names = ["HOME_WIN", "DRAW", "AWAY_WIN"]

    # Predict on the full feature slice in one batch call. Pulling single rows
    # out via iterrows() (r[feature_cols].to_frame().T) turns each row into a
    # Series first, which forces every column to a common dtype (object, since
    # the row mixes ints/floats) — that's what was tripping up XGBoost, which
    # only accepts numeric/bool/category dtypes. Slicing features[feature_cols]
    # keeps each column's real dtype intact.
    X = features[feature_cols]
    probs = model.predict_proba(X)
    predicted = [label_names[i] for i in probs.argmax(axis=1)]

    backtest_df = pd.DataFrame({
        "Date": features["MatchDate"].values,
        "Match": features["HomeTeam"].values + " vs " + features["AwayTeam"].values,
        "Predicted": predicted,
        "Actual": features["Result"].values,
    })
    backtest_df["Correct"] = backtest_df["Predicted"] == backtest_df["Actual"]
    accuracy = backtest_df["Correct"].mean() if not backtest_df.empty else 0
    return accuracy, backtest_df


# --------------------------------------------------------------------------
# UI
# --------------------------------------------------------------------------
st.title("J-League match & fan reaction predictor")

try:
    model, feature_cols, label_map, squad_medians = load_model()
except FileNotFoundError:
    st.error("Model not found. Run `python train_model.py` first.")
    st.stop()

played, upcoming, squad = load_matches()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Matches in database", len(played) + len(upcoming))
col2.metric("Played matches", len(played))
col3.metric("Upcoming matches", len(upcoming))

acc, backtest_df = accuracy_tracker(model, feature_cols, played, squad)
col4.metric("Backtest accuracy (last 30)", f"{acc:.0%}")

st.divider()

st.subheader("Pick a matchup")
mode = st.radio("Source", ["Upcoming fixture", "Custom matchup"], horizontal=True)

teams = sorted(played["HomeTeam"].dropna().unique().tolist())

if mode == "Upcoming fixture" and not upcoming.empty:
    fixture_labels = upcoming.apply(
        lambda r: f"{r['HomeTeam']} vs {r['AwayTeam']} ({r['MatchDate'].date()})", axis=1
    )
    choice = st.selectbox("Fixture", fixture_labels)
    idx = fixture_labels[fixture_labels == choice].index[0]
    home_team = upcoming.loc[idx, "HomeTeam"]
    away_team = upcoming.loc[idx, "AwayTeam"]
    match_date = upcoming.loc[idx, "MatchDate"]
else:
    home_team = st.selectbox("Home team", teams, key="home")
    away_team = st.selectbox("Away team", teams, key="away")
    match_date = pd.Timestamp.now().normalize()

if st.button("Predict"):
    _, elo_ratings = compute_elo_features(played)
    row = build_matchup_features(
        home_team, away_team, match_date, played, squad, elo_ratings, squad_medians, feature_cols
    )

    if row is None:
        st.warning("Not enough match history for one of these teams yet.")
    else:
        probs = model.predict_proba(row)[0]
        home_p, draw_p, away_p = probs

        st.subheader(f"{home_team} vs {away_team}")
        fig = go.Figure(go.Bar(
            x=[home_p, draw_p, away_p],
            y=[home_team, "Draw", away_team],
            orientation="h",
        ))
        fig.update_layout(
            xaxis_title="Win probability", height=250,
            margin=dict(l=10, r=10, t=10, b=10)
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Simulated fan reactions")
        if check_ollama_available():
            with st.spinner("Simulating fan personas..."):
                reactions = simulate_reactions(home_team, away_team, home_p, draw_p, away_p)
            for r in reactions:
                st.markdown(f"**{r['label']}**  \n{r['text']}")
        else:
            st.info(
                "Fan reaction simulation is unavailable right now. This feature calls a "
                "local Ollama instance, so it only works when the dashboard is run on "
                "your own machine with Ollama installed and running "
                "(`ollama pull llama3.1`, then `ollama serve`). It's disabled automatically "
                "on hosted deployments like Streamlit Community Cloud, which have no "
                "local Ollama to reach."
            )

st.divider()
st.subheader("Prediction accuracy — last 30 played matches")
st.dataframe(backtest_df, use_container_width=True)