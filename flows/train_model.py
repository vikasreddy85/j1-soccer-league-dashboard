from collections import defaultdict

import duckdb
import joblib
import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    log_loss,
)
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.utils.class_weight import compute_sample_weight

USE_XGBOOST = True

if USE_XGBOOST:
    from xgboost import XGBClassifier
else:
    from sklearn.ensemble import HistGradientBoostingClassifier

DB_PATH = "./j1_league.duckdb"
MODEL_PATH = "./outcome_predictor.joblib"
FORM_WINDOW = 5
FORM_WINDOW_SHORT = 3
FORM_WINDOW_LONG = 10
H2H_WINDOW = 5
ELO_HOME_ADVANTAGE = 60
ELO_SCALE = 400
TEST_SIZE_FRACTION = 0.2
VAL_SIZE_FRACTION = 0.15
RANDOM_STATE = 42
N_SEARCH_ITER = 60
EARLY_STOPPING_ROUNDS = 50
CLASS_BALANCE_STRENGTH = 0.0
CLASS_BALANCE_CANDIDATES = [0.0, 0.15, 0.3]

ENSEMBLE_BLEND_WEIGHTS = [1.0, 0.85, 0.7, 0.5]

SQUAD_BASE_COLS = ["SquadValueEUR_M", "AvgAge", "ForeignPlayers"]

FEATURE_COLS = [
    "HomeFormPoints", "HomeFormPointsShort", "HomeFormPointsLong",
    "HomeFormGoalsFor", "HomeFormGoalsAgainst", "HomeFormGoalDiff", "HomeMatchesPlayedSoFar",
    "AwayFormPoints", "AwayFormPointsShort", "AwayFormPointsLong",
    "AwayFormGoalsFor", "AwayFormGoalsAgainst", "AwayFormGoalDiff", "AwayMatchesPlayedSoFar",
    "HomeSpecificFormPoints", "AwaySpecificFormPoints",
    "HomeH2HFormPoints", "AwayH2HFormPoints",
    "HomeUnbeatenStreak", "AwayUnbeatenStreak",
    "HomeDaysSinceLastMatch", "AwayDaysSinceLastMatch",
    "HomeSeasonPointsSoFar", "AwaySeasonPointsSoFar", "HomeSeasonGoalDiffSoFar", "AwaySeasonGoalDiffSoFar",
    "HomeEloPre", "AwayEloPre", "EloDiff", "EloWinProbHome",
    "HomeSquadValue", "HomeAvgAge", "HomeForeignPlayers",
    "AwaySquadValue", "AwayAvgAge", "AwayForeignPlayers",
    "SquadValueDiff", "AvgAgeDiff", "ForeignPlayersDiff",
]


def elo_win_prob(home_elo, away_elo, home_advantage=ELO_HOME_ADVANTAGE, scale=ELO_SCALE):
    return 1 / (1 + 10 ** (-((home_elo + home_advantage) - away_elo) / scale))


def load_played_matches():
    con = duckdb.connect(DB_PATH)
    df = con.execute("""
        SELECT Year, Matchday, MatchDate, HomeTeam, HomeClubID, AwayTeam, AwayClubID,
               HomeGoalsInt, AwayGoalsInt, Result
        FROM played_matches
        ORDER BY MatchDate
    """).df()
    con.close()
    df["MatchDate"] = pd.to_datetime(df["MatchDate"])
    return df


def load_squad_summary():
    con = duckdb.connect(DB_PATH)
    df = con.execute("""
        SELECT Year, Team, SquadValueEUR_M, AvgAge, ForeignPlayers
        FROM squad_summary
    """).df()
    con.close()
    return df


def load_upcoming_matches():
    con = duckdb.connect(DB_PATH)
    df = con.execute("SELECT * FROM upcoming_matches ORDER BY MatchDate").df()
    con.close()
    if not df.empty:
        df["MatchDate"] = pd.to_datetime(df["MatchDate"])
    return df


def compute_elo_features(matches, k_factor=20, home_advantage=ELO_HOME_ADVANTAGE, initial_rating=1500):
    matches = matches.sort_values("MatchDate").reset_index(drop=True).copy()
    ratings = defaultdict(lambda: initial_rating)
    home_elo_pre, away_elo_pre, win_prob_home = [], [], []

    for row in matches.itertuples():
        home_r = ratings[row.HomeTeam]
        away_r = ratings[row.AwayTeam]
        home_elo_pre.append(home_r)
        away_elo_pre.append(away_r)

        expected_home = elo_win_prob(home_r, away_r, home_advantage)
        win_prob_home.append(expected_home)

        if row.Result == "HOME_WIN":
            actual_home = 1.0
        elif row.Result == "DRAW":
            actual_home = 0.5
        else:
            actual_home = 0.0

        goal_margin = abs(row.HomeGoalsInt - row.AwayGoalsInt)
        mov_multiplier = np.log(goal_margin + 1) + 1
        delta = k_factor * mov_multiplier * (actual_home - expected_home)

        ratings[row.HomeTeam] = home_r + delta
        ratings[row.AwayTeam] = away_r - delta

    matches["HomeEloPre"] = home_elo_pre
    matches["AwayEloPre"] = away_elo_pre
    matches["EloDiff"] = matches["HomeEloPre"] - matches["AwayEloPre"]
    matches["EloWinProbHome"] = win_prob_home
    return matches, dict(ratings)


def build_team_match_log(matches: pd.DataFrame) -> pd.DataFrame:
    home = matches.rename(columns={
        "HomeTeam": "Team", "AwayTeam": "Opponent",
        "HomeGoalsInt": "GoalsFor", "AwayGoalsInt": "GoalsAgainst"
    }).copy()
    home["IsHome"] = 1
    home["Points"] = home["Result"].map({"HOME_WIN": 3, "DRAW": 1, "AWAY_WIN": 0})

    away = matches.rename(columns={
        "AwayTeam": "Team", "HomeTeam": "Opponent",
        "AwayGoalsInt": "GoalsFor", "HomeGoalsInt": "GoalsAgainst"
    }).copy()
    away["IsHome"] = 0
    away["Points"] = away["Result"].map({"AWAY_WIN": 3, "DRAW": 1, "HOME_WIN": 0})

    keep = ["MatchDate", "Year", "Team", "Opponent", "GoalsFor", "GoalsAgainst", "IsHome", "Points"]
    log = pd.concat([home[keep], away[keep]], ignore_index=True)
    return log.sort_values(["Team", "MatchDate"])


def rolling_unbeaten_streak(points):
    streak, current = [], 0
    for p in points:
        streak.append(current)
        current = current + 1 if p >= 1 else 0
    return streak


def add_rolling_form(log: pd.DataFrame, window: int = FORM_WINDOW) -> pd.DataFrame:
    log = log.sort_values(["Team", "MatchDate"]).copy()
    log["GoalDiff"] = log["GoalsFor"] - log["GoalsAgainst"]
    grouped = log.groupby("Team")

    log["FormPoints"] = grouped["Points"].transform(
        lambda s: s.shift(1).rolling(window, min_periods=1).mean()
    )
    log["FormPointsShort"] = grouped["Points"].transform(
        lambda s: s.shift(1).rolling(FORM_WINDOW_SHORT, min_periods=1).mean()
    )
    log["FormPointsLong"] = grouped["Points"].transform(
        lambda s: s.shift(1).rolling(FORM_WINDOW_LONG, min_periods=1).mean()
    )
    log["FormGoalsFor"] = grouped["GoalsFor"].transform(
        lambda s: s.shift(1).rolling(window, min_periods=1).mean()
    )
    log["FormGoalsAgainst"] = grouped["GoalsAgainst"].transform(
        lambda s: s.shift(1).rolling(window, min_periods=1).mean()
    )
    log["FormGoalDiff"] = grouped["GoalDiff"].transform(
        lambda s: s.shift(1).rolling(window, min_periods=1).mean()
    )
    log["MatchesPlayedSoFar"] = grouped.cumcount()
    log["UnbeatenStreak"] = grouped["Points"].transform(
        lambda s: pd.Series(rolling_unbeaten_streak(s.tolist()), index=s.index)
    )
    log["DaysSinceLastMatch"] = grouped["MatchDate"].transform(lambda s: s.diff().dt.days)

    season_grouped = log.groupby(["Year", "Team"])
    log["SeasonPointsSoFar"] = season_grouped["Points"].transform(lambda s: s.shift(1).cumsum()).fillna(0)
    log["SeasonGoalDiffSoFar"] = season_grouped["GoalDiff"].transform(lambda s: s.shift(1).cumsum()).fillna(0)
    return log


def add_home_away_specific_form(log: pd.DataFrame, window: int = FORM_WINDOW):
    home_only = log[log["IsHome"] == 1].sort_values(["Team", "MatchDate"]).copy()
    home_only["HomeSpecificFormPoints"] = home_only.groupby("Team")["Points"].transform(
        lambda s: s.shift(1).rolling(window, min_periods=1).mean()
    )
    away_only = log[log["IsHome"] == 0].sort_values(["Team", "MatchDate"]).copy()
    away_only["AwaySpecificFormPoints"] = away_only.groupby("Team")["Points"].transform(
        lambda s: s.shift(1).rolling(window, min_periods=1).mean()
    )
    home_specific = home_only[["MatchDate", "Team", "HomeSpecificFormPoints"]].rename(columns={"Team": "HomeTeam"})
    away_specific = away_only[["MatchDate", "Team", "AwaySpecificFormPoints"]].rename(columns={"Team": "AwayTeam"})
    return home_specific, away_specific


def add_head_to_head_form(log: pd.DataFrame, window: int = H2H_WINDOW) -> pd.DataFrame:
    h2h = log.sort_values(["Team", "Opponent", "MatchDate"]).copy()
    h2h["H2HFormPoints"] = h2h.groupby(["Team", "Opponent"])["Points"].transform(
        lambda s: s.shift(1).rolling(window, min_periods=1).mean()
    )
    return h2h[["MatchDate", "Team", "Opponent", "H2HFormPoints"]]


def compute_h2h_form(played: pd.DataFrame, home_team: str, away_team: str, window: int = H2H_WINDOW):
    log = build_team_match_log(played)
    home_meetings = log[(log["Team"] == home_team) & (log["Opponent"] == away_team)].sort_values("MatchDate")
    away_meetings = log[(log["Team"] == away_team) & (log["Opponent"] == home_team)].sort_values("MatchDate")
    home_h2h = home_meetings["Points"].tail(window).mean() if not home_meetings.empty else np.nan
    away_h2h = away_meetings["Points"].tail(window).mean() if not away_meetings.empty else np.nan
    return home_h2h, away_h2h


def print_outcome_distribution_by_year(features):
    dist = features.groupby("Year")["Result"].value_counts(normalize=True).unstack().fillna(0)
    print("\nActual outcome distribution by year (checks for drift, e.g. rising parity):")
    print(dist.round(3).to_string())


def print_test_accuracy_by_year(features, X_test, y_test, preds):
    diag = pd.DataFrame({
        "Year": features.loc[X_test.index, "Year"].values,
        "Correct": (y_test.values == preds),
    })
    print("\nTest-set accuracy by year:")
    print(diag.groupby("Year")["Correct"].agg(["mean", "count"]).round(3).to_string())


def build_features_from_matches(matches: pd.DataFrame, squad: pd.DataFrame):
    matches = matches.copy()
    matches["MatchDate"] = pd.to_datetime(matches["MatchDate"])
    matches_elo, _ = compute_elo_features(matches)

    log = build_team_match_log(matches)
    log = add_rolling_form(log)
    home_specific, away_specific = add_home_away_specific_form(log)
    h2h = add_head_to_head_form(log)

    home_form = log[log["IsHome"] == 1][[
        "MatchDate", "Team", "FormPoints", "FormPointsShort", "FormPointsLong",
        "FormGoalsFor", "FormGoalsAgainst", "FormGoalDiff",
        "MatchesPlayedSoFar", "UnbeatenStreak", "DaysSinceLastMatch", "SeasonPointsSoFar", "SeasonGoalDiffSoFar"
    ]].rename(columns={
        "Team": "HomeTeam", "FormPoints": "HomeFormPoints",
        "FormPointsShort": "HomeFormPointsShort", "FormPointsLong": "HomeFormPointsLong",
        "FormGoalsFor": "HomeFormGoalsFor",
        "FormGoalsAgainst": "HomeFormGoalsAgainst", "FormGoalDiff": "HomeFormGoalDiff",
        "MatchesPlayedSoFar": "HomeMatchesPlayedSoFar", "UnbeatenStreak": "HomeUnbeatenStreak",
        "DaysSinceLastMatch": "HomeDaysSinceLastMatch", "SeasonPointsSoFar": "HomeSeasonPointsSoFar",
        "SeasonGoalDiffSoFar": "HomeSeasonGoalDiffSoFar"
    })
    away_form = log[log["IsHome"] == 0][[
        "MatchDate", "Team", "FormPoints", "FormPointsShort", "FormPointsLong",
        "FormGoalsFor", "FormGoalsAgainst", "FormGoalDiff",
        "MatchesPlayedSoFar", "UnbeatenStreak", "DaysSinceLastMatch", "SeasonPointsSoFar", "SeasonGoalDiffSoFar"
    ]].rename(columns={
        "Team": "AwayTeam", "FormPoints": "AwayFormPoints",
        "FormPointsShort": "AwayFormPointsShort", "FormPointsLong": "AwayFormPointsLong",
        "FormGoalsFor": "AwayFormGoalsFor",
        "FormGoalsAgainst": "AwayFormGoalsAgainst", "FormGoalDiff": "AwayFormGoalDiff",
        "MatchesPlayedSoFar": "AwayMatchesPlayedSoFar", "UnbeatenStreak": "AwayUnbeatenStreak",
        "DaysSinceLastMatch": "AwayDaysSinceLastMatch", "SeasonPointsSoFar": "AwaySeasonPointsSoFar",
        "SeasonGoalDiffSoFar": "AwaySeasonGoalDiffSoFar"
    })

    home_h2h = h2h.rename(columns={"Team": "HomeTeam", "Opponent": "AwayTeam", "H2HFormPoints": "HomeH2HFormPoints"})
    away_h2h = h2h.rename(columns={"Team": "AwayTeam", "Opponent": "HomeTeam", "H2HFormPoints": "AwayH2HFormPoints"})

    features = matches_elo.merge(home_form, on=["MatchDate", "HomeTeam"], how="left")
    features = features.merge(away_form, on=["MatchDate", "AwayTeam"], how="left")
    features = features.merge(home_specific, on=["MatchDate", "HomeTeam"], how="left")
    features = features.merge(away_specific, on=["MatchDate", "AwayTeam"], how="left")
    features = features.merge(home_h2h, on=["MatchDate", "HomeTeam", "AwayTeam"], how="left")
    features = features.merge(away_h2h, on=["MatchDate", "HomeTeam", "AwayTeam"], how="left")

    core_form_cols = ["HomeFormPoints", "AwayFormPoints", "HomeFormGoalsFor", "AwayFormGoalsFor"]
    features = features.dropna(subset=core_form_cols)

    features["HomeSpecificFormPoints"] = features["HomeSpecificFormPoints"].fillna(features["HomeFormPoints"])
    features["AwaySpecificFormPoints"] = features["AwaySpecificFormPoints"].fillna(features["AwayFormPoints"])
    features["HomeH2HFormPoints"] = features["HomeH2HFormPoints"].fillna(features["HomeFormPoints"])
    features["AwayH2HFormPoints"] = features["AwayH2HFormPoints"].fillna(features["AwayFormPoints"])
    features["HomeFormPointsShort"] = features["HomeFormPointsShort"].fillna(features["HomeFormPoints"])
    features["AwayFormPointsShort"] = features["AwayFormPointsShort"].fillna(features["AwayFormPoints"])
    features["HomeFormPointsLong"] = features["HomeFormPointsLong"].fillna(features["HomeFormPoints"])
    features["AwayFormPointsLong"] = features["AwayFormPointsLong"].fillna(features["AwayFormPoints"])
    features["HomeDaysSinceLastMatch"] = features["HomeDaysSinceLastMatch"].fillna(
        features["HomeDaysSinceLastMatch"].median()
    )
    features["AwayDaysSinceLastMatch"] = features["AwayDaysSinceLastMatch"].fillna(
        features["AwayDaysSinceLastMatch"].median()
    )

    squad_medians = squad[SQUAD_BASE_COLS].median().to_dict()

    home_squad = squad.rename(columns={
        "Team": "HomeTeam", "SquadValueEUR_M": "HomeSquadValue",
        "AvgAge": "HomeAvgAge", "ForeignPlayers": "HomeForeignPlayers"
    })
    away_squad = squad.rename(columns={
        "Team": "AwayTeam", "SquadValueEUR_M": "AwaySquadValue",
        "AvgAge": "AwayAvgAge", "ForeignPlayers": "AwayForeignPlayers"
    })
    features = features.merge(home_squad, on=["Year", "HomeTeam"], how="left")
    features = features.merge(away_squad, on=["Year", "AwayTeam"], how="left")

    squad_cols = ["HomeSquadValue", "HomeAvgAge", "HomeForeignPlayers",
                  "AwaySquadValue", "AwayAvgAge", "AwayForeignPlayers"]
    matched_pct = features[squad_cols].notna().all(axis=1).mean()
    print(f"Squad data matched for {matched_pct:.0%} of matches "
          f"(team name mismatches between pages reduce this — worth checking if low)")

    features["HomeSquadValue"] = features["HomeSquadValue"].fillna(squad_medians["SquadValueEUR_M"])
    features["AwaySquadValue"] = features["AwaySquadValue"].fillna(squad_medians["SquadValueEUR_M"])
    features["HomeAvgAge"] = features["HomeAvgAge"].fillna(squad_medians["AvgAge"])
    features["AwayAvgAge"] = features["AwayAvgAge"].fillna(squad_medians["AvgAge"])
    features["HomeForeignPlayers"] = features["HomeForeignPlayers"].fillna(squad_medians["ForeignPlayers"])
    features["AwayForeignPlayers"] = features["AwayForeignPlayers"].fillna(squad_medians["ForeignPlayers"])

    features["SquadValueDiff"] = features["HomeSquadValue"] - features["AwaySquadValue"]
    features["AvgAgeDiff"] = features["HomeAvgAge"] - features["AwayAvgAge"]
    features["ForeignPlayersDiff"] = features["HomeForeignPlayers"] - features["AwayForeignPlayers"]

    return features, FEATURE_COLS, squad_medians


def build_feature_matrix():
    matches = load_played_matches()
    if matches.empty:
        raise RuntimeError("No played matches found — check that fixtures were scraped and loaded.")
    squad = load_squad_summary()
    features, feature_cols, squad_medians = build_features_from_matches(matches, squad)
    X = features[feature_cols]
    y = features["Result"]
    return X, y, features, feature_cols, squad_medians


def build_current_state(played: pd.DataFrame, as_of_date) -> pd.DataFrame:
    log = build_team_match_log(played)
    teams = log["Team"].unique()
    latest_year = log["Year"].max()

    def make_placeholder(is_home):
        return pd.DataFrame({
            "MatchDate": as_of_date, "Year": latest_year, "Team": teams,
            "Opponent": None, "GoalsFor": np.nan, "GoalsAgainst": np.nan,
            "IsHome": is_home, "Points": np.nan,
        })

    overall_ext = pd.concat([log, make_placeholder(1)], ignore_index=True)
    overall_ext = add_rolling_form(overall_ext)
    overall_state = overall_ext[overall_ext["MatchDate"] == as_of_date].set_index("Team")

    home_only = log[log["IsHome"] == 1]
    home_ext = pd.concat([home_only, make_placeholder(1)], ignore_index=True).sort_values(["Team", "MatchDate"])
    home_ext["HomeSpecificFormPoints"] = home_ext.groupby("Team")["Points"].transform(
        lambda s: s.shift(1).rolling(FORM_WINDOW, min_periods=1).mean()
    )
    home_state = home_ext[home_ext["MatchDate"] == as_of_date].set_index("Team")["HomeSpecificFormPoints"]

    away_only = log[log["IsHome"] == 0]
    away_ext = pd.concat([away_only, make_placeholder(0)], ignore_index=True).sort_values(["Team", "MatchDate"])
    away_ext["AwaySpecificFormPoints"] = away_ext.groupby("Team")["Points"].transform(
        lambda s: s.shift(1).rolling(FORM_WINDOW, min_periods=1).mean()
    )
    away_state = away_ext[away_ext["MatchDate"] == as_of_date].set_index("Team")["AwaySpecificFormPoints"]

    state = overall_state[[
        "FormPoints", "FormPointsShort", "FormPointsLong",
        "FormGoalsFor", "FormGoalsAgainst", "FormGoalDiff",
        "MatchesPlayedSoFar", "UnbeatenStreak", "DaysSinceLastMatch",
        "SeasonPointsSoFar", "SeasonGoalDiffSoFar",
    ]].copy()
    state["DaysSinceLastMatch"] = state["DaysSinceLastMatch"].fillna(7)
    state["FormPointsShort"] = state["FormPointsShort"].fillna(state["FormPoints"])
    state["FormPointsLong"] = state["FormPointsLong"].fillna(state["FormPoints"])
    state["HomeSpecificFormPoints"] = home_state.reindex(state.index).fillna(state["FormPoints"])
    state["AwaySpecificFormPoints"] = away_state.reindex(state.index).fillna(state["FormPoints"])
    return state


def get_latest_squad_row(squad: pd.DataFrame, team: str):
    rows = squad[squad["Team"] == team].sort_values("Year")
    if rows.empty:
        return None
    return rows.iloc[-1]


def build_matchup_features(home_team, away_team, as_of_date, played, squad, elo_ratings, squad_medians, feature_cols):
    state = build_current_state(played, as_of_date)
    if home_team not in state.index or away_team not in state.index:
        return None

    home, away = state.loc[home_team], state.loc[away_team]
    home_squad_row = get_latest_squad_row(squad, home_team)
    away_squad_row = get_latest_squad_row(squad, away_team)

    def squad_value(row, col):
        if row is not None and pd.notna(row[col]):
            return row[col]
        return squad_medians[col]

    home_h2h, away_h2h = compute_h2h_form(played, home_team, away_team)
    if pd.isna(home_h2h):
        home_h2h = home["FormPoints"]
    if pd.isna(away_h2h):
        away_h2h = away["FormPoints"]

    home_elo = elo_ratings.get(home_team, 1500)
    away_elo = elo_ratings.get(away_team, 1500)
    home_squad_value = squad_value(home_squad_row, "SquadValueEUR_M")
    away_squad_value = squad_value(away_squad_row, "SquadValueEUR_M")
    home_avg_age = squad_value(home_squad_row, "AvgAge")
    away_avg_age = squad_value(away_squad_row, "AvgAge")
    home_foreign = squad_value(home_squad_row, "ForeignPlayers")
    away_foreign = squad_value(away_squad_row, "ForeignPlayers")

    row = {
        "HomeFormPoints": home["FormPoints"],
        "HomeFormPointsShort": home["FormPointsShort"], "HomeFormPointsLong": home["FormPointsLong"],
        "HomeFormGoalsFor": home["FormGoalsFor"],
        "HomeFormGoalsAgainst": home["FormGoalsAgainst"], "HomeFormGoalDiff": home["FormGoalDiff"],
        "HomeMatchesPlayedSoFar": home["MatchesPlayedSoFar"],
        "AwayFormPoints": away["FormPoints"],
        "AwayFormPointsShort": away["FormPointsShort"], "AwayFormPointsLong": away["FormPointsLong"],
        "AwayFormGoalsFor": away["FormGoalsFor"],
        "AwayFormGoalsAgainst": away["FormGoalsAgainst"], "AwayFormGoalDiff": away["FormGoalDiff"],
        "AwayMatchesPlayedSoFar": away["MatchesPlayedSoFar"],
        "HomeSpecificFormPoints": home["HomeSpecificFormPoints"],
        "AwaySpecificFormPoints": away["AwaySpecificFormPoints"],
        "HomeH2HFormPoints": home_h2h, "AwayH2HFormPoints": away_h2h,
        "HomeUnbeatenStreak": home["UnbeatenStreak"], "AwayUnbeatenStreak": away["UnbeatenStreak"],
        "HomeDaysSinceLastMatch": home["DaysSinceLastMatch"], "AwayDaysSinceLastMatch": away["DaysSinceLastMatch"],
        "HomeSeasonPointsSoFar": home["SeasonPointsSoFar"], "AwaySeasonPointsSoFar": away["SeasonPointsSoFar"],
        "HomeSeasonGoalDiffSoFar": home["SeasonGoalDiffSoFar"], "AwaySeasonGoalDiffSoFar": away["SeasonGoalDiffSoFar"],
        "HomeEloPre": home_elo, "AwayEloPre": away_elo, "EloDiff": home_elo - away_elo,
        "EloWinProbHome": elo_win_prob(home_elo, away_elo),
        "HomeSquadValue": home_squad_value, "HomeAvgAge": home_avg_age, "HomeForeignPlayers": home_foreign,
        "AwaySquadValue": away_squad_value, "AwayAvgAge": away_avg_age, "AwayForeignPlayers": away_foreign,
        "SquadValueDiff": home_squad_value - away_squad_value,
        "AvgAgeDiff": home_avg_age - away_avg_age,
        "ForeignPlayersDiff": home_foreign - away_foreign,
    }
    return pd.DataFrame([row])[feature_cols]


def time_based_split(features, X, y, test_fraction=TEST_SIZE_FRACTION, val_fraction=VAL_SIZE_FRACTION):
    sorted_idx = features.sort_values("MatchDate").index
    n = len(sorted_idx)
    test_start = int(n * (1 - test_fraction))
    val_start = int(test_start * (1 - val_fraction))

    train_idx = sorted_idx[:val_start]
    val_idx = sorted_idx[val_start:test_start]
    test_idx = sorted_idx[test_start:]

    X_train, X_val, X_test = X.loc[train_idx], X.loc[val_idx], X.loc[test_idx]
    y_train, y_val, y_test = y.loc[train_idx], y.loc[val_idx], y.loc[test_idx]

    def _describe(idx, label):
        dates = features.loc[idx, "MatchDate"]
        print(f"{label} period: {dates.min().date()} to {dates.max().date()} ({len(idx)} matches)")

    _describe(train_idx, "Train")
    _describe(val_idx, "Validation")
    _describe(test_idx, "Test")
    return X_train, X_val, X_test, y_train, y_val, y_test


def build_sample_weights(y, dates, half_life_days: int = 365 * 3,
                          class_balance_strength: float = CLASS_BALANCE_STRENGTH):
    days_before_end = (dates.max() - dates).dt.days.values
    recency_weight = 0.5 ** (days_before_end / half_life_days)

    if class_balance_strength > 0:
        balanced = compute_sample_weight(class_weight="balanced", y=y)
        class_weight = balanced ** class_balance_strength
    else:
        class_weight = 1.0

    return class_weight * recency_weight


def build_search_space():
    if USE_XGBOOST:
        estimator = XGBClassifier(
            objective="multi:softprob", num_class=3, eval_metric="mlogloss", random_state=RANDOM_STATE
        )
        param_distributions = {
            "n_estimators": [100, 200, 300, 400, 500],
            "max_depth": [3, 4, 5, 6, 8],
            "learning_rate": [0.01, 0.03, 0.05, 0.08, 0.1],
            "subsample": [0.6, 0.8, 1.0],
            "colsample_bytree": [0.6, 0.8, 1.0],
            "min_child_weight": [1, 3, 5, 7],
            "gamma": [0, 0.1, 0.3, 0.5],
        }
    else:
        estimator = HistGradientBoostingClassifier(random_state=RANDOM_STATE)
        param_distributions = {
            "max_iter": [100, 200, 300],
            "max_depth": [3, 4, 5, 6, None],
            "learning_rate": [0.01, 0.03, 0.05, 0.08, 0.1],
            "l2_regularization": [0, 0.1, 0.5, 1.0],
            "min_samples_leaf": [10, 20, 30, 50],
        }
    return estimator, param_distributions


def tune_hyperparameters(X_train, y_train, sample_weight):
    estimator, param_distributions = build_search_space()
    fold_size = max(200, len(X_train) // 10)
    search = RandomizedSearchCV(
        estimator=estimator,
        param_distributions=param_distributions,
        n_iter=N_SEARCH_ITER,
        scoring="neg_log_loss",
        cv=TimeSeriesSplit(n_splits=5, test_size=fold_size),
        random_state=RANDOM_STATE,
        n_jobs=-1,
        verbose=1,
    )
    search.fit(X_train, y_train, sample_weight=sample_weight)
    print(f"\nBest CV log loss: {-search.best_score_:.3f}")
    print(f"Best params: {search.best_params_}\n")
    return search


def train():
    X, y, features, feature_cols, squad_medians = build_feature_matrix()
    print_outcome_distribution_by_year(features)

    label_map = {"HOME_WIN": 0, "DRAW": 1, "AWAY_WIN": 2}
    y_encoded = y.map(label_map)

    X_train, X_val, X_test, y_train, y_val, y_test = time_based_split(features, X, y_encoded)
    train_dates = features.loc[X_train.index, "MatchDate"]
    val_dates = features.loc[X_val.index, "MatchDate"]

    sample_weight_train = build_sample_weights(y_train, train_dates)

    search = tune_hyperparameters(X_train, y_train, sample_weight_train)

    if USE_XGBOOST:
        final_params = search.best_params_.copy()
        final_params["n_estimators"] = 3000
        model = XGBClassifier(
            objective="multi:softprob", num_class=3, eval_metric="mlogloss",
            random_state=RANDOM_STATE, early_stopping_rounds=EARLY_STOPPING_ROUNDS,
            **final_params,
        )
        model.fit(
            X_train, y_train, sample_weight=sample_weight_train,
            eval_set=[(X_val, y_val)], verbose=False,
        )
        print(f"Early stopping selected {model.best_iteration + 1} trees "
              f"(validation mlogloss {model.best_score:.3f})")
    else:
        X_train_full = pd.concat([X_train, X_val])
        y_train_full = pd.concat([y_train, y_val])
        dates_full = pd.concat([train_dates, val_dates])
        sample_weight_full = build_sample_weights(y_train_full, dates_full)
        model = search.best_estimator_
        model.fit(X_train_full, y_train_full, sample_weight=sample_weight_full)

    preds = model.predict(X_test)
    probs = model.predict_proba(X_test)
    acc = accuracy_score(y_test, preds)
    ll = log_loss(y_test, probs, labels=[0, 1, 2])

    majority_class = y_train.value_counts().idxmax()
    baseline_acc = (y_test == majority_class).mean()

    print(f"\nBaseline (always predict majority class): {baseline_acc:.3f}")
    print(f"Model hold-out accuracy:                   {acc:.3f}")
    print(f"Improvement over baseline:                 {acc - baseline_acc:+.3f}")
    print(f"Log loss:                                   {ll:.3f}\n")
    print(classification_report(y_test, preds, target_names=["HOME_WIN", "DRAW", "AWAY_WIN"]))
    print("Confusion matrix (rows=actual, cols=predicted, order HOME_WIN/DRAW/AWAY_WIN):")
    print(confusion_matrix(y_test, preds))
    print_test_accuracy_by_year(features, X_test, y_test, preds)

    if USE_XGBOOST:
        importances = pd.Series(model.feature_importances_, index=feature_cols).sort_values(ascending=False)
    else:
        perm = permutation_importance(model, X_test, y_test, n_repeats=10, random_state=RANDOM_STATE, n_jobs=-1)
        importances = pd.Series(perm.importances_mean, index=feature_cols).sort_values(ascending=False)
    print("\nTop 10 features by importance:")
    print(importances.head(10).to_string())

    prob_df = pd.DataFrame(probs, columns=["P(HOME_WIN)", "P(DRAW)", "P(AWAY_WIN)"])
    prob_df["Actual"] = y_test.map({0: "HOME_WIN", 1: "DRAW", 2: "AWAY_WIN"}).values
    print("\nMean predicted probability by actual outcome (checks calibration, "
          "not just hard-label accuracy):")
    print(prob_df.groupby("Actual")[["P(HOME_WIN)", "P(DRAW)", "P(AWAY_WIN)"]].mean().to_string())

    joblib.dump({
        "model": model,
        "feature_cols": feature_cols,
        "label_map": label_map,
        "squad_medians": squad_medians,
    }, MODEL_PATH)
    print(f"\nSaved model to {MODEL_PATH}")

    return model, acc


if __name__ == "__main__":
    train()