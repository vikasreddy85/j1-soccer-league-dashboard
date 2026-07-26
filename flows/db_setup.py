import duckdb
import os

DB_PATH = "./j1_league.duckdb"
CSV_DIR = "./CSV"


def load_csv_as_table(con, csv_name: str, table_name: str):
    path = os.path.join(CSV_DIR, csv_name)
    if not os.path.exists(path):
        print(f"Skipping {table_name}: {path} not found yet")
        return
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{path}', header=True)
    """)
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Loaded {table_name}: {count} rows")


def build_database():
    con = duckdb.connect(DB_PATH)

    load_csv_as_table(con, "j1_league_table.csv", "standings")
    load_csv_as_table(con, "j1_league_goal_minutes.csv", "goal_minutes")
    load_csv_as_table(con, "j1_league_card_stats.csv", "card_stats")
    load_csv_as_table(con, "j1_league_fixtures.csv", "fixtures")
    load_csv_as_table(con, "j1_league_squad_summary.csv", "squad_summary")

    if _table_exists(con, "fixtures"):
        con.execute("""
            CREATE OR REPLACE VIEW played_matches AS
            SELECT *,
                   TRY_CAST(HomeGoals AS INTEGER) AS HomeGoalsInt,
                   TRY_CAST(AwayGoals AS INTEGER) AS AwayGoalsInt,
                   CASE
                       WHEN TRY_CAST(HomeGoals AS INTEGER) > TRY_CAST(AwayGoals AS INTEGER) THEN 'HOME_WIN'
                       WHEN TRY_CAST(HomeGoals AS INTEGER) < TRY_CAST(AwayGoals AS INTEGER) THEN 'AWAY_WIN'
                       WHEN TRY_CAST(HomeGoals AS INTEGER) = TRY_CAST(AwayGoals AS INTEGER) THEN 'DRAW'
                   END AS Result,
                   TRY_CAST(Date AS DATE) AS MatchDate
            FROM fixtures
            WHERE TRY_CAST(HomeGoals AS INTEGER) IS NOT NULL
              AND TRY_CAST(AwayGoals AS INTEGER) IS NOT NULL
        """)
        print("Created view: played_matches")

        con.execute("""
            CREATE OR REPLACE VIEW upcoming_matches AS
            SELECT *, TRY_CAST(Date AS DATE) AS MatchDate
            FROM fixtures
            WHERE TRY_CAST(HomeGoals AS INTEGER) IS NULL
               OR TRY_CAST(AwayGoals AS INTEGER) IS NULL
        """)
        print("Created view: upcoming_matches")

    con.close()
    print(f"\nDatabase ready at {DB_PATH}")


def _table_exists(con, name: str) -> bool:
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [name]
    ).fetchone()[0]
    return result > 0


if __name__ == "__main__":
    build_database()