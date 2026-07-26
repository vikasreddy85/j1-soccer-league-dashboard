import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from prefect import flow, task

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/113.0.0 Safari/537.36"
}
BASE = "https://www.transfermarkt.com"
YEARS = [str(y) for y in range(2004, 2026)] 


def get_soup(url: str):
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"Failed to fetch {url} (status {resp.status_code})")
        return None
    return BeautifulSoup(resp.text, "html.parser")


def extract_club_id(href: str):
    if not href:
        return None
    match = re.search(r"/verein/(\d+)", href)
    return match.group(1) if match else None


def parse_market_value(text: str):
    text = text.strip().replace("€", "")
    if not text or text == "-":
        return None
    multiplier = 1
    if text.endswith("bn"):
        multiplier = 1000
        text = text[:-2]
    elif text.endswith("m"):
        multiplier = 1
        text = text[:-1]
    elif text.endswith("k"):
        multiplier = 0.001
        text = text[:-1]
    try:
        return round(float(text) * multiplier, 3)
    except ValueError:
        return None

@task(log_prints=True)
def create_table(year: str):
    url = f"{BASE}/j1-league/tabelle/wettbewerb/JAP1/saison_id/{year}"
    soup = get_soup(url)
    if soup is None:
        return pd.DataFrame()

    table = soup.find("table", {"class": "items"})
    if not table:
        print(f"No standings table found for year {year}")
        return pd.DataFrame()

    rows = table.find_all("tr")[1:]
    data = []
    for row in rows:
        columns = row.find_all("td")
        if len(columns) < 10:
            continue
        try:
            rank = columns[0].text.strip()
            team_link = columns[2].find("a")
            team = team_link.text.strip() if team_link else columns[2].text.strip()
            club_id = extract_club_id(team_link["href"]) if team_link else None
            matches = columns[3].text.strip()
            wins = columns[4].text.strip()
            draws = columns[5].text.strip()
            losses = columns[6].text.strip()
            goals_text = columns[7].text.strip()
            if ":" in goals_text:
                goals_for, goals_against = goals_text.split(":")
            else:
                goals_for, goals_against = None, None
            goal_diff = columns[8].text.strip()
            points = columns[9].text.strip()
            data.append([
                int(year) + 1, rank, team, club_id, matches, wins, draws, losses,
                goals_for, goals_against, goal_diff, points
            ])
        except (IndexError, AttributeError):
            print(f"Skipping a standings row due to missing data for year {year}")

    return pd.DataFrame(data, columns=[
        "Year", "Rank", "Team", "ClubID", "Matches", "Wins", "Draws", "Losses",
        "GoalsFor", "GoalsAgainst", "GoalDiff", "Points"
    ])

@task(log_prints=True)
def create_goal_table(year: str):
    url = f"{BASE}/j1-league/torverteilungminuten/wettbewerb/JAP1/plus/1?saison_id={year}"
    soup = get_soup(url)
    if soup is None:
        return pd.DataFrame()

    table = soup.find("table", {"class": "items"})
    if not table:
        print(f"No goal-minute table found for year {year}")
        return pd.DataFrame()

    rows = table.find_all("tr")[2:]
    data = []
    for row in rows:
        columns = row.find_all("td")
        if len(columns) < 10:
            continue
        try:
            team_text = columns[1].text.strip()
            team = team_text.split(" (")[0]
            rank = team_text.split(" (")[1].replace(".)", "").strip()
            data.append([
                int(year) + 1, team, rank,
                columns[2].text.strip(), columns[3].text.strip(), columns[4].text.strip(),
                columns[5].text.strip(), columns[6].text.strip(), columns[7].text.strip(),
                columns[8].text.strip(), columns[9].text.strip()
            ])
        except IndexError:
            print(f"Skipping a goal-minute row due to missing data for year {year}")

    return pd.DataFrame(data, columns=[
        "Year", "Team", "Rank", "1-15", "16-30", "31-45", "45+", "46-60", "61-75", "76-90", "90+"
    ])

@task(log_prints=True)
def create_card_table(year: str):
    url = f"{BASE}/j1-league/fairnesstabelle/wettbewerb/JAP1/plus/1?saison_id={year}"
    soup = get_soup(url)
    if soup is None:
        return pd.DataFrame()

    table = soup.find("table", {"class": "items"})
    if not table:
        print(f"No card table found for year {year}")
        return pd.DataFrame()

    rows = table.find_all("tr")[2:]
    data = []
    for row in rows:
        columns = row.find_all("td")
        if len(columns) < 8:
            continue
        try:
            data.append([
                int(year) + 1, columns[2].text.strip(), columns[0].text.strip(),
                columns[4].text.strip(), columns[5].text.strip(),
                columns[6].text.strip(), columns[7].text.strip()
            ])
        except IndexError:
            print(f"Skipping a card row due to missing data for year {year}")

    return pd.DataFrame(data, columns=[
        "Year", "Team", "Rank", "Yellow", "Second Yellow", "Red", "Second Yellow And Red"
    ])

@task(log_prints=True)
def create_fixtures_table(year: str):
    url = f"{BASE}/j1-league/gesamtspielplan/wettbewerb/JAP1/saison_id/{year}"
    soup = get_soup(url)
    if soup is None:
        return pd.DataFrame()

    headlines = soup.find_all("div", {"class": "content-box-headline"})
    if not headlines:
        print(f"No matchday headers found for year {year}")
        return pd.DataFrame()

SCORE_PATTERN = re.compile(r"^\d+\s*:\s*\d+$")
DATE_PATTERN = re.compile(r"(\d{2})/(\d{2})/(\d{2})")


@task(log_prints=True)
def create_fixtures_table(year: str):
    url = f"{BASE}/j1-league/gesamtspielplan/wettbewerb/JAP1/saison_id/{year}"
    soup = get_soup(url)
    if soup is None:
        return pd.DataFrame()

    headlines = soup.find_all("div", {"class": "content-box-headline"})
    if not headlines:
        print(f"No matchday headers found for year {year}")
        return pd.DataFrame()

    data = []
    for headline in headlines:
        match = re.search(r"(\d+)", headline.text.strip())
        matchday = match.group(1) if match else None
        table = headline.find_next("table")
        if table is None:
            continue

        current_date = None
        for row in table.find_all("tr"):
            columns = row.find_all("td")
            if len(columns) < 5:
                continue

            if "Home team" in row.text and "Away team" in row.text:
                continue 

            try:
                date_text = columns[0].text.strip()
                if date_text:
                    date_match = DATE_PATTERN.search(date_text)
                    if date_match:
                        dd, mm, yy = date_match.groups()
                        current_date = f"20{yy}-{mm}-{dd}"
                time_text = columns[1].text.strip()
                team_links = [a for a in row.find_all("a") if a.get("href") and "/verein/" in a["href"]]
                teams = []
                for a in team_links:
                    href = a["href"]
                    text = a.text.strip()
                    if teams and teams[-1][2] == href:
                        if text and not teams[-1][0]:
                            teams[-1] = (text, teams[-1][1], href)
                        continue
                    teams.append((text, extract_club_id(href), href))

                if len(teams) < 2:
                    continue
                home_team, home_id, _ = teams[0]
                away_team, away_id, _ = teams[-1]
                home_goals, away_goals = None, None
                for col in columns:
                    text = col.text.strip()
                    if SCORE_PATTERN.match(text):
                        home_goals, away_goals = [p.strip() for p in text.split(":")]
                        break

                if not home_team or not away_team:
                    continue

                data.append([
                    int(year) + 1, matchday, current_date, time_text,
                    home_team, home_id, home_goals, away_goals, away_team, away_id
                ])
            except (IndexError, AttributeError):
                print(f"Skipping a fixture row due to missing data for year {year}")

    return pd.DataFrame(data, columns=[
        "Year", "Matchday", "Date", "Time", "HomeTeam", "HomeClubID",
        "HomeGoals", "AwayGoals", "AwayTeam", "AwayClubID"
    ])

@task(log_prints=True)
def create_squad_summary_table(year: str, club_id: str, team_name: str):
    if not club_id:
        return pd.DataFrame()

    url = f"{BASE}/verein/kader/verein/{club_id}/saison_id/{year}/plus/1"
    soup = get_soup(url)
    if soup is None:
        return pd.DataFrame()

    table = soup.find("table", {"class": "items"})
    if not table:
        print(f"No squad table found for {team_name} ({club_id}), year {year}")
        return pd.DataFrame()

    rows = table.find_all("tr", {"class": ["odd", "even"]})
    ages, values, nationalities = [], [], []
    for row in rows:
        columns = row.find_all("td")
        if len(columns) < 6:
            continue
        try:
            age_cell = next((c.text for c in columns if re.search(r"\(\d{2}\)", c.text)), None)
            if age_cell:
                age_match = re.search(r"\((\d{2})\)", age_cell)
                if age_match:
                    ages.append(int(age_match.group(1)))

            value_cell = columns[-1].text.strip()
            value = parse_market_value(value_cell)
            if value is not None:
                values.append(value)

            flags = row.find_all("img", {"class": "flaggenrahmen"})
            if flags:
                nationalities.append(flags[0].get("title", "").strip())
        except (IndexError, AttributeError):
            continue

    if not values and not ages:
        return pd.DataFrame()

    total_value = round(sum(values), 2) if values else None
    avg_age = round(sum(ages) / len(ages), 1) if ages else None
    foreign_count = sum(1 for n in nationalities if n and n != "Japan")

    return pd.DataFrame([[
        int(year) + 1, team_name, club_id, total_value, avg_age,
        len(nationalities), foreign_count
    ]], columns=[
        "Year", "Team", "ClubID", "SquadValueEUR_M", "AvgAge", "SquadSize", "ForeignPlayers"
    ])

@flow(name="J1 League ETL", log_prints=True)
def web_scrape():
    os.makedirs("./CSV", exist_ok=True)
    all_data_table, all_data_goal, all_data_card = [], [], []
    all_data_fixtures, all_data_squad = [], []

    for year in YEARS:
        df_table = create_table(year)
        df_goal = create_goal_table(year)
        df_card = create_card_table(year)
        df_fixtures = create_fixtures_table(year)

        if not df_table.empty:
            all_data_table.append(df_table)
        if not df_goal.empty:
            all_data_goal.append(df_goal)
        if not df_card.empty:
            all_data_card.append(df_card)
        if not df_fixtures.empty:
            all_data_fixtures.append(df_fixtures)

        if not df_table.empty:
            for _, row in df_table.iterrows():
                df_squad = create_squad_summary_table(year, row["ClubID"], row["Team"])
                if not df_squad.empty:
                    all_data_squad.append(df_squad)
                time.sleep(1)

    if all_data_table:
        pd.concat(all_data_table, ignore_index=True).to_csv("./CSV/j1_league_table.csv", index=False)
    if all_data_goal:
        pd.concat(all_data_goal, ignore_index=True).to_csv("./CSV/j1_league_goal_minutes.csv", index=False)
    if all_data_card:
        pd.concat(all_data_card, ignore_index=True).to_csv("./CSV/j1_league_card_stats.csv", index=False)
    if all_data_fixtures:
        pd.concat(all_data_fixtures, ignore_index=True).to_csv("./CSV/j1_league_fixtures.csv", index=False)
    if all_data_squad:
        pd.concat(all_data_squad, ignore_index=True).to_csv("./CSV/j1_league_squad_summary.csv", index=False)


if __name__ == "__main__":
    web_scrape()