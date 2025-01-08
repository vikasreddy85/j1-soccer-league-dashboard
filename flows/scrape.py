import requests
import pandas as pd
from bs4 import BeautifulSoup
from prefect import flow, task

@task(log_prints=True)
def create_table(year: str):
    url = f"https://www.transfermarkt.com/j1-league/tabelle/wettbewerb/JAP1/saison_id/{year}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0 Safari/537.36",}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"class": "items"})
        if not table:
            print(f"No table found for year {year}")
            return pd.DataFrame()

        rows = table.find_all("tr")[1:]
        data = []
        for row in rows:
            columns = row.find_all("td")
            if len(columns) < 10:
                continue
            try:
                rank = columns[0].text.strip() 
                team = columns[2].text.strip()
                matches = columns[3].text.strip()
                wins = columns[4].text.strip()
                draws = columns[5].text.strip()
                losses = columns[6].text.strip()
                points = columns[9].text.strip()
                data.append([int(year) + 1, rank, team, matches, wins, draws, losses, points])
            except IndexError:
                print(f"Skipping a row due to missing data for year {year}")

        df = pd.DataFrame(data, columns=["Year", "Rank", "Team", "Matches", "Wins", "Draws", "Losses", "Points"])
        return df
    else:
        print(f"Failed to retrieve data for year {year}. Status code: {response.status_code}")
        return pd.DataFrame()

@task(log_prints=True)
def create_goal_table(year: str):
    url = f"https://www.transfermarkt.com/j1-league/torverteilungminuten/wettbewerb/JAP1/plus/1?saison_id={year}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0 Safari/537.36",}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"class": "items"})
        if not table:
            print(f"No table found for year {year}")
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
                one_to_fifteen = columns[2].text.strip()
                sixteen_to_thirty = columns[3].text.strip()
                thirty1_to_fourty5 = columns[4].text.strip()
                fourty5_plus = columns[5].text.strip()
                fourty6_to_sixty = columns[6].text.strip()
                sixty1_to_seventy5 = columns[7].text.strip()
                seventy6_to_ninety = columns[8].text.strip()
                ninety_plus = columns[9].text.strip()
                data.append([int(year) + 1, team, rank, one_to_fifteen, sixteen_to_thirty, thirty1_to_fourty5, 
                fourty5_plus, fourty6_to_sixty, sixty1_to_seventy5, seventy6_to_ninety, ninety_plus])
            except IndexError:
                print(f"Skipping a row due to missing data for year {year}")

        df = pd.DataFrame(data, columns=["Year", "Team", "Rank", "1-15", "16-30", "31-45", "45+", "46-60", "61-75", "76-90", "90+"])
        return df
    else:
        print(f"Failed to retrieve data for year {year}. Status code: {response.status_code}")
        return pd.DataFrame() 

@task(log_prints=True)
def create_card_table(year: str):
    url = f"https://www.transfermarkt.com/j1-league/fairnesstabelle/wettbewerb/JAP1/plus/1?saison_id={year}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0 Safari/537.36",}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"class": "items"})
        if not table:
            print(f"No table found for year {year}")
            return pd.DataFrame()

        rows = table.find_all("tr")[2:]
        data = []
        for row in rows:
            columns = row.find_all("td")
            if len(columns) < 8:
                continue
            try:
                rank = columns[0].text.strip()
                team = columns[2].text.strip()
                yellow = columns[4].text.strip()
                second_yellow = columns[5].text.strip()
                red = columns[6].text.strip()
                second_yellow_and_red = columns[7].text.strip()
                data.append([int(year) + 1, team, rank, yellow, second_yellow, red, second_yellow_and_red])
            except IndexError:
                print(f"Skipping a row due to missing data for year {year}")

        df = pd.DataFrame(data, columns=["Year", "Team", "Rank", "Yellow", "Second Yellow", "Red", "Second Yellow And Red"])
        return df
    else:
        print(f"Failed to retrieve data for year {year}. Status code: {response.status_code}")
        return pd.DataFrame() 

@flow(name="J1 League ETL", log_prints=True)
def web_scrape():
    years = [
        "2004", "2005", "2006", "2007", "2008", "2009", "2010", 
        "2011", "2012", "2013", "2014", "2015", "2016", "2017", 
        "2018", "2019", "2020", "2021", "2022", "2023"
    ]
    all_data_table, all_data_goal, all_data_card = [], [], []
    for year in years:
        df_table = create_table(year)
        df_goal = create_goal_table(year)
        df_card = create_card_table(year)
        if not df_table.empty:
            all_data_table.append(df_table)
        if not df_goal.empty:
            all_data_goal.append(df_goal)
        if not df_card.empty:
            all_data_card.append(df_card)  
    if all_data_table:
        combined_df_table = pd.concat(all_data_table, ignore_index=True)
        combined_df_table.to_csv("./CSV/j1_league_table.csv", index=False)

    if all_data_goal:
        combined_df_goal = pd.concat(all_data_goal, ignore_index=True)
        combined_df_goal.to_csv("./CSV/j1_league_goal_minutes.csv", index=False)

    if all_data_card:
        combined_df_card = pd.concat(all_data_card, ignore_index=True)
        combined_df_card.to_csv("./CSV/j1_league_card_stats.csv", index=False)

if __name__ == "__main__":
    web_scrape()
