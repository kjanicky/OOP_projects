import os
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

MASTER_CSV = "eurobasket_stats.csv"
HEADERS = [
    "player_number", "photo", "player_name", "position", "minutes_played",
    "field_goals", "2PT", "3PT", "FT", "OREB", "DREB", "REB", "AST",
    "STL", "BLK", "TO", "PF", "FR", "plus_minus", "EF", "PTS"
]


def parse_match_metadata(url: str):
    path_parts = urlparse(url).path.strip("/").split("/")
    game_date = path_parts[2]
    team_home_slug, team_away_slug = path_parts[3].split("-vs-")
    return game_date, team_home_slug.title(), team_away_slug.title()


def fetch_match_stats(url: str, stage: str = "group") -> pd.DataFrame:
    game_date, team_home, team_away = parse_match_metadata(url)

    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")


    table1 = soup.find("table", {"id": "table1", "class": "boxscore_table_header"})
    table2 = soup.find("table", {"class": "boxscore_table_header", "id": False})

    if not table1 or not table2:
        raise RuntimeError("Nie udało się znaleźć dwóch tabel boxscore")

    all_dfs = []
    for table, team_name in zip([table1, table2], [team_home, team_away]):
        rows = []
        tbody = table.find("tbody")
        for tr in tbody.find_all("tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all("td")]
            if cols:
                rows.append(cols)

        if rows:
            rows = rows[:-1] # delete summary of a game for a team

        if rows:
            df = pd.DataFrame(rows, columns=HEADERS)

            df["plays_on"] = team_name
            df["game_date"] = game_date
            df["match"] = f"{team_home}-vs-{team_away}"
            df["stage"] = stage

            all_dfs.append(df)


    result_df = pd.concat(all_dfs, ignore_index=True)
    if "photo" in result_df.columns:
        result_df = result_df.drop(columns=["photo"])
    return result_df


def save_to_master(df, filename=MASTER_CSV):
    if os.path.exists(filename):
        df.to_csv(filename, mode="a", header=False, index=False, encoding="utf-8-sig")
    else:
        df.to_csv(filename, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    url = "https://globalsportsarchive.com/match/basketball/2017-09-09/slovenia-vs-ukraine/1162785"
    df = fetch_match_stats(url, stage="group")
    save_to_master(df)
    print("Added data:", df.shape)
