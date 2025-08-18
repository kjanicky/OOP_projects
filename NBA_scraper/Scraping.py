import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv

def Scraper(seasons,url_template):

    all_dfs = []

    for season in seasons:
        final_url = url_template.format(season)
        print(f"Downloading {season} from {final_url}")
        page = requests.get(final_url)
        soup = BeautifulSoup(page.text, 'html.parser')
        table = soup.find('table', {'id': 'per_game_stats'})

        header_row = table.find('thead').find_all('tr')[-1]
        column_names = [th.text.strip() for th in header_row.find_all('th')]

        table_content = table.find('tbody').find_all('tr')
        rows = []

        for row in table_content:
            if row.find('th', {"scope": "row"}) is None:
                continue

            player_name = row.find('th').text.strip()
            cells = row.find_all('td')
            row_data = [player_name] + [cell.text.strip() for cell in cells]

            row_data = row_data[:len(column_names)]
            while len(row_data) < len(column_names):
                row_data.append("") # making sure the data have the same lenght

            rows.append(row_data)

        df = pd.DataFrame(rows, columns=column_names)
        if "Rk" in df.columns:  # making sure df is not empty
            df = df[df["Rk"] != ""]
        df = df[:-1] # deleting season averages will calculate it eventually no need for that now
        df = df[df['Team'] != '2TM']
        df["Season"] = season
        df = df.drop_duplicates()
        all_dfs.append(df)

    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df = final_df.replace("", None)
    return final_df


def save_to_master(df, filename='master_data_NBA_stats.csv'):
    if os.path.exists(filename):
        df.to_csv(filename, mode="a", header=False, index=False, encoding="utf-8-sig")
    else:
        df.to_csv(filename, index=False, encoding="utf-8-sig")

load_dotenv()
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
}

def postgres_Loader(db_params,df,table_name='br_nba_stats'):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    sql = """
    INSERT INTO br_nba_stats (
        Rk, Player, Age, Team, Pos, G, GS, MP, FG, FGA, FG_percent,
        "3P", "3PA", "3P_percent", "2P", "2PA", "2P_percent", eFG_percent,
        FT, FTA, FT_percent, ORB, DRB, TRB, AST, STL, BLK, TOV, PF, PTS,
        Awards, Season
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for i, row in final_df.iterrows():
        cur.execute(sql, tuple(row))  # lub tuple(row.values)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Dodano {len(df)} rekordów do {table_name}")

final_df = Scraper(
    [2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024],
    'https://www.basketball-reference.com/leagues/NBA_{}_per_game.html'
)
save_to_master(final_df)
postgres_Loader(db_params,final_df)