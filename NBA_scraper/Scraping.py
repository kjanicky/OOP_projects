import requests
from bs4 import BeautifulSoup
import pandas as pd
url = 'https://www.basketball-reference.com/leagues/NBA_2024_per_game.html'
page = requests.get(url)
soup = BeautifulSoup(page.text, 'html.parser')
table = soup.find('table', {'id': 'per_game_stats'})

header_row = table.find('thead').find_all('tr')[-1]
column_names = [th.text.strip() for th in header_row.find_all('th')]

table_content = table.find('tbody').find_all('tr')

df = pd.DataFrame( columns = column_names[:-1])
lenght = len(df)

df = pd.DataFrame(columns=column_names) # setting up data frame

for row in table_content:
    if row.find('th', {"scope": "row"}) is None:
        continue

    player_name = row.find('th').text.strip()
    cells = row.find_all('td')
    row_data = [player_name] + [cell.text.strip() for cell in cells]

    row_data = row_data[:len(column_names)]
    row_data += [''] * (len(column_names) - len(row_data)) # making sure the data have the same lenght

    df.loc[len(df)] = row_data
df = df[:-1] # deleting season averages will calculate it eventually no need for that now

