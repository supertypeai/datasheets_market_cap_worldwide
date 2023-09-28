import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime
import calendar
from dotenv import load_dotenv
load_dotenv()
import os
from supabase import create_client

month = datetime.now().month
year = datetime.now().year

if month == 12:
    month_name = calendar.month_name[1].lower()
    year += 1
else:
    month_name = calendar.month_name[month + 1].lower()

market_cap_column = month - 1

url = f'https://focus.world-exchanges.org/issue/{month_name}-{year}/market-statistics'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

table = soup.find('table')

data = []

for row in table.find_all('tr'):
    columns = row.find_all('td')
    if len(columns) >= 7:
        stock_exchange = columns[0].text.strip()
        market_cap = columns[market_cap_column].text.strip()
        if market_cap != '' and ('Total' not in stock_exchange):
          data.append({
              'stock_exchange': stock_exchange,
              'market_cap': market_cap
          })

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

response = supabase.table("idx_company_report").select("market_cap").execute()
idn_market_cap_idr = sum(item['market_cap'] for item in response.data if item['market_cap'] is not None)
response = requests.get("https://api.exchangerate-api.com/v4/latest/IDR")
rate_data = response.json()
usd_rate = rate_data["rates"]["USD"]
idn_market_cap_usd = (idn_market_cap_idr * usd_rate)/1000000

data.append({'stock_exchange': 'Indonesia Stock Exchange','market_cap': str(idn_market_cap_usd)})
df = pd.DataFrame(data)
df['market_cap'] = df['market_cap'].apply(lambda x: x.replace(',', '') if ',' in x else x).astype(float)
df = df.sort_values(by='market_cap', ascending=False)

df['rank'] = range(1, len(df) + 1)

market_cap_worldwide_json = df.to_dict(orient='records')
with open('stock_exchanges_by_market_cap.json', 'w') as json_file:
    json.dump(market_cap_worldwide_json, json_file, indent=4)