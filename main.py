import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime
import calendar
import os
from supabase import create_client
import cloudscraper

exchanges = {

    'B3 - Brasil Bolsa Balcão': 'br',
    'Bermuda Stock Exchange':'bm',
    'Bolsa de Comercio de Santiago':'cl',
    'Bolsa de Valores de Colombia':'co',
    'Bolsa de Valores de Lima':'pe',
    'Bolsa Latinoamericana de Valores (Latinex)':'pa', 
    'Bolsa Mexicana de Valores': 'mx',
    'Bolsa Nacional de Valores':'cr',
    'Canadian Securities Exchange':'ca',
    'Jamaica Stock Exchange':'jm',
    'Nasdaq - US': 'us',
    'NYSE': 'us',
    'TMX Group': 'ca',

    'Armenia Securities Exchange':'am',
    'Astana International Exchange':'kz',
    'ASX Australian Securities Exchange': 'au',
    'Baku Stock Exchange':'az',
    'Bursa Malaysia': 'my',
    'Colombo Stock Exchange':'lk',
    'Hochiminh Stock Exchange':'vn',
    'Hong Kong Exchanges and Clearing': 'hk',
    'Indonesia Stock Exchange': 'id',
    'Japan Exchange Group': 'jp',
    'Kazakhstan Stock Exchange':'kz',
    'Korea Exchange': 'kr',
    'National Equities Exchange and Quotations':'cn',
    'National Stock Exchange of India': 'in',
    'NZX Limited':'nz',
    'Pakistan Stock Exchange':'pk',
    'Philippine Stock Exchange':'ph',
    'Shanghai Stock Exchange': 'cn',
    'Shenzhen Stock Exchange': 'cn',
    'Singapore Exchange': 'sg',
    'Taipei Exchange': 'tw',
    'Taiwan Stock Exchange': 'tw',
    'The Stock Exchange of Thailand': 'th',

    'Abu Dhabi Securities Exchange': 'ae',
    'Amman Stock Exchange':'jo',
    'Athens Stock Exchange':'gr',
    'Belarusian Currency and Stock Exchange':'by',
    'Bahrain Bourse':'bh',
    'BME Spanish Exchanges': 'es',
    'Borsa Istanbul':'tr',
    'Botswana Stock Exchange':'bw',
    'Boursa Kuwait':'kw',
    'Bourse de Casablanca':'ma',
    'BRVM':'ci',
    'Bucharest Stock Exchange':'ro',
    'Budapest Stock Exchange':'hu',
    'Bulgarian Stock Exchange':'bg',
    'Cyprus Stock Exchange':'cy',
    'Dar Es Salaam Stock Exchange':'tz',
    'Deutsche Boerse AG': 'de',
    'Dubai Financial Market':'ae',
    'Euronext': 'eu',
    'Ghana Stock Exchange':'gh',
    'Iran Fara Bourse Securities Exchange':'ir',
    'Johannesburg Stock Exchange': 'za',
    'Ljubljana Stock Exchange':'si',
    'Lusaka Securities Exchange':'zm',
    'Luxembourg Stock Exchange':'lu',
    'Malta Stock Exchange':'mt',
    'MERJ Exchange Limited':'sc',
    'Moscow Exchange':'ru',
    'Nairobi Securities Exchange':'ke',
    'Namibian Stock Exchange':'na',
    'Nasdaq Nordic and Baltics': 'nc',
    'Nigerian Exchange':'ng',
    'Palestine Exchange':'ps',
    'Prague Stock Exchange':'cz',
    'Rwanda Stock Exchange':'rw',
    'Saudi Exchange (Tadawul)': 'sa',
    'SIX Swiss Exchange': 'ch',
    'Stock Exchange of Mauritius':'mu',
    'Tehran Stock Exchange': 'ir',
    'Tel-Aviv Stock Exchange': 'il',
    'The Egyptian Exchange':'eg',
    'Tunis Stock Exchange':'tn',
    'Vienna Stock Exchange':'at',
    'Warsaw Stock Exchange':'pl',
    'Zagreb Stock Exchange':'hr',
    'LSE Group London Stock Exchange': 'gb',

}

def get_url(row):
    country = exchanges.get(row['stock_exchange'])
    if country:
        return f'https://flagcdn.com/w40/{country}.webp'
    else:
        return ''

month = datetime.now().month
year = datetime.now().year
month_name = calendar.month_name[month].lower()


market_cap_column = month - 2
if market_cap_column == 0:
  market_cap_column = 12


url = f'https://focus.world-exchanges.org/issue/{month_name}-{year}/market-statistics'

scraper = cloudscraper.create_scraper()
soup = BeautifulSoup(scraper.get(url).text, 'html.parser')
table = soup.find('table')


indo = False
data = []
for row in table.find_all('tr'):
    columns = row.find_all('td')
    if len(columns) >= 4:
        stock_exchange = columns[0].text.strip()
        if stock_exchange == 'Indonesia Stock Exchange':
            indo = True
        market_cap = columns[market_cap_column].text.strip()
        if market_cap != '' and ('Total' not in stock_exchange):
            data.append({
            'stock_exchange': stock_exchange,
            'market_cap': market_cap
            })


if indo is False:
    print('No information regarding Indonesian Stock Exchange in WFE')
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
df['country_img_url'] = df.apply(get_url, axis=1)
df['rank'] = range(1, len(df) + 1)

market_cap_worldwide_json = df.to_dict(orient='records')
with open('stock_exchanges_by_market_cap.json', 'w') as json_file:
    json.dump(market_cap_worldwide_json, json_file, indent=4)
