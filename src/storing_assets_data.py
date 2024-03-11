from sqlalchemy import create_engine
from bs4 import BeautifulSoup as bs
import requests
import yfinance as yf
import pandas as pd


##################### Getting all stock symbols of S&P500 from wikipedia ##################### 

html = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies').content
soup = bs(html, 'html.parser')

# Here is all the <a>SYMBOL</a>
sp500_symbols_html = soup.findAll("a", class_='external text')

# Eventually some wrong strings came with the html, so i put this filter of 4 str only for stocks symbols
sp500_symbols_list = []

for html in sp500_symbols_html:

    symbol = html.text
    
    if len(symbol) <= 4: sp500_symbols_list.append(html.text)


#####################  Getting all the assets information ##################### 

# we need: name | symbol | website | sector

def get_asset_info(asset):

    stock = yf.Ticker(asset)

    name = stock.info['shortName']
    symbol = stock.info['symbol']
    website = stock.info['website']
    sector = stock.info['sector']

    return [name, symbol, website, sector]

info_list = []

for asset in sp500_symbols_list:
    try:
        info = get_asset_info(asset)
        info_list.append(info)
    except: 
        # this is because bf.b stock is giving an error
        bf_stock = ['Brown-Forman Corporation', 'BF.B', 'https://www.brown-forman.com/', 'Consumer Defensive']
        info_list.append(bf_stock)

columns = ['asset_name', 'asset_symbol', 'website', 'sector']
df = pd.DataFrame(info_list, columns=columns)

# setting the id column
df["asset_id"] = df.index

# rearranging columns order
df = df.reindex(columns=['asset_id', 'asset_name', 'asset_symbol', 'website', 'sector'])


##################### Saving info into database #####################

# parameters
user = 'root'
password = '123'
host = 'localhost'
port = 3306
database = 'sp_500'
table = 'assets'

# connection
engine = create_engine(
    url=f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
)

with engine.connect() as connection:
    df.to_sql(table, con=connection, if_exists='append', index=False)
    connection.commit()
    connection.close()