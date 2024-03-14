from sqlalchemy import create_engine
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


##################### Get assets symbols #####################

user = 'root'
password = '123'
host = 'localhost'
port = 3306
database = 'sp_500'
table_assets = 'assets'
table_info = 'info'

# connection
engine = create_engine(
    url=f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
)

# writing a query to select all assets symbols
query_get_symbols_and_ids  = 'SELECT asset_symbol, asset_id FROM assets;'

with engine.connect() as connection:
    df_asset_symbols_and_ids = pd.read_sql(query_get_symbols_and_ids, engine)
    connection.close()

df_asset_symbols_and_ids.set_index('asset_symbol', inplace=True)

##################### Check last update #####################

# this is for cases that i forgot to update database

query_last_date = "select max(date) as last_date from info"

with engine.connect() as connection:
    df_last_update = pd.read_sql(query_last_date, engine)
    connection.close()

last_date = df_last_update.iloc[0]['last_date']
today = datetime.now().date()

def get_missed_dates(last_date, today):
    missed_days = today - last_date
    
    dates = []
    for i in range(missed_days.days): 
        day = last_date + timedelta(days=i)
        dates.append(day.strftime('%Y-%m-%d'))

    return dates

missed_dates = get_missed_dates(last_date, today)


##################### Get data #####################

def get_daily_data(asset_symbol,  df_asset_symbols_and_ids, today_date):

    # getting data
    asset = yf.Ticker(asset_symbol)
    df = asset.history(start=today_date)

    # organizing dataframe
    df.reset_index(inplace=True) # index to date clomun
    df.drop(columns=['Open', 'High', 'Low'], inplace=True)

    # setting the asset id
    asset_id = df_asset_symbols_and_ids.loc[asset_symbol]['asset_id']
    df['asset_id'] = asset_id

    return df

df_list = []
error_list = []

for day in missed_dates: # this is also to execute all days that i missed
    today = day
    for asset_symbol in df_asset_symbols_and_ids.index:
        
        try:
            df = get_daily_data(asset_symbol, df_asset_symbols_and_ids, today)
            df_list.append(df)
        except:
            error_list.append(asset_symbol)

    filtered_df_list = [df for df in df_list if not df.empty and not df.isnull().values.all()]

main_df = pd.concat(filtered_df_list) # join all data frames
main_df.index = range(len(main_df)) # fixing index

new_columns_dict = {'Date' : 'date', 'Close' : 'price', 'Volume' : 'volume', 'Dividends' : 'dividends', 'Stock Splits' : 'slpit'}
main_df.rename(columns=new_columns_dict, inplace=True)

# defining info_id column
main_df['info_id'] = main_df.index

# rearranging columns order
main_df = main_df.reindex(columns=['info_id', 'asset_id', 'date', 'price', 'volume', 'dividends', 'split'])

# nan to 0.0
main_df['split'] = main_df['split'].apply(lambda x: 0 if np.isnan(x) else x)


##################### Fixing info_id #####################

# writing a query to select the last info id
query_get_last_info_id  = "SELECT MAX(info_id) as 'last_info_id' FROM info;"

with engine.connect() as connection:
    df_last_info_id = pd.read_sql(query_get_last_info_id, engine)
    connection.close()

# making a new index to main_df
last_info_id = df_last_info_id['last_info_id'][0]
len_main_df = len(main_df)
new_info_range = last_info_id + len_main_df

new_info = list(range(last_info_id + 1, new_info_range + 1))
main_df['info_id'] = new_info


##################### Storage data #####################

with engine.connect() as connection:
    main_df.to_sql(table_info,  con=connection, if_exists='append', index=False)
    connection.commit()
    connection.close()