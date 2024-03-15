# Steps and scripts

### Step one: [creating_tables.py](https://github.com/Iveteras/sp_500_etl/blob/main/src/creating_tables.py)

```python
import mysql.connector
```

- Basically it makes a connection with mysql and creates the tables that will be used in the project.

### Step two: [storing_assets_data.py](https://github.com/Iveteras/sp_500_etl/blob/main/src/storing_assets_data.py)

```python
from sqlalchemy import create_engine
from bs4 import BeautifulSoup as bs
import requests
import yfinance as yf
import pandas as pd
```
- Fill in the assets sql table;
- Get all stock symbols of S&P500 from wikipedia with web scraping;
- Get assets information with yfinance: name, symbol, website, sector;
- Store everything in mysql;

### Step three: [get_historical_assets_data.py](https://github.com/Iveteras/sp_500_etl/blob/main/src/get_historical_assets_data.py)

```python
from sqlalchemy import create_engine
import yfinance as yf
import pandas as pd
import numpy as np
import time
```
- Get the historical data from all assets and store in the info sql table;
- First connect with assets sql table to get all symbols;
- Then get all data with yfinance and store it all into info sql table;

### Step four: [get_daily_assets.py](https://github.com/Iveteras/sp_500_etl/blob/main/src/get_daily_assets.py)

```python
from sqlalchemy import create_engine
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
```
- Main script of the application;
- Responsable to get data every day;
- First connect with assets sql table to get all symbols;
- Connect with assets sql table to check all remaining dates;
- Then get all data with yfinance and store it all into info sql table;
  
