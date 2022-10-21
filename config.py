import datetime as dt
import pandas as pd


END_DATA = dt.datetime.strftime(dt.datetime.today(), '%Y-%m-%d')
START_DATA = dt.datetime.strftime(dt.datetime.today() - dt.timedelta(weeks=1), '%Y-%m-%d')
STOCK_LIST = pd.read_csv('./Dataset/exist_kis_nasdaq_list.csv')
INTERVAL = '1m'
ADDR = './Dataset'
FILE_NAME = 'stock_minute'
DB_INFO = 'postgresql://junginseo:0000@localhost:5432/stock_db'
TABLE_NAME = 'tick_stock'
SQL  = f'SELECT * FROM {TABLE_NAME}'
DATA_TYPE = 'db'