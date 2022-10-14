from matplotlib.dates import DAILY
import pandas as pd
import yfinance as yf
import sqlalchemy
from tqdm import tqdm
from sqlalchemy.types import Integer, String, BIGINT
'''
NAME = 'testdb'
PW = '0000'
USER_NAME = 'localhost'
PORT = '5432'
DB = 'testdb'
db_info = f'postgresql://{NAME}:{PW}@{USER_NAME}:{PORT}/{DB}'
'''

def creat_table(df, db_info, tb_name, exist = 'append'):
    engine = sqlalchemy.create_engine(db_info)
    conn = engine.connect()
    df.to_sql(tb_name, conn
            , if_exists=  exist# ---append, replace
            , index=False
            , dtype={
                    # "Datetime": sqlalchemy.DateTime,
                    "TickerName": String(),
                     "Ticker": String(10)
                    ,"Open": BIGINT()
                    , "High": BIGINT()
                    , "Low": BIGINT()
                    , "Close": BIGINT()
                    , "Volume": BIGINT()
                    , 'Adj Close': BIGINT()
                    }
            )
    conn.close()

def stock_data_generator(stock_list, st_date, end_date, itv):
    '''
    stock_list : 다운로드드 받을 주식종목목 리스트
    st_date : start date, YYYY-MM-DD 형식
    end_date : end date, YYYY-MM-DD 형식
    itv : interval, 수집할 데이터 단위
    '''
    total_df_list = []
    error_stock = []
    for stock in tqdm(stock_list['Symbol']):
        try:
            _ = yf.download(tickers = stock, start = st_date, end=end_date, interval = itv, progress=False, show_errors=False)
            if len(_) == 0:
                error_stock.append(stock)
            else :
                _['Ticker'] = stock
                _['TickerName'] = stock_list[stock_list['Symbol']==stock]['Name'].item()
                total_df_list.append(_)
        except:
            print('ERROR!')
            continue
    return pd.concat(total_df_list), error_stock

def read_from_db(sql, db_info):
    engine = sqlalchemy.create_engine(db_info)
    conn = engine.connect()
    df = pd.read_sql(sql, conn)
    # df = df.set_index('Date')
    conn.close()
    return df

