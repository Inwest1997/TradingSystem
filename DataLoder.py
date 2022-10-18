import os
import pandas as pd
import datetime as dt
import yfinance as yf
import sqlalchemy
from tqdm import tqdm
from sqlalchemy.types import Integer, String, BIGINT

from config import *

'''
NAME = 'testdb'
PW = '0000'
USER_NAME = 'localhost'
PORT = '5432'
DB = 'testdb'
db_info = f'postgresql://{NAME}:{PW}@{USER_NAME}:{PORT}/{DB}'
'''
class DataGenerator:
    def __init__(self, addr = ADDR ,file_name = FILE_NAME, sql = SQL, data_type = DATA_TYPE):
        self.addr = addr
        self.file_name = file_name
        self.data_type = data_type

    def stock_data_generator(self, itv = INTERVAL, st_date = START_DATA , end_date = END_DATA, error_list = False):
        total_df_list = []
        error_stock = []
        for stock in tqdm(STOCK_LIST['Symbol'][:10]):
            try:
                _ = yf.download(tickers = stock, start = st_date, end = end_date, interval = itv, progress=False, show_errors=False)
                _.reset_index(inplace=True)
                if len(_) == 0:
                    error_stock.append(stock)
                else :
                    _['Ticker'] = stock
                    _['TickerName'] = STOCK_LIST[STOCK_LIST['Symbol']==stock]['Name'].item()
                    total_df_list.append(_)
            except:
                print('ERROR!')
                continue
        if error_list == True:
            return pd.concat(total_df_list), error_stock
        else :
            return pd.concat(total_df_list)

    def creat_table(self, tb_name = TABLE_NAME, exist = 'replace'):
        try:
            engine = sqlalchemy.create_engine(DB_INFO)
            conn = engine.connect()
            self.concat.to_sql(tb_name, conn
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
            print('DB 저장 완료')
            conn.close()
        except:
            print('DB 저장 실패')
    
    def read_from_db():
        engine = sqlalchemy.create_engine(DB_INFO)
        conn = engine.connect()
        df = pd.read_sql(SQL, conn)
        # df = df.set_index('Date')
        conn.close()
        return df

    def concat(self):
        new_data = self.stock_data_generator()
        # new_data.reset_index(inplace=True)
        try:
            if self.data_type == 'csv':
                old_data = pd.read_csv(f'{os.path.join(self.addr, self.file_name)}.csv')
            elif self.data_type == 'db':
                self.old_data = self.read_from_db()   
            print('SUCCESS!')
            old_data.drop(['Unnamed: 0'], axis = 1, inplace = True)

        except:
            if self.data_type == 'csv':
                error_data = os.path.join(ADDR, FILE_NAME)+'.csv'
            elif self.data_type == 'db':
                error_data = TABLE_NAME
            print(f'NO SUCH DIRECTORY : {error_data}')
            old_data = pd.DataFrame(columns=new_data.columns)
            pass
        # old_data.reset_index(inplace=True)
        df = pd.concat([old_data, new_data], axis=0, ignore_index=True)
        df.drop_duplicates(keep = 'first', inplace=True)
        df = df[new_data.columns]
        return df

    def upload(self) :
        df = self.concat()
        if self.data_type == 'csv':
            df.to_csv(f'./Dataset/{self.file_name}.csv')
            pass
        elif self.data_type == 'db':
            self.creat_table(TABLE_NAME)
        else:
            print('데이터 타입을 확인해주세요.')

