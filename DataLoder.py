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

    def stock_data_generator(self, itv = INTERVAL, st_date = START_DATA , end_date = END_DATA, error_list = False):
        total_df_list = []
        error_stock = []
        for stock in tqdm(STOCK_LIST['Symbol']):
            try:
                _ = yf.download(tickers = stock, start = st_date, end = end_date, interval = itv, progress=False, show_errors=False)

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
    
    def read_from_db(self):
        engine = sqlalchemy.create_engine(DB_INFO)
        conn = engine.connect()
        df = pd.read_sql(SQL, conn)
        conn.close()
        return df

    def concat(self):
        new_data = self.stock_data_generator()
        new_data.reset_index(inplace = True)
        new_data.index.name = 'Index'
        
        try:
            if self.data_type == 'csv':
                old_data = pd.read_csv(f'{os.path.join(ADDR, FILE_NAME)}.csv', index_col='index')
            elif self.data_type == 'db':
                self.old_data = self.read_from_db()   
            print('SUCCESS!')

        except:
            if DATA_TYPE == 'csv':
                error_data = os.path.join(ADDR, FILE_NAME)+'.csv'
            elif DATA_TYPE == 'db':
                error_data = TABLE_NAME
            print(f'NO SUCH DIRECTORY : {error_data}')
            old_data = pd.DataFrame(columns=new_data.columns)
            pass
        df = pd.concat([old_data, new_data], axis=0)
        df.drop_duplicates(keep = 'first', inplace=True)
        return df

    def upload(self) :
        df = self.concat()
        if DATA_TYPE == 'csv':
            df.to_csv(f'./Dataset/{FILE_NAME}.csv',index=False)
            pass
        elif DATA_TYPE == 'db':
            self.creat_table(TABLE_NAME)
        else:
            print('데이터 타입을 확인해주세요.')
        # df['Datetime'] = df['Datetime'].apply(lambda x : dt.datetime.strptime(x[:-6], '%Y-%m-%d %H:%M:%S'))
        return df

def to_datetime(datetime):
    datetime = datetime.apply(lambda x : dt.datetime.strptime(x[:-6], '%Y-%m-%d %H:%M:%S'))
    return datetime
