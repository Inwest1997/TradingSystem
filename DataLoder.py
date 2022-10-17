import os
import pandas as pd
import datetime as dt
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
END_DATA = dt.datetime.strftime(dt.datetime.today(), '%Y-%m-%d')
START_DATA = dt.datetime.strftime(dt.datetime.today() - dt.timedelta(weeks=1), '%Y-%m-%d')
STOCK_LIST = pd.read_csv('./Dataset/exist_kis_nasdaq_list.csv')
INTERVAL = '1m'
SQL  = 'SELECT * FROM tick_stock'
ADDR = './Dataset'
FILE_NAME = 'stock_minute'
DB_INFO = 'postgresql://junginseo:0000@localhost:5432/stock_db'
TABLE_NAME = 'tick_stock'

class DataGenerator:
    def __init__(self, addr = ADDR ,file_name = FILE_NAME, sql = SQL, data_type='csv'):
        self.addr = addr
        self.file_name = file_name
        self.data_type = data_type

    def stock_data_generator(self, itv = INTERVAL, st_date = START_DATA , end_date = END_DATA, error_list = False):
        total_df_list = []
        error_stock = []
        for stock in tqdm(STOCK_LIST['Symbol'][:10]):
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
    
    def read_from_db():
        engine = sqlalchemy.create_engine(DB_INFO)
        conn = engine.connect()
        df = pd.read_sql(SQL, conn)
        # df = df.set_index('Date')
        conn.close()
        return df

    def concat(self):
        new_data = self.stock_data_generator()
        try:
            if self.data_type == 'csv':
                
                    old_data = pd.read_csv(f'{os.path.join(self.addr, self.file_name)}.csv')
            elif self.data_type == 'db':
                self.old_data = self.read_from_db()            
            df = pd.concat([old_data, new_data], axis=0)
            df.drop_duplicates(keep = 'first')
            return df
        except:
            print('NO SUCH DIRECTORY')
        

    def upload(self) :
        df = self.concat()
        if self.data_type == 'csv':
            df.to_csv(f'.Dataset/{self.file_name}.csv')
            pass
        elif self.data_type == 'db':
            self.creat_table(TABLE_NAME)
        else:
            print('데이터 타입을 확인해주세요.')

