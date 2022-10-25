import os
import pandas as pd
import datetime as dt


import yfinance as yf
import sqlalchemy
from tqdm import tqdm
from sqlalchemy.types import Integer, String, BIGINT

stock_list = pd.read_csv('./Dataset/exist_kis_nasdaq_list.csv')
db_info = 'postgresql://junginseo:0000@localhost:5432/stock_db'

class DataGenerator:
    def __init__(self, data_type, interval = 'day') -> None:
        '''
        date_type : db or csv
        interval: day or min
        '''
        self.data_type = data_type
        self.interval = interval
        self.end_date = dt.datetime.today()

    def read_from_csv(self, dir = './Dataset', file = 'stock_d'):
        self.origin = pd.read_csv(f'{os.path.join(dir, file)}.csv')
        self.dir = dir
        self.file = file
        print('파일에서 CSV 데이터 불러오기 성공')


    def read_from_db(self, db_info = db_info, table_name='tick_stock', sql = None):
        self.db_info = db_info
        self.table_name = table_name
        engine = sqlalchemy.create_engine(self.db_info)
        conn = engine.connect()
        if sql == None:
            sql = f'''SELECT * FROM {self.table_name}'''
        self.origin = pd.read_sql(sql, conn)
        print('DB에서 데이터 불러오기 성공')
        conn.close()


    def read_origin_data(self, **kwargs):
        if self.data_type == 'csv':
            self.read_from_csv(**kwargs)
        elif self.data_type == 'db':
            self.read_from_db(**kwargs)


    def date_gap(self):
        return (self.end_date - dt.datetime.strptime(self.origin.Datetime.max(), '%Y-%m-%d')).days

                

    def stock_data_generator(self,  error_list = False, stock_list = stock_list) :
        total_df_list = []
        error_stock = []
        itv = '1'+self.interval[0]
        if self.data_type == 'min' and self.date_gap() > 7:
            st_date = dt.datetime.strftime(self.end_date - dt.timedelta(weeks=1), '%Y-%m-%d')
        elif self.data_type == 'day':
            st_date = dt.datetime.strftime(self.origin.Datetime.max() + dt.timedelta(days=1), '%Y-%m-%d')
        else:
            st_date = self.origin.Datetime.max() 
        for stock in tqdm(stock_list['Symbol']):
            try:
                _ = yf.download(tickers = stock, start = st_date, end = self.end_date, interval = itv, progress=False, show_errors=False)
                _.reset_index(inplace=True)
                _.rename(columns={'Date':'Datetime'},inplace=True)
                if len(_) == 0:
                    error_stock.append(stock)
                else :
                    _['Ticker'] = stock
                    _['TickerName'] = stock_list[stock_list['Symbol']==stock]['Name'].item()
                    total_df_list.append(_)
            except:
                print('ERROR!')
                continue
        self.new = pd.concat(total_df_list)
        if error_list == True:
            return error_stock
        

    def data_concat(self) :

        try:
            self.read_origin_data()
            self.stock_data_generator()
            print('SUCCESS!')
            self.concat = pd.concat([self.origin, self.new], axis=0)
            self.concat.reset_index(drop = True)
            self.concat.drop_duplicates(keep = 'first', inplace=True)

        except Exception as e:
            # print(f'NO SUCH DIRECTORY OR TABLE')
            print('에러 발생 : ',e)
            pass



    def creat_table(self, exist = 'append'):
        try :
            engine = sqlalchemy.create_engine(self.db_info)
            conn = engine.connect()
            print('DB 연동 성공')
            self.new.to_sql(self.table_name, conn
                        , if_exists=  exist# ---append, replace
                        , index=False
                        , dtype={
                                # "Datetime": sqlalchemy.DateTime,
                                "TickerName": String(),
                                "Ticker": String(10)
                                # ,"Open": BIGINT()
                                # , "High": BIGINT()
                                # , "Low": BIGINT()
                                # , "Close": BIGINT()
                                # , "Volume": BIGINT()
                                # , 'Adj Close': BIGINT()
                                }
                        )
            print('DB 저장 완료')

            conn.close()
        except Exception as e:
            print(e, '로 인해 DB 저장 실패')

    def upload(self) -> None:
        if self.data_type == 'csv':
            self.data_concat()
            print('데이터 병합 성공')
            self.concat.to_csv(f'./Dataset/{self.file}.csv',index=False)
        elif self.data_type == 'db':
            self.stock_data_generator()
            self.creat_table()
        else:
            print('데이터 타입을 확인해주세요.')
        # df['Datetime'] = df['Datetime'].apply(lambda x : dt.datetime.strptime(x[:-6], '%Y-%m-%d %H:%M:%S'))


def to_datetime(datetime, type = 'm'):
    if type == 'm':
        datetime = datetime.apply(lambda x : dt.datetime.strptime(x[:-6], '%Y-%m-%d %H:%M:%S'))
    elif type == 'd':
        datetime = datetime.apply(lambda x : dt.datetime.strptime(x, '%Y-%m-%d'))
    return datetime

