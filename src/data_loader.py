import os
import pandas as pd
import datetime as dt


import yfinance as yf
import sqlalchemy
from tqdm import tqdm
from sqlalchemy.types import Integer, String, BIGINT

stock_list = pd.read_csv('./Dataset/exist_kis_nasdaq_list.csv')
info = 'postgresql://junginseo:0000@localhost:5432/stock_db'

class DataGenerator:
    def __init__(self, data_type, interval = 'day', db_info = info) -> None:
        '''
        date_type : db or csv
        interval: day or min
        '''
        
        self.data_type = data_type
        self.interval = interval
        if self.interval == 'day':
            self.table_name = 'daily_stock'
        elif self.interval == 'min':
            self.table_name = 'tick_stock'
        self.end_date = dt.datetime.today()
        self.db_info = db_info

    def read_from_csv(self, dir = './Dataset',ticker = None):
        self.origin = pd.read_csv(f'{os.path.join(dir, self.table_name)}.csv', index_col=0)
        if ticker != None:
            self.origin = self.origin[self.origin['Ticker']==ticker]
        self.dir = dir
        print('파일에서 CSV 데이터 불러오기 성공')


    def read_from_db(self, sql = None, ticker = None):

        engine = sqlalchemy.create_engine(self.db_info)
        conn = engine.connect()
        if sql == None:
            sql = f'''SELECT * FROM {self.table_name}'''
        if ticker != None:
            sql = f'''SELECT * FROM {self.table_name} WHERE "{self.table_name}"."Ticker" = '{ticker}';'''

        self.origin = pd.read_sql(sql, conn)
        self.origin['Datetime'] = self.origin['Datetime'].apply(lambda x : x.strftime('%Y-%m-%d')[:10])
        print('DB에서 데이터 불러오기 성공')
        conn.close()


    def read_origin_data(self, **kwargs):
        if self.data_type == 'csv':
            self.read_from_csv(**kwargs)
        elif self.data_type == 'db':
            self.read_from_db(**kwargs)

    def date_gap(self):
        return (self.end_date - dt.datetime.strptime(self.origin.Datetime.max(), '%Y-%m-%d')).days

                

    def stock_data_generator(self,  error_list = False, stock_list = stock_list, all = False) :
        total_df_list = []
        error_stock = []
        itv = '1'+self.interval[0]

        if all == True:
            if self.interval == 'day':
                st_date = dt.datetime.strftime(self.end_date - dt.timedelta(weeks=52*30), '%Y-%m-%d')
            elif self.interval == 'min':
                st_date = dt.datetime.strftime(self.end_date - dt.timedelta(weeks=1), '%Y-%m-%d')
        else:
            if self.interval == 'min' and self.date_gap() > 7:
                st_date = dt.datetime.strftime(self.end_date - dt.timedelta(weeks=1), '%Y-%m-%d')
            elif self.interval == 'day':
                st_date = dt.datetime.strftime(dt.datetime.strptime(self.origin.Datetime.max(),'%Y-%m-%d') + dt.timedelta(days=1), '%Y-%m-%d')
            else:
                st_date = self.origin.Datetime.max() 
        for stock in tqdm(stock_list['Symbol']):
            try:
                _ = yf.download(tickers = stock, start = st_date, end = self.end_date, interval = itv, progress=False, show_errors=False)
                if len(_) == 0:
                    error_stock.append(stock)
                else :
                    _.reset_index(inplace=True)
                    _.rename(columns={'Date':'Datetime'},inplace=True)
                    _['Ticker'] = stock
                    total_df_list.append(_)
            except:
                print('ERROR!')
                continue
        self.new = pd.concat(total_df_list)
        if error_list == True:
            return error_stock
        

    def data_concat(self) :

        try:
            if self.origin==None:
                self.read_origin_data()
            if self.new == None:
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
                                # "TickerName": String(),
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
        try:
            if self.data_type == 'csv':
                self.data_concat()
                print('데이터 병합 성공')
                self.concat.to_csv(f'./Dataset/{self.file}.csv',index=False)
            elif self.data_type == 'db':
                self.stock_data_generator()
                self.creat_table()
            else:
                print('데이터 타입을 확인해주세요.')
        except:
            self.read_origin_data()
            if self.data_type == 'csv':
                self.data_concat()
                print('데이터 병합 성공')
                self.concat.to_csv(f'./Dataset/{self.file}.csv',index=False)
            elif self.data_type == 'db':
                self.stock_data_generator()
                self.creat_table()
            else:
                print('데이터 타입을 확인해주세요.')

    def data_search(self, ticker= None, stard_date=None, end_date=None):
        self.read_origin_data(ticker = ticker)
        self.origin['Datetime'] = to_datetime(datetime = self.origin['Datetime'],type = self.interval)
        if stard_date != None:
            self.origin = self.origin[self.origin['Datetime']>=stard_date]
        if end_date != None:
            self.origin = self.origin[self.origin['Datetime']<=end_date]
        return self.origin.set_index('Datetime').drop('Ticker', axis = 1).sort_index()



def to_datetime(datetime, type = 'min'):

    if type == 'min':
        datetime = datetime.apply(lambda x : dt.datetime.strptime(str(x)[:-6], '%Y-%m-%d %H:%M:%S'))
    elif type == 'day':
        datetime = datetime.apply(lambda x : dt.datetime.strptime(str(x), '%Y-%m-%d'))
    return datetime

