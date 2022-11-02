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
    def __init__(self, data_type, db_info = info, table_name = 'daily_stock') -> None:
        '''
        date_type : db or csv
        '''
        self.table_name = table_name
        self.data_type = data_type
        self.end_date = dt.datetime.today()
        self.db_info = db_info

    def read_from_csv(self, dir = './Dataset',ticker = None):
        origin = pd.read_csv(f'{os.path.join(dir, self.table_name)}.csv', index_col=0)
        if ticker != None:
            self.origin = self.origin[self.origin['Ticker']==ticker]
        self.dir = dir
        # print('파일에서 CSV 데이터 불러오기 성공')
        return origin


    def read_from_db(self, sql = None, ticker = None):

        engine = sqlalchemy.create_engine(self.db_info)
        conn = engine.connect()
        if sql == None:
            sql = f'''SELECT * FROM {self.table_name}'''
        if ticker != None:
            sql = f'''SELECT * FROM {self.table_name} WHERE "{self.table_name}"."Ticker" = '{ticker}';'''

        origin = pd.read_sql(sql, conn)
        conn.close()
        origin['Datetime'] = origin['Datetime'].apply(lambda x : x.strftime('%Y-%m-%d')[:10])
        return origin


    def read_origin_data(self, **kwargs):
        if self.data_type == 'csv':
            origin = self.read_from_csv(**kwargs)
        elif self.data_type == 'db':
            origin = self.read_from_db(**kwargs)
        return origin

    def date_gap(self):
        return (self.end_date - dt.datetime.strptime(self.origin.Datetime.max(), '%Y-%m-%d')).days

                

    def stock_data_generator(self,  error_list = False, stock_list = stock_list, all = False) :
        total_df_list = []
        self.error_stock = []

        if all == True:
            st_date = dt.datetime.strftime(self.end_date - dt.timedelta(weeks=52*30), '%Y-%m-%d')
        else:
            st_date = dt.datetime.strftime(dt.datetime.strptime(self.origin.Datetime.max(),'%Y-%m-%d') + dt.timedelta(days=1), '%Y-%m-%d')

        for stock in tqdm(stock_list['Symbol']):
            try:
                _ = yf.download(tickers = stock, start = st_date, end = self.end_date, interval = '1d', progress=False, show_errors=False)
                if len(_) == 0:
                    self.error_stock.append(stock)
                else :
                    _.reset_index(inplace=True)
                    _.rename(columns={'Date':'Datetime'},inplace=True)
                    _['Ticker'] = stock
                    total_df_list.append(_)
            except:
                print('ERROR!')
                continue
        new = pd.concat(total_df_list)
        return new        
        

    def data_concat(self) :

        try:
            if self.origin==None:
                self.origin = self.read_origin_data()
            if self.new == None:
                self.new = self.stock_data_generator()
            print('SUCCESS!')
            
            concat = pd.concat([self.origin, self.new], axis=0)
            concat.reset_index(drop = True)
            concat.drop_duplicates(keep = 'first', inplace=True)
            return concat
        except Exception as e:
            print('에러 발생 : ',e)
            pass
        



    def upload_to_table(self, exist = 'append'):
        try :
            engine = sqlalchemy.create_engine(self.db_info)
            conn = engine.connect()
            self.new.to_sql(self.table_name, conn
                        , if_exists=  exist# ---append, replace
                        , index=False
                        , dtype={

                                "Ticker": String(10)
      
                                }
                        )

            conn.close()
        except Exception as e:
            print(e, '로 인해 DB 저장 실패')

    def upload_to_csv(self):
        concat = self.data_concat()
        try:
            concat.to_csv(f'./Dataset/{self.file}.csv',index=False)
        except Exception as e:
            print(e, '로 인해 CSV 저장 실패')


    def update(self) -> None:
        try:
            if self.data_type == 'csv':
                self.upload_to_csv()
            elif self.data_type == 'db':
                self.new = self.stock_data_generator()
                self.upload_to_table()
        except Exception as e:
            print(e, '로 인해 업로드 실패')


    def data_search(self, ticker= None, stard_date=None, end_date=None):
        df = self.read_origin_data(ticker = ticker)
        df['Datetime'] = to_datetime(datetime = df['Datetime'])
        if stard_date != None:
            df = df[df['Datetime']>=stard_date]
        if end_date != None:
            df = df[df['Datetime']<=end_date]
        if ticker !=  None:
            return df.set_index('Datetime').drop('Ticker', axis = 1).sort_index()
        else:
            return df
            



def to_datetime(datetime, type = 'min'):
    datetime = datetime.apply(lambda x : dt.datetime.strptime(str(x), '%Y-%m-%d'))
    return datetime

