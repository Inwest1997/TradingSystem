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
    def __init__(self, data_type, db_info = info, dir = "./Dataset/", table_name = 'daily_stock') -> None:
        '''
        date_type : db or csv
        '''
        self.dir = dir
        self.table_name = table_name
        self.data_type = data_type
        self.end_date = dt.datetime.today()
        self.db_info = db_info
        self.origin = pd.DataFrame(columns=['Datetime','Open','High','Low','Close','Adj Close','Volume'])

    def read_from_csv(self,ticker = None):
        df = pd.read_csv(f'{os.path.join(self.dir, self.table_name)}.csv')
        if ticker != None:
            df = df[df['Ticker']==ticker]
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        # print('파일에서 CSV 데이터 불러오기 성공')
        self.origin = df
        return df


    def read_from_db(self, sql = None, ticker = None):

        engine = sqlalchemy.create_engine(self.db_info)
        conn = engine.connect()
 
        if ticker != None:
            sql = f'''SELECT * FROM {self.table_name} WHERE "Ticker" = '{ticker}';'''
            df = pd.read_sql(sql, conn)
            self.origin_ticker = df
        else:
            sql = f'''SELECT * FROM {self.table_name}'''
            df = pd.read_sql(sql, conn)
            self.origin = df 

        conn.close()
        return df


    def read_origin_data(self, **kwargs):

        if self.data_type == 'csv':
            df = self.read_from_csv(**kwargs)
        elif self.data_type == 'db':
            df = self.read_from_db(**kwargs)
        print('기존 데이터 불러오기 성공')
        return df

    def date_gap(self):
        return (self.end_date - self.origin.Datetime.max()).days

                

    def stock_data_generator(self,  error_list = False, stock_list = stock_list, all = False) :
        total_df_list = []
        # self.error_stock = []

        if all == True:
            st_date = self.end_date - dt.timedelta(weeks=52*30)
        else:
            st_date = self.origin.Datetime.max() + dt.timedelta(days=1)

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
        print('새로운 데이터 불러오기 성공')
        return new        
        

    def data_concat(self) :
        try:
            if self.origin.shape[0] == 0:
                self.origin = self.read_origin_data()
            self.new = self.stock_data_generator()
            print('SUCCESS!')
            
            self.concat = pd.concat([self.origin, self.new], axis=0)
            self.concat.reset_index(drop = True)
            self.concat.drop_duplicates(keep = 'first', inplace=True)
            return self.concat
        except Exception as e:
            print('에러 발생 : ',e)
            pass
        



    def upload_to_table(self, ):
        try :
            engine = sqlalchemy.create_engine(self.db_info)
            conn = engine.connect()
            self.new.to_sql(self.table_name, conn
                        , if_exists= 'append'
                        , index=False
                        , dtype={

                                "Ticker": String(10)
      
                                }
                        )

            conn.close()
        except Exception as e:
            print(e, '로 인해 DB 저장 실패')

    def upload_to_csv(self):
        try:
            self.concat.to_csv(f'./Dataset/{self.file}.csv',index=False)
        except Exception as e:
            print(e, '로 인해 CSV 저장 실패')


    def update(self) -> None:
        self.data_concat()
        try:
            if self.data_type == 'csv':
                self.upload_to_csv()
            elif self.data_type == 'db':
                self.upload_to_table()
        except Exception as e:
            print(e, '로 인해 업로드 실패')




    # def data_search(self, ticker= None, stard_date=None, end_date=None):
    #     try:
    #         if self.origin.shape[0] == 0:
    #             if  ticker != None:
    #                 df = self.read_origin_data(ticker = ticker)

    #             else:
    #                 df = self.read_origin_data()

    #         elif self.origin.shape[0] > 0:
    #             if  ticker != None:
    #                 df = self.origin[self.origin['Ticker']==ticker]

    #             else:
    #                 df = self.origin

    #         if stard_date != None:
    #             df = df[df['Datetime']>=stard_date]
    #         if end_date != None:
    #             df = df[df['Datetime']<=end_date]
            
    #         if ticker != None:
    #             return df.drop('Ticker',axis=1).set_index('Datetime')
    #         else:
    #             return df
    #     except Exception as e:
    #         print(e, '로 인해 데이터를 불러오지 못 했습니다.')
            

    def data_search(self, ticker= None, stard_date=None, end_date=None):
        try:
            if ticker != None:
                if self.origin.shape[0] == 0:
                    df = self.read_origin_data(ticker = ticker)

                elif self.origin.shape[0] > 0:
                    df = self.origin[self.origin['Ticker']==ticker]

                df = df.drop('Ticker',axis=1).set_index('Datetime')


            else:
                if self.origin.shape[0] == 0:
                    df = self.read_origin_data()

                elif self.origin.shape[0] > 0:

                    df = self.origin
   
            if stard_date != None:
                df = df[df['Datetime']>=stard_date]
            if end_date != None:
                df = df[df['Datetime']<=end_date]
            
            return df.sort_index()
        except Exception as e:
            print(e, '로 인해 데이터를 불러오지 못 했습니다.')          

    # def data_search(self, ticker= None, stard_date=None, end_date=None):
    #     try:
    #         if self.origin.shape[0] == 0:
    #             df = self.read_origin_data()
    #         else:
    #             df = self.origin

    #         if ticker != None:
    #             df = self.origin[self.origin['Ticker']==ticker]
    #             df = df.drop('Ticker',axis=1).set_index('Datetime')
   
    #         if stard_date != None:
    #             df = df[df['Datetime']>=stard_date]
    #         if end_date != None:
    #             df = df[df['Datetime']<=end_date]
    #         return df
    #     except Exception as e:
    #         print(e, '로 인해 데이터를 불러오지 못 했습니다.')  


# def to_datetime(datetime):
#     datetime = datetime.apply(lambda x : dt.datetime.strptime(str(x), '%Y-%m-%d'))
#     return datetime

