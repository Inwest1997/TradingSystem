import numpy as np
import pandas as pd

from .index import *
import warnings
warnings.filterwarnings("ignore")

class stock_standard():
    def __init__(self, df):
        self.df = df

    def s1(self):
        ma = self.df[['adjClose']]
        ma['SMA(120)'] = sma(self.df,120)
        ma['SMA(60)'] = sma(self.df,60)
        ma['SMA(20)'] = sma(self.df,20)
        ma['SMA(5)']= sma(self.df,5)
        # ma.dropna(how = 'any', inplace = True)
        s = [np.NAN if ma.iloc[idx].isnull().any() else 1 if ma.iloc[idx]['SMA(120)'] < ma.iloc[idx]['SMA(60)'] and 
            ma.iloc[idx]['SMA(60)'] < ma.iloc[idx]['SMA(20)'] and
            ma.iloc[idx]['SMA(20)'] < ma.iloc[idx]['SMA(5)'] and
            ma.iloc[idx]['SMA(5)'] < ma.iloc[idx]['adjClose'] 
        else 0 for idx in range(len(ma) )]
        return s

    def s2(self):
        s = [np.NAN if idx<20 else 1 if self.df.iloc[idx]['high'] > self.df.iloc[idx-20]['high'] else 0 for idx in range(len(self.df))]
        return s

    def s3(self):
        s = [np.NAN if idx < 1 else 1 if self.df.iloc[idx]['volume'] > self.df.iloc[idx-1]['volume']*3 else 0 for idx in range(len(self.df))]
        return s

    def s4(self):
        s = [np.NAN if idx < 2 else 1 if self.df.iloc[idx-1]['volume']>self.df.iloc[idx-2]['volume'] and self.df.iloc[idx-1]['adjClose']>self.df.iloc[idx-2]['adjClose'] else 0 for idx in range(len(self.df))]
        return s

    def s5(self, **kwargs):
        bol = bollinger(self.df,  **kwargs)[['bo_center','bo_lb']]
        s = [1 if self.df.iloc[idx]['adjClose'] < bol.iloc[idx]['bo_center'] and self.df.iloc[idx]['adjClose']> bol.iloc[idx]['bo_lb'] else 0 for idx in range(len(self.df))]
        return s

    def s6(self):
        s = [1 if self.df.iloc[idx]['adjClose'] > self.df.iloc[idx]['open'] else 0 for idx in range(len(self.df))]
        return s

    def s7(self):
        s = [np.NAN if idx == 0 else 1 if self.df.iloc[idx]['open'] > self.df.iloc[idx-1]['adjClose'] else 0 for idx in range(len(self.df))]
        return s

    def s8(self):
        s = [np.NAN if idx == 0 else 1 if self.df.iloc[idx]['high'] > self.df.iloc[idx-1]['high'] else 0 for idx in range(len(self.df))]
        return s

    def s9(self):
        '''
        PDI = 현재고가 - 전일고가
        MDI = 전일저가 - 현재저가
        PDI가 MDI 밑에 있을 경우 매도
        ADX = |PDI - MDI| / PDI * MDI * 100
        ADX가 상승할 경우 : 상승
        ADX가 하락할 경우 : 하락
        PDI + ADX 가 MID 위에 있을때 ADX가 상승할 경우: 선정
        '''
        pdi = self.df['high'] - self.df['high'].shift(1)
        mdi = self.df['low'] - self.df['low'].shift(1)
        adx = np.abs(pdi-mdi)/pdi*mdi*100
        s = [np.NAN if idx == 0 else 1 if pdi[idx] + adx[idx] > mdi[idx] and adx[idx]>adx[idx] else 0 for idx in range(len(self.df))]
        return s

    def calculator(self, standard = 2):
        df = self.df.copy()
        for i in range(1,9):
            exec(f"df['s{i}']=self.s{i}()")
        # df['s1'] = self.s1()
        # df['s2'] = self.s2()
        # df['s3'] = self.s3()
        # df['s4'] = self.s4()
        # df['s5'] = self.s5()
        # df['s6'] = self.s6()
        # df['s7'] = self.s7()
        # df['s8'] = self.s8()
        # df['s9'] = self.s9()
        df.dropna(inplace = True)
        # cnt = [1 if df.iloc[idx]['s1'] + df.iloc[idx]['s2'] + df.iloc[idx]['s3'] + df.iloc[idx]['s4'] + df.iloc[idx]['s5'] + df.iloc[idx]['s6'] + df.iloc[idx]['s7'] + df.iloc[idx]['s8'] + df.iloc[idx]['s9'] >= standard else 0 for idx in range(len(df))]
        # df['Standard'] = cnt
        self.calculate = df
        return df


# class ticker_Select:
#     def __init__(self, df, date, cnt):
#         pass
#     def ticker_select(self, date = None, cnt = 5):
#         if date == None:
#             date = self.calculate['rdatetime'].max()
#         dummy_df =  self.calculate[self.calculate['rdatetime']==date]

