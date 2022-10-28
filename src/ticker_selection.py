import numpy as np
import pandas as pd

from src.index import *
import warnings
warnings.filterwarnings("ignore")

class stock_standard():
    def __init__(self, df):
        self.df = df

    def s1(self):
        ma = self.df[['Adj Close']]
        ma['SMA(120)'] = sma(self.df,120)
        ma['SMA(60)'] = sma(self.df,60)
        ma['SMA(20)'] = sma(self.df,20)
        ma['SMA(5)']= sma(self.df,5)
        # ma.dropna(how = 'any', inplace = True)
        s = [np.NAN if ma.iloc[idx].isnull().any() else 1 if ma.iloc[idx]['SMA(120)'] < ma.iloc[idx]['SMA(60)'] and 
            ma.iloc[idx]['SMA(60)'] < ma.iloc[idx]['SMA(20)'] and
            ma.iloc[idx]['SMA(20)'] < ma.iloc[idx]['SMA(5)'] and
            ma.iloc[idx]['SMA(5)'] < ma.iloc[idx]['Adj Close'] 
        else 0 for idx in range(len(ma) )]
        return s

    def s2(self):
        s = [np.NAN if idx<20 else 1 if self.df.iloc[idx]['High'] > self.df.iloc[idx-20]['High'] else 0 for idx in range(len(self.df))]
        return s

    def s3(self):
        s = [np.NAN if idx < 1 else 1 if self.df.iloc[idx]['Volume'] > self.df.iloc[idx-1]['Volume']*3 else 0 for idx in range(len(self.df))]
        return s

    def s4(self):
        s = [np.NAN if idx < 2 else 1 if self.df.iloc[idx-1]['Volume']>self.df.iloc[idx-2]['Volume'] and self.df.iloc[idx-1]['Adj Close']>self.df.iloc[idx-2]['Adj Close'] else 0 for idx in range(len(self.df))]
        return s

    def s5(self, **kwargs):
        bol = bollinger(self.df,  **kwargs)[['bo_center','bo_lb']]
        s = [1 if self.df.iloc[idx]['Adj Close'] < bol.iloc[idx]['bo_center'] and self.df.iloc[idx]['Adj Close']> bol.iloc[idx]['bo_lb'] else 0 for idx in range(len(self.df))]
        return s

    def s6(self):
        s = [1 if self.df.iloc[idx]['Adj Close'] > self.df.iloc[idx]['Open'] else 0 for idx in range(len(self.df))]
        return s

    def s7(self):
        s = [np.NAN if idx == 0 else 1 if self.df.iloc[idx]['Open'] > self.df.iloc[idx-1]['Adj Close'] else 0 for idx in range(len(self.df))]
        return s

    def s8(self):
        s = [np.NAN if idx == 0 else 1 if self.df.iloc[idx]['High'] > self.df.iloc[idx-1]['High'] else 0 for idx in range(len(self.df))]
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
        pdi = self.df['High'] - self.df['High'].shift(1)
        mdi = self.df['Low'] - self.df['Low'].shift(1)
        adx = np.abs(pdi-mdi)/pdi*mdi*100
        s = [np.NAN if idx == 0 else 1 if pdi[idx] + adx[idx] > mdi[idx] and adx[idx]>adx[idx] else 0 for idx in range(len(self.df))]
        return s

    def calculator(self, standard = 5)
    
    def calculator(self, standard = 2):
        df = self.df.copy()
        df['s1'] = self.s1()
        df['s2'] = self.s2()
        df['s3'] = self.s3()
        df['s4'] = self.s4()
        df['s5'] = self.s5()
        df['s6'] = self.s6()
        df['s7'] = self.s7()
        df['s8'] = self.s8()
        df['s9'] = self.s9()
        df.dropna(inplace = True)
       
       
        cnt = [1 if df.iloc[idx]['s1'] + df.iloc[idx]['s2'] + df.iloc[idx]['s3'] + df.iloc[idx]['s4'] + df.iloc[idx]['s5'] + df.iloc[idx]['s6'] + df.iloc[idx]['s7'] + df.iloc[idx]['s8'] + df.iloc[idx]['s9'] >= standard else 0 for idx in range(len(df))]
        df['Standard'] = cnt
        return df