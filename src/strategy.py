from sqlalchemy import true
from .index import *
import pandas as pd
import numpy as np


def rsi_strategy(df,up=70, down=30, **kwrgs):
    df = pd.concat([df,rsi(df, **kwrgs)], axis =1)
    position = [-1 if df.iloc[i]['RSI']>up else 1 if df.iloc[i]['RSI']<down else 0 for i in range(len(df))] 
    return position
    # df.apply(lambda x : -1 if x['RSI'] > up else (1 if x['RSI'] < down else 0 ) ,axis =1)

def macd_strategy(df, **kwrgs):
    df = pd.concat([df,macd(df, **kwrgs)], axis =1)
    position = [-1 if df.iloc[i]['macd']<df.iloc[i]['macd_signal'] else 1 if df.iloc[i]['macd']>df.iloc[i]['macd_signal'] else 0 for i in range(len(df))] 
    return position
    # df.apply(lambda x : -1 if x['macd'] < x['macd_signal'] else (1 if x['macd'] > x['macd_signal'] else 0 ),axis=1 )


def envelope_strategy(df, **kwrgs):
    df = pd.concat([df,envelope(df, **kwrgs)], axis =1)
    position = [-1 if df.iloc[i]['Adj Close'] > df.iloc[i]['en_ub'] else 1 if df.iloc[i]['Adj Close']<df.iloc[i]['en_lb'] else 0 for i in range(len(df))] 
    return position
    # df.apply(lambda x : 1 if x['Adj Close'] < x['en_lb'] else (-1 if x['Adj Close'] > x['en_ub'] else 0 ) ,axis =1)


        
def bollinger_strategy(df, **kwrgs):
    df = pd.concat([df,bollinger(df, **kwrgs)], axis =1)
    position = [-1 if df.iloc[i]['Adj Close'] > df.iloc[i]['bo_ub'] else 1 if df.iloc[i]['Adj Close']<df.iloc[i]['bo_lb'] else 0 for i in range(len(df))] 
    return position
    # df.apply(lambda x : 1 if x['Adj Close'] < x['bo_lb'] else (-1 if x['Adj Close'] > x['bo_ub'] else 0 ) ,axis =1)



def stochastic_strategy(df, **kwrgs):
    df = pd.concat([df,stochastic(df, **kwrgs)], axis =1)
    position = [-1 if df.iloc[i]['slow_k'] > df.iloc[i]['slow_d'] else 1 if df.iloc[i]['slow_k']<df.iloc[i]['slow_d'] else 0 for i in range(len(df))] 
    return position
    df.apply(lambda x : 1 if x['slow_k'] < x['slow_d'] else (-1 if x['slow_k'] > x['slow_d'] else 0 ) ,axis =1  )

    

def cci_strategy(df, up =80 ,down = 20, **kwrgs):
    df = pd.concat([df,cci(df, **kwrgs)], axis =1)
    position = [-1 if df.iloc[i]['CCI']>up else 1 if df.iloc[i]['CCI']<down else 0 for i in range(len(df))] 
    return position

def wmr_strategy(df, **kwrgs):
    df = pd.concat([df,wmr(df, **kwrgs)], axis =1)
    position = [-1 if  df.iloc[i]['wmr'] > 80 else 1 if df.iloc[i]['wmr'] < 20 else 0 for i in range(len(df))] 
    return position

def obv_strategy(df, **kwrgs):
    df = pd.concat([df,obv(df, **kwrgs)], axis =1)
    position =  [-1 if df.iloc[i]['OBV'] > df.iloc[i]['OBV_mv5'] and df.iloc[i-1]['OBV'] < df.iloc[i-1]['OBV_mv5'] else 1 if df.iloc[i]['OBV']<df.iloc[i]['OBV_mv5'] and df.iloc[i-1]['OBV']>df.iloc[i-1]['OBV_mv5'] else 0 for i in range(len(df))] 
    return position
    

def sell_sum(df,ticker='GOOG',date= "2022-10-05",*args):
    sum = 0
    for i in args:
        print(df[(df['Date'] == date) & (df['Ticker']==ticker)][f'{i}_position'])
        sum+=df[(df['Date'] == date) & (df['Ticker']==ticker)][f'{i}_position'].values[0]
    if sum == len(args):
        print('매수')

def buy_sum(df,ticker='GOOG',date= "2022-10-05",*args):
    sum = 0
    for i in args:
        print(df[(df['Date'] == date) & (df['Ticker']==ticker)][f'{i}_position'])
        sum+=df[(df['Date'] == date) & (df['Ticker']==ticker)][f'{i}_position'].values[0]
    if abs(sum) == len(args) and sum < 0:
        print('매도')

def test(df, *args):
    for i in args:
        exec(f"df['{i}_position'] = {i}_strategy(df)")
    position_list = df.columns[-len(args):]
    # [i+'_position' for i in args]
    df['position'] = df[position_list].apply(lambda x :1 if np.array([i for i in x]).sum() == len(x) else  0, axis = 1)
    
    
    return df





def position_format(df):
    df = df.copy()
    new_position = []
    for idx in range(1, len(df)):
        if df['position'].iloc[idx] == 0 and df['position'].iloc[idx-1] == 0:
            new_position.append(2)
        else: new_position.append(df['position'].iloc[idx])
    df['postion'] = new_position
    return df