# from sqlalchemy import true
# from src.index import *

# '''
# buy signal : 1
# sell signal : -1
# maintain signal : 0

# '''

# def rsi_strategy(df,up, down):
#     df = pd.concat([df,rsi(df)], axis =1)
#     print(df['RSI'].head())
#     df['rsi_position'] = df.T.apply(lambda x : -1 if x['RSI'] > up else (1 if x['RSI'] < down else 0 ) )
#     return df

# def macd_strategy(df):
#     df = pd.concat([df,macd(df)], axis =1)
#     df['macd_position'] = df.T.apply(lambda x : -1 if x['macd'] < x['macd_signal'] else (1 if x['macd'] > x['macd_signal'] else 0 ) )
#     return df


# def envelope_strategy(df):
#     df = pd.concat([df,envelope(df)], axis =1)
#     df['envelope_position'] = df.T.apply(lambda x : 1 if x['Adj Close'] < x['en_lb'] else (-1 if x['Adj Close'] > x['en_ub'] else 0 ) )
#     return df


        
# def bollinger_strategy(df):
#     df = pd.concat([df,bollinger(df)], axis =1)
#     df['bollinger_position'] = df.T.apply(lambda x : 1 if x['Adj Close'] < x['bo_lb'] else (-1 if x['Adj Close'] > x['bo_ub'] else 0 ) )
#     return df


# def stochastic_strategy(df):
#     df = pd.concat([df,stochastic(df)], axis =1)
#     df['stochastic_position'] = df.T.apply(lambda x : 1 if x['slow_k'] < x['slow_d'] else (-1 if x['slow_k'] > x['slow_d'] else 0 ) )
#     return df






from sqlalchemy import true
from src.index import *
import pandas as pd
import numpy as np

'''
buy signal : 1
sell signal : -1
maintain signal : 0
'''

# def rsi_strategy(df,up, down):
#     df = pd.concat([df,rsi(df)], axis =1).set_index('Date')
#     df['position'] = df.T.apply(lambda x : -1 if x['RSI'] > up else (1 if x['RSI'] < down else 0 ) )
#     return df

# def macd_strategy(df):
#     df = pd.concat([df,macd(df)], axis =1).set_index('Date')
#     df['position'] = df.T.apply(lambda x : -1 if x['macd'] < x['macd_signal'] else (1 if x['macd'] > x['macd_signal'] else 0 ) )
#     return df


# def envelope_strategy(df):
#     df = pd.concat([df,envelope(df)], axis =1).set_index('Date')
#     df['position'] = df.T.apply(lambda x : -1 if x['Adj Close'] < x['en_lb'] else (1 if x['Adj Close'] > x['en_ub'] else 0 ) )
#     return df


        
# def bollinger_strategy(df):
#     df = pd.concat([df,bollinger(df)], axis =1).set_index('Date')
#     df['position'] = df.T.apply(lambda x : -1 if x['Adj Close'] < x['bo_lb'] else (1 if x['Adj Close'] > x['bo_ub'] else 0 ) )
#     return df


# def stochastic_strategy(df):
#     df = pd.concat([df,stochastic(df)], axis =1).set_index('Date')
#     df['position'] = df.T.apply(lambda x : -1 if x['slow_k'] < x['slow_d'] else (1 if x['slow_k'] > x['slow_d'] else 0 ) )
#     return df


def rsi_strategy(df,up, down, **kwrgs):
    df = pd.concat([df,rsi(df, **kwrgs)], axis =1)
    df['rsi_position'] = df.T.apply(lambda x : -1 if x['RSI'] > up else (1 if x['RSI'] < down else 0 ) )
    return df

def macd_strategy(df, **kwrgs):
    df = pd.concat([df,macd(df, **kwrgs)], axis =1)
    df['macd_position'] = df.T.apply(lambda x : -1 if x['macd'] < x['macd_signal'] else (1 if x['macd'] > x['macd_signal'] else 0 ) )
    return df


def envelope_strategy(df, **kwrgs):
    df = pd.concat([df,envelope(df, **kwrgs)], axis =1)
    df['envelope_position'] = df.T.apply(lambda x : 1 if x['Adj Close'] < x['en_lb'] else (-1 if x['Adj Close'] > x['en_ub'] else 0 ) )
    return df


        
def bollinger_strategy(df, **kwrgs):
    df = pd.concat([df,bollinger(df, **kwrgs)], axis =1)
    df['bollinger_position'] = df.T.apply(lambda x : 1 if x['Adj Close'] < x['bo_lb'] else (-1 if x['Adj Close'] > x['bo_ub'] else 0 ) )
    return df


def stochastic_strategy(df, **kwrgs):
    df = pd.concat([df,stochastic(df, **kwrgs)], axis =1)
    df['stochastic_position'] = df.T.apply(lambda x : 1 if x['slow_k'] < x['slow_d'] else (-1 if x['slow_k'] > x['slow_d'] else 0 ) )
    return df



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
    kwargs = [i+'_position' for i in kwargs]
    df['position'] = df[args].T.apply(lambda x :1 if np.array([i for i in x]).sum() == len(x) else  0)
    return df




