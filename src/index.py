import pandas as pd
import numpy as np
from fredapi import Fred


def rsi(df, a=1 / 14, w=14):
    '''
    Calculate RSI indicator
    :param df: Dataframe containing historical prices
    :param w: Window size
    :param a: alpha
    :return: Series of RSI values
    '''
    df = df.copy()
    if len(df) > w:
        df['Change'] = df['adjClose'] - df['adjClose'].shift(1)
        df['Up'] = np.where(df['Change'] >= 0, df['Change'], 0)
        df['Down'] = np.where(df['Change'] < 0, df['Change'].abs(), 0)
        # welles moving average
        df['AU'] = df['Up'].ewm(alpha=a, min_periods=w).mean()
        df['AD'] = df['Down'].ewm(alpha=a, min_periods=w).mean()
        df['RSI'] = df['AU'] / (df['AU'] + df['AD']) * 100
        
    else:
        df['RSI'] = 0
    
    return df[['RSI']]


def macd(df, short=12, long=26, signal=9):
    '''
    Calculate MACD indicators
    :param df: Dataframe containing historical prices
    :param short: Day length of short term MACD
    :param long: Day length of long term MACD
    :param signal: Day length of MACD signal
    :return: Dataframe of MACD values
    '''
    df = df.copy()
    try :
        df['ema_short'] = df['adjClose'].ewm(span=short).mean()
        df['ema_long'] = df['adjClose'].ewm(span=long).mean()
        df['macd'] = (df['ema_short'] - df['ema_long']).round(2)
        df['macd_signal'] = df['macd'].ewm(span=signal).mean().round(2)
        df['macd_oscillator'] = (df['macd'] - df['macd_signal']).round(2)
    except:
        df['macd'] =0
        df['macd_signal'] =0
        df['macd_oscillator'] =0



    return df[['macd', 'macd_signal', 'macd_oscillator']]


def envelope(df, w=50, spread=.05):
    '''
    Calculate Envelope indicators
    :param df: Dataframe containing historical prices
    :param w: Window size
    :param spread: % difference from center line to determine band width
    :return: Dataframe of Envelope values
    '''
    df = df.copy()
    df['en_center'] = df['adjClose'].rolling(w).mean()
    df['en_ub'] = df['en_center'] * (1 + spread)
    df['en_lb'] = df['en_center'] * (1 - spread)
    return df[['en_center', 'en_ub', 'en_lb']]


def bollinger(df, w=20, k=2):
    '''
    Calculate bollinger band indicators
    :param df: Dataframe containing historical prices
    :param w: Window size
    :param k: Multiplier to determine band width
    :return: Dataframe of bollinger band values
    '''
    df = df.copy()
    df['bo_center'] = df['adjClose'].rolling(w).mean()
    df['sigma'] = df['adjClose'].rolling(w).std(ddof=0)
    df['bo_ub'] = df['bo_center'] + k * df['sigma']
    df['bo_lb'] = df['bo_center'] - k * df['sigma']
    return df[['bo_center', 'bo_ub', 'bo_lb']]


def stochastic(df, n=14, m=3, t=3):
    '''
    Calculate stochastic indicators
    :param df: Dataframe containing historical prices
    :param symbol: Symbol or ticker of equity by finance.yahoo.com
    :param n: Day length of fast k stochastic
    :param m: Day length of slow k stochastic
    :param t: Day length of slow d stochastic
    :return: Dataframe of stochastic values
    '''
    df = df.copy()
    try:
        df['fast_k'] = ((df['adjClose'] - df['low'].rolling(n).min()) / (
                    df['high'].rolling(n).max() - df['low'].rolling(n).min())).round(4) * 100
        df['slow_k'] = df['fast_k'].rolling(m).mean().round(2)
        df['slow_d'] = df['slow_k'].rolling(t).mean().round(2)
        # df.rename(columns={'Close': df['Ticker'].unique().item()}, inplace=True)
        # df.drop(columns=['high','open','low','volume','adjClose','fast_k'], inplace=True)
        return df[['slow_k', 'fast_k']]
    except:
        return 'Error. The stochastic indicator requires OHLC data and symbol. Try get_ohlc() to retrieve price data.'

def cci(df, type = '단기'):
    df = df.copy()
    df['avg price'] = [(df.iloc[i]['open'] + df.iloc[i]['low'] + df.iloc[i]['high'])/3 for i in range(len(df))]
    p = 0
    if type == '단기':
        p=9
    else:
        p=14

    df['AMA'] = df['avg price'].rolling(p).mean()
    df['평균오차'] = [np.abs(df.iloc[i]['avg price'] - df.iloc[i]['AMA']) for i in range(len(df))]
    df['평균오차'] = df['평균오차'].rolling(p).mean()
    df['CCI'] = [(df.iloc[i]['avg price'] - df.iloc[i]['AMA'])/(df.iloc[i]['평균오차']*0.015) for i in range(len(df))]
    return df[['CCI']]


def bond(df):
    df = df.copy()
    try:
        fred = Fred(api_key='d929757b1ad9cd1d5115620a50badb0a')
        df['T10Y2Y'] = fred.get_series('T10Y2Y', df.index.min())
        return df[['T10Y2Y']]

    except:
        return 'Error, Cannot read bond data from FRED'


def vix(df):
    df = df.copy()
    try:
        fred = Fred(api_key='d929757b1ad9cd1d5115620a50badb0a')
        df['VIX'] = fred.get_series('VIXCLS', df.index.min())
        return df[['VIX']]

    except:
        return 'Error, Cannot read VIX data from FRED'
#     return


def obv(df):
    df = df.copy()  
    OBV = []
    OBV.append(0)
    for i in range(1, len(df['adjClose'])):
        if df['adjClose'][i] > df['adjClose'][i-1]: 
            OBV.append(OBV[-1] + df['volume'][i]) 
        elif df['adjClose'][i] < df['adjClose'][i-1]:
            OBV.append( OBV[-1] - df['volume'][i])
        else:
         OBV.append(OBV[-1])
    df['OBV'] = OBV
    df["OBV_mv20"] = df["OBV"].rolling(20).mean()
    return df[['OBV', 'OBV_mv20']]


def wmr(df, 기간 = 7):
    df = df.copy()

    df["wmr"] = 100 * ((df["high"].rolling(기간).max() - df["adjClose"]) / (df["high"].rolling(기간).max() - df["high"].rolling(기간).min()) )
    return df[["wmr"]]

def sma(df, w=5):
    df = df.copy()
    df[f'SMA({w})'] = df['adjClose'].rolling(w).mean()

    return df[[f'SMA({w})']]

def read_all(df):
    df = df.copy()  
    df['RSI'] = rsi(df)
    df[['macd', 'macd_signal', 'macd_oscillator']] = macd(df)
    df[['en_center', 'en_ub', 'en_lb']] = envelope(df)
    df[['slow_k', 'slow_d']] = stochastic(df)
    df['cci'] = cci(df)
    df['T10Y2Y'] = bond(df)
    df['VIX'] = vix(df)
    df[['OBV', 'OBV_mv20']] = obv(df)
    
    df['SMA(5)'] = sma(df, w = 5)
    df['SMA(10)'] = sma(df, w = 10)
    return df