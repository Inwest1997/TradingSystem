import pandas as pd
import numpy as np


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
        df['Change'] = df['Adj Close'] - df['Adj Close'].shift(1)
        df['Up'] = np.where(df['Change'] >= 0, df['Change'], 0)
        df['Down'] = np.where(df['Change'] < 0, df['Change'].abs(), 0)
        # welles moving average
        df['AU'] = df['Up'].ewm(alpha=a, min_periods=w).mean()
        df['AD'] = df['Down'].ewm(alpha=a, min_periods=w).mean()
        df['RSI'] = df['AU'] / (df['AU'] + df['AD']) * 100
        return df[['RSI']]
    else:
        return None


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
    df['ema_short'] = df['Adj Close'].ewm(span=short).mean()
    df['ema_long'] = df['Adj Close'].ewm(span=long).mean()
    df['macd'] = (df['ema_short'] - df['ema_long']).round(2)
    df['macd_signal'] = df['macd'].ewm(span=signal).mean().round(2)
    df['macd_oscillator'] = (df['macd'] - df['macd_signal']).round(2)
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
    df['en_center'] = df['Adj Close'].rolling(w).mean()
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
    df['bo_center'] = df['Adj Close'].rolling(w).mean()
    df['sigma'] = df['Adj Close'].rolling(w).std(ddof=0)
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
        df['fast_k'] = ((df['Adj Close'] - df['Low'].rolling(n).min()) / (
                    df['High'].rolling(n).max() - df['Low'].rolling(n).min())).round(4) * 100
        df['slow_k'] = df['fast_k'].rolling(m).mean().round(2)
        df['slow_d'] = df['slow_k'].rolling(t).mean().round(2)
        # df.rename(columns={'Close': df['Ticker'].unique().item()}, inplace=True)
        # df.drop(columns=['High','Open','Low','Volume','Adj Close','fast_k'], inplace=True)
        return df[['slow_k', 'slow_d']]
    except:
        return 'Error. The stochastic indicator requires OHLC data and symbol. Try get_ohlc() to retrieve price data.'

def sma(df, w=5):
    df = df.copy()
    df[f'SMA({w})'] = df['Adj Close'].rolling(w).mean()

    return df[[f'SMA({w})']]

