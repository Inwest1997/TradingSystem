import pandas as pd
import numpy as np

def rsi(df, a = 1/14, w=14):
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
        df['Up'] = np.where(df['Change']>=0, df['Change'], 0)
        df['Down'] = np.where(df['Change'] <0, df['Change'].abs(), 0)
        # welles moving average
        df['AU'] = df['Up'].ewm(alpha=a, min_periods=w).mean()
        df['AD'] = df['Down'].ewm(alpha=a, min_periods=w).mean()
        df['RSI'] = df['AU'] / (df['AU'] + df['AD']) * 100
        return df[['Datetime', 'Ticker', 'RSI', 'Adj Close']]
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
    return df[['Datetime','Ticker', 'macd','macd_signal','macd_oscillator', 'Adj Close']]


def envelope(df, w=50, spread=.05):
    '''
    Calculate Envelope indicators
    :param df: Dataframe containing historical prices
    :param w: Window size
    :param spread: % difference from center line to determine band width
    :return: Dataframe of Envelope values
    '''
    df = df.copy()
    df['center'] = df['Adj Close'].rolling(w).mean()
    df['ub'] = df['center']*(1+spread)
    df['lb'] = df['center']*(1-spread)
    return df[['Datetime', 'Ticker', 'center','ub','lb', 'Adj Close']]


def bollinger(df, w=20, k=2):
    '''
    Calculate bollinger band indicators
    :param df: Dataframe containing historical prices
    :param w: Window size
    :param k: Multiplier to determine band width
    :return: Dataframe of bollinger band values
    '''
    df = df.copy()
    df['center'] = df['Adj Close'].rolling(w).mean()
    df['sigma'] = df['Adj Close'].rolling(w).std()
    df['ub'] = df['center'] + k * df['sigma']
    df['lb'] = df['center'] - k * df['sigma']
    return df[['Datetime', 'Ticker', 'center','ub','lb', 'Adj Close']]


def stochastic(df, symbol, n=14, m=3, t=3):
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
        df['fast_k'] = ( ( df['Adj Close'] - df['Low'].rolling(n).min() ) / ( df['High'].rolling(n).max() - df['Low'].rolling(n).min() ) ).round(4) * 100
        df['slow_k'] = df['fast_k'].rolling(m).mean().round(2)
        df['slow_d'] = df['slow_k'].rolling(t).mean().round(2)
        df.rename(columns={'Close':df['Ticker'].unique().item()}, inplace=True)
        # df.drop(columns=['High','Open','Low','Volume','Adj Close','fast_k'], inplace=True)
        return df[['Datetime', df['Ticker'].unique().item(), 'slow_k', 'slow_d', 'Adj Close']]
    except:
        return 'Error. The stochastic indicator requires OHLC data and symbol. Try get_ohlc() to retrieve price data.'




class stock_standard():
    def __init__(self, df, only_ma = True):
        self.df = df
        if only_ma == False:
            self.df = self.moving_average()
    
    def moving_average(self):
        df = self.df.copy()
        df['MA(120)'] = df['Adj Close'].rolling(120).mean()
        df['MA(60)'] = df['Adj Close'].rolling(60).mean()
        df['MA(20)'] = df['Adj Close'].rolling(20).mean()
        df['MA(5)'] = df['Adj Close'].rolling(5).mean()
        df.dropna(inplace=True)
        return df

    def s1(self):
        s = []
        for idx in range(len(self.df)):
            if self.df.iloc[idx]['MA(120)']< self.df.iloc[idx]['MA(60)'] and self.df.iloc[idx]['MA(60)']< self.df.iloc[idx]['MA(20)'] and self.df.iloc[idx]['MA(20)']< self.df.iloc[idx]['MA(5)'] and self.df.iloc[idx]['MA(120)']< self.df.iloc[idx]['Adj Close']:
                s.append(1)
            else:        
                s.append(0)
        return s

    def s2(self):
        s = []
        for idx in range(len(self.df)):
            if idx < 20:
                s.append(np.NAN)
            else:
                if self.df.iloc[idx]['High'] > self.df.iloc[idx-20]['High']:
                    s.append(1)
                else:
                    s.append(0)
        return s

    def s3(self):
        s = []
        for idx in range(len(self.df)):
            if idx < 1:
                s.append(np.NAN)
            else:
                if self.df.iloc[idx]['Volume']>self.df.iloc[idx-1]['Volume']*3:
                    s.append(1)
                else:
                    s.append(0)
        return s

    def s4(self):
        s = []
        for idx in range(len(self.df)):
            if idx < 2:
                s.append(np.NAN)
            else:
                if self.df.iloc[idx-1]['Volume']>self.df.iloc[idx-2]['Volume'] and self.df.iloc[idx-1]['Adj Close']>self.df.iloc[idx-2]['Adj Close'] :
                    s.append(1)
                else:
                    s.append(0)
        return s

    def s5(self, **kwargs):
        bol = bollinger(self.df,  **kwargs)[['center','lb', 'Adj Close']]
        s = []
        for idx in range(len(self.df)):
            if bol.iloc[idx]['Adj Close']<bol.iloc[idx]['center'] and bol.iloc[idx]['Adj Close']>bol.iloc[idx]['lb']:
                s.append(1)
            else:
                s.append(0)
        return s
   
    def calculator(self, standard = 2):
        df = self.df.copy()
        df['s1'] = self.s1()
        df['s2'] = self.s2()
        df['s3'] = self.s3()
        df['s4'] = self.s4()
        df['s5'] = self.s5()
        df.dropna(inplace = True)
        cnt = []
        for idx in range(len(df)):
            if df.iloc[idx]['s1'] + df.iloc[idx]['s2'] + df.iloc[idx]['s3'] + df.iloc[idx]['s4'] + df.iloc[idx]['s5'] >= standard:
                cnt.append(1)
            else:
                cnt.append(0)
        df['Standard'] = cnt
        return df
    