from src.index_trend import *
from src.strategy import *

class backtester:
    def __init__(self, df, money, stock, total, strategy = 'rsi'):
        self.df = df
        self.strategy = strategy
        self.money = money
        self.stock = stock
        self.stock_name = self.df['Ticker']
        self.total = total
    def get_strategy(self,  **kwargs) :
        if self.strategy=="rsi" :
            index_df = rsi(self.df, **kwargs)
        elif self.strategy == 'macd':
            index_df = macd(self.df, **kwargs)
        elif self.strategy == 'envelope':
            index_df = envelope(self.df, **kwargs)
        elif self.strategy == 'bollinger':
            index_df = bollinger(self.df, **kwargs)
        elif self.strategy == 'stochastic':
            index_df = stochastic(self.df, **kwargs)
        return index_df
    def trading_signal(self, index_df, **kwargs):
        index_df = index_df.copy()
        if self.strategy=="rsi" :
            signal = index_df['RSI'].apply(lambda x : rsi_strategy(x, **kwargs))
        elif self.strategy == 'macd':
            signal = index_df.apply(lambda x : macd_strategy(x['macd'],x['macd_signal']))
        elif self.strategy == 'envelope':
            signal = index_df.apply(lambda x : envelope_strategy(x['Adj Close'],x['ub'], x['lb']))
        elif self.strategy == 'bollinger':
            signal = index_df.apply(lambda x : bollinger_strategy(x['Adj Close'],x['ub'], x['lb']))
        elif self.strategy == 'stochastic':
            signal = index_df.apply(lambda x : stochastic_strategy(x['slow_k'], x['slow_d'], **kwargs))
        index_df['signal'] = signal
        return index_df   
    def back_trading_result(self, index_df):
        m_list = []
        s_list = []
        r_list = []
        for idx, trade in enumerate(index_df['signal']):
            if trade == 1 and self.money > index_df.iloc[idx]['Adj Close']:
                self.money -= index_df.iloc[idx]['Adj Close']
                self.stock +=1
            elif trade == -1 and self.stock >0:
                self.money += index_df.iloc[idx]['Adj Close']
                self.stock -=1
            else:
                pass
            m_list.append(self.money)
            s_list.append(self.stock)
            self.total = self.money  + self.stock * index_df.iloc[idx]['Adj Close']
            r_list.append(self.total/m_list[0])

        return pd.DataFrame({'Datetime':index_df['Datetime'],'Ticker':index_df['Ticker'], 'signal':index_df['signal'], 'balance':m_list, 'stock':s_list, 'return':r_list})


