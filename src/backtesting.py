import numpy as np
import pandas as pd

class BacktestBase(object):
    def __init__(self, df ,position, result_show =False):
            self.df =df
            self.position = position
            self.result_show = result_show
            self.df = self.evaluate(self.df, cost=.001)
            self.performance(self.df)


    def __get_period(self, df):

        df.dropna(inplace=True)
        # db에서 불러온 데이터를 바로 백테스팅을 할 수 있게 변환
        df = df.reset_index()
        end_date = df['rdatetime'].iloc[-1]
        start_date = df['rdatetime'].iloc[0]
        days_between = (end_date - start_date).days
        return abs(days_between)

    def __annualize(self, rate, period):
        if period < 360:
            rate = ((rate-1) / period * 365) + 1
        elif period > 365:
            rate = rate ** (365 / period)
        else:
            rate = rate
        return round(rate, 4)


    def __get_sharpe_ratio(self, df, rf_rate):
        '''
        Calculate sharpe ratio
        :param df:
        :param rf_rate:
        :return: Sharpe ratio
        '''
        period = self.__get_period(df)
        rf_rate_daily = rf_rate / 365 + 1
        df['exs_rtn_daily'] = df['daily_rtn'] - rf_rate_daily
        exs_rtn_annual = (self.__annualize(df['acc_rtn'][-1:], period) - 1) - rf_rate
        exs_rtn_vol_annual = df['exs_rtn_daily'].std() * np.sqrt(365)
        sharpe_ratio = exs_rtn_annual / exs_rtn_vol_annual if exs_rtn_vol_annual>0 else 0
        return round(sharpe_ratio, 4)
    def evaluate(self, df, cost= .1):
        '''
        Calculate daily returns and MDDs of portfolio
        :param df: The dataframe containing trading position
        :param cost: Transaction cost when sell
        :return: Returns, MDD
        '''
        df['signal_price'] = np.nan
        df['signal_price'].mask(df[self.position]== 1, df['adjClose'], inplace=True)
        df['signal_price'].mask(df[self.position]==-1, df['adjClose'], inplace=True)
        record = df[[self.position,'signal_price']].dropna()
        record['rtn'] = 1
        record['rtn'].mask(record[self.position]==-1, (record['signal_price']*(1-cost))/record['signal_price'].shift(1), inplace=True)
        record['acc_rtn'] = record['rtn'].cumprod()

        df['signal_price'].mask(df[self.position]== 10, df['adjClose'], inplace=True)
        df['rtn'] = record['rtn']
        df['rtn'].fillna(1, inplace=True)

        df['daily_rtn'] = 1
        df['daily_rtn'].mask(df[self.position] == 10, df['signal_price'] / df['signal_price'].shift(1), inplace=True)
        df['daily_rtn'].mask(df[self.position] == -1, (df['signal_price']*(1-cost)) / df['signal_price'].shift(1), inplace=True)
        df['daily_rtn'].fillna(1, inplace=True)
        df['acc_rtn'] = df['daily_rtn'].cumprod()
        df['acc_rtn_dp'] = ((df['acc_rtn']-1)*100).round(2)
        df['mdd'] = (df['acc_rtn'] / df['acc_rtn'].cummax()).round(4)
        df['bm_mdd'] = (df['adjClose'] / df['adjClose'].cummax()).round(4)
        df.drop(columns='signal_price', inplace=True)
        return df


    def performance(self, df, rf_rate=.01):
        '''
        Calculate additional information of portfolio
        :param df: The dataframe with daily returns
        :param rf_rate: Risk free interest rate
        :return: Number of trades, Number of wins, Hit ratio, Sharpe ratio, ...
        '''

        rst = {}
        rst['no_trades'] = (df[self.position]==1).sum()
        rst['no_win'] = (df['rtn']>1).sum()
        rst['acc_rtn'] = df['acc_rtn'][-1:].round(4)
        rst['hit_ratio'] = round((df['rtn']>1.0).sum() / rst['no_trades'], 4) if rst['no_trades']>0 else 0
        rst['avg_rtn'] = round(df[df['rtn']!=1.0]['rtn'].mean(), 4)
        rst['period'] = self.__get_period(df)
        rst['annual_rtn'] = self.__annualize(rst['acc_rtn'], rst['period'])
        rst['bm_rtn'] = round(df.iloc[-1,5]/df.iloc[0,5], 4)
        rst['sharpe_ratio'] = self.__get_sharpe_ratio(df, rf_rate)
        rst['mdd'] = df['mdd'].min()
        rst['bm_mdd'] = df['bm_mdd'].min()
        if self.result_show ==True:
            print('CAGR: ',round(rst['annual_rtn'].values[0] - 1,2))                       # 연간 수익
            print('Accumulated return:',round(rst['acc_rtn'].values[0] - 1,2))         # 
            print('Average return: ',round(rst['avg_rtn'] - 1,2))
            print('Benchmark return :',round(rst['bm_rtn']-1,2))
            print('Number of trades: ',(rst['no_trades']))
            print('Number of win:',(rst['no_win']))
            print('Hit ratio:',(rst['hit_ratio']))
            print('Investment period:',(rst['period']/365),'yrs')
            print('Sharpe ratio:',(rst['sharpe_ratio']))
            print('MDD:',(rst['mdd']-1)*100)
            print('Benchmark MDD:',(rst['bm_mdd']-1)*100)

        self.res = {'Symbol':self.df['ticker'].unique().item(),
                    'CAGR':(rst['annual_rtn'].values[0] - 1)*100,
                    'Accumulated_return':(rst['acc_rtn'].values[0] - 1)*100,
                    'Average_return': (rst['avg_rtn'] - 1)*100,
                    'MDD':(rst['mdd']-1)*100,
                    'Benchmark_return': round(rst['bm_rtn']-1,2),
                    'Number_of_trades': (rst['no_trades']),
                    'Number_of_win': (rst['no_win']),
                    'Hit_ratio': (rst['hit_ratio']),
                    'Investment_period(Year)': (rst['period']/365),
                    'Sharpe_ratio': (rst['sharpe_ratio']),
                    'MDD': (rst['mdd']-1)*100,
                    'Benchmark_MDD': (rst['bm_mdd']-1)*100}
        print(self.df.columns)
        print('백테스팅 성공')
        

        