import numpy as np
import pandas as pd


def get_period(df:pd.core) -> str: 
    df.dropna(inplace = True)
    end_date = df.index.max()
    start_date = df.index.max()
    days_between = (end_date - start_date).days
    return abs(days_between) 

def annualize(rate, period):
    if period < 360:
        rate = ((rate-1) / period * 365) + 1
    elif period > 365:
        rate = rate ** (365 / period)
    else:
        rate = rate
    return round(rate, 4)

def sharp_ratio(df, rf_rate):
    '''
    Calculate sharpe ratio
    :param df:
    :param rf_rate:
    :return: Sharpe ratio
    '''
    period = get_period(df)
    rf_rate_daily = rf_rate / 365 + 1
    df['exs_rtn_daily'] = df['daily_rtn'] - rf_rate_daily
    exs_rtn_annual = (annualize(df['acc_rtn'][-1], period) - 1) - rf_rate
    exs_rtn_vol_annual = df['exs_rtn_daily'].std() * np.sqrt(365)
    sharpe_ratio = exs_rtn_annual / exs_rtn_vol_annual if exs_rtn_vol_annual>0 else 0
    return round(sharpe_ratio, 4)