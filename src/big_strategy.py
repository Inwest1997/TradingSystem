import numpy as np
import pandas as pd 
from .backtesting import *
from .data_loader import *
from .strategy import *



# temp = ['rsi','macd','envelope','bollinger','stochastic']


def big_strategy(df):
    df = df.copy()
    temp = ['rsi','macd','envelope','bollinger','stochastic' , 'cci']
    result = {}
    result_list = []
    for i in temp:
        exec(f"df['{i}_position'] = {i}_strategy(df)")
        wow = BacktestBase(df,f'{i}_position')
        result[i] = wow.res
    test_list = [[i, result[i]['Accumulated_return']] for i in result ]
    a = 0
    for i in range(len(test_list)):
        for j in range(len(test_list)):
            if test_list[i][1]>test_list[j][1]:
                a=test_list[i]
                test_list[i] = test_list[j]
                test_list[j] = a
    print(test_list)

    df = test(df,test_list[0][0],test_list[1][0])

    return BacktestBase(df,'position',True).res

