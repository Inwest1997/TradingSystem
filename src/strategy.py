from sqlalchemy import true
from src.index_trend import *

'''
buy signal : 1
sell signal : -1
maintain signal : 0

'''

def rsi_strategy(rsi, up, down):
    if rsi > up :
        return -1
    elif rsi < down:
        return 1
    else:
        return 0

def macd_strategy(macd, macd_signal):
    if macd < macd_signal:
        return 1
    elif macd > macd_signal:
        return -1
    else:
        return 0


def envelope_strategy(price, ub, lb):
    if price < lb:
        return 1
    elif price > ub:
        return -1
    else:
        return 0

        
def bollinger_strategy(price, ub, lb):
    if price < lb:
        return 1
    elif price > ub:
        return -1
    else:
        return 0


def stochastic_strategy(slow_k, slow_d):
    if slow_k > slow_d : 
        return -1
    elif slow_k < slow_d:
        return 1
    else : 
        return 0




