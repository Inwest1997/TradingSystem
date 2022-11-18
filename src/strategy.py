from .index import *
import pandas as pd
import numpy as np

def rsi_strategy(df,up=40, down=30, **kwrgs):
    df = pd.concat([df,rsi(df)], axis =1)
    df["trade"] = [-1 if df.iloc[i]['RSI']>up else 1 if df.iloc[i]['RSI']<down else 0 for i in range(len(df))] 
    buy =0
    position=[]
    for i in range(len(df)):
        if i ==len(df)-1:
            position.append(-1)
        else:
            if df.iloc[i]['trade'] == 1:
                buy=1
                position.append(1)
            elif df.iloc[i]['trade'] == 0 and buy ==1:
                position.append(10)
            elif (df.iloc[i]['trade'] == 0 or df.iloc[i]['trade'] == -1) and buy ==0 :
                position.append(0)
            else: 
                buy=0
                position.append(-1)
    return position

def macd_strategy(df, **kwrgs):
    df = pd.concat([df,macd(df, **kwrgs)], axis =1)
    df["trade"] = [-1 if df.iloc[i]['macd']<df.iloc[i]['macd_signal']  and df.iloc[i-1]['macd']> df.iloc[i-1]['macd_signal'] else 1 if df.iloc[i]['macd']>df.iloc[i]['macd_signal'] and df.iloc[i-1]['macd']<df.iloc[i-1]['macd_signal'] else 0 for i in range(len(df))] 
    buy =0
    position=[]
    for i in range(len(df)):
        if i ==len(df)-1:
            position.append(-1)
        else:
            if df.iloc[i]['trade'] == 1:
                buy=1
                position.append(1)
            elif df.iloc[i]['trade'] == 0 and buy ==1:
                position.append(10)
            elif (df.iloc[i]['trade'] == 0 or df.iloc[i]['trade'] == -1) and buy ==0 :
                position.append(0)
            else: 
                buy=0
                position.append(-1)
    return position

def envelope_strategy(df, **kwrgs):
    df = pd.concat([df,envelope(df, **kwrgs)], axis =1)
    df["trade"] = [-1 if df.iloc[i]['adjClose'] > df.iloc[i]['en_ub'] else 1 if df.iloc[i]['adjClose']<df.iloc[i]['en_lb'] else 0 for i in range(len(df))] 
    buy =0
    position=[]
    for i in range(len(df)):
        if i ==len(df)-1:
            position.append(-1)
        else:
            if df.iloc[i]['trade'] == 1:
                buy=1
                position.append(1)
            elif df.iloc[i]['trade'] == 0 and buy ==1:
                position.append(10)
            elif (df.iloc[i]['trade'] == 0 or df.iloc[i]['trade'] == -1) and buy ==0 :
                position.append(0)
            else: 
                buy=0
                position.append(-1)
    return position


def bollinger_strategy(df, **kwrgs):
    df = pd.concat([df,bollinger(df, **kwrgs)], axis =1)
    df["trade"] = [-1 if df.iloc[i]['adjClose'] > df.iloc[i]['bo_center'] else 1 if df.iloc[i]['adjClose']<df.iloc[i]['bo_lb'] else 0 for i in range(len(df))] 
    buy =0
    position=[]
    for i in range(len(df)):
        if i ==len(df)-1:
            position.append(-1)
        else:
            if df.iloc[i]['trade'] == 1:
                buy=1
                position.append(1)
            elif df.iloc[i]['trade'] == 0 and buy ==1:
                position.append(10)
            elif (df.iloc[i]['trade'] == 0 or df.iloc[i]['trade'] == -1) and buy ==0 :
                position.append(0)
            else: 
                buy=0
                position.append(-1)
    return position


def stochastic_strategy(df, **kwrgs):
    df = pd.concat([df,stochastic(df, **kwrgs)], axis =1)
    df["trade"] = [-1 if df.iloc[i]['fast_k'] > df.iloc[i]['slow_k'] and df.iloc[i-1]['fast_k'] < df.iloc[i-1]['slow_k'] else 1 if df.iloc[i]['fast_k']<df.iloc[i]['slow_k'] and df.iloc[i-1]['fast_k']>df.iloc[i-1]['slow_k'] else 0 for i in range(len(df))]     
    buy =0
    position=[]
    for i in range(len(df)):
        if i ==len(df)-1:
            position.append(-1)
        else:
            if df.iloc[i]['trade'] == 1:
                buy=1
                position.append(1)
            elif df.iloc[i]['trade'] == 0 and buy ==1:
                position.append(10)
            elif (df.iloc[i]['trade'] == 0 or df.iloc[i]['trade'] == -1) and buy ==0 :
                position.append(0)
            else: 
                buy=0
                position.append(-1)
    return position


def cci_strategy(df, up =80 ,down = 20, **kwrgs):
    df = pd.concat([df ,cci(df, **kwrgs)], axis =1)
    df["trade"] = [-1 if df.iloc[i]['CCI']>up else 1 if df.iloc[i]['CCI']<down else 0 for i in range(len(df))] 
    buy =0
    position=[]
    for i in range(len(df)):
        if i ==len(df)-1:
            position.append(-1)
        else:
            if df.iloc[i]['trade'] == 1:
                buy=1
                position.append(1)
            elif df.iloc[i]['trade'] == 0 and buy ==1:
                position.append(10)
            elif (df.iloc[i]['trade'] == 0 or df.iloc[i]['trade'] == -1) and buy ==0 :
                position.append(0)
            else: 
                buy=0
                position.append(-1)
    return position

def ovb_strategy(df, **kwrgs):
    df = pd.concat([df,obv(df, **kwrgs)], axis =1)
    df["trade"] =  [-1 if df.iloc[i]['OBV'] > df.iloc[i]['OBV_mv5'] and df.iloc[i-1]['OBV'] < df.iloc[i-1]['OBV_mv5'] else 1 if df.iloc[i]['OBV']<df.iloc[i]['OBV_mv5'] and df.iloc[i-1]['OBV']>df.iloc[i-1]['OBV_mv5'] else 0 for i in range(len(df))] 
    buy =0
    position=[]
    for i in range(len(df)):
        if i ==len(df)-1:
            position.append(-1)
        else:
            if df.iloc[i]['trade'] == 1:
                buy=1
                position.append(1)
            elif df.iloc[i]['trade'] == 0 and buy ==1:
                position.append(10)
            elif (df.iloc[i]['trade'] == 0 or df.iloc[i]['trade'] == -1) and buy ==0 :
                position.append(0)
            else: 
                buy=0
                position.append(-1)
    return position
    
def coin_strategy(df, **kwrgs):
    df["trade"] =  [-1 if df.iloc[i]['high'] > df.iloc[i]['open'] * 1.05 or df.iloc[i]['open'] > df.iloc[i-1]['adjClose'] * 1.05 or df.iloc[i]['open'] < df.iloc[i-1]['adjClose'] * 0.97  or df.iloc[i]['Low'] > df.iloc[i]['open'] * 0.97 else 1 if df.index[0] else 0 for i in range(len(df))] 
    buy =0
    position=[]
    for i in range(len(df)):
        if i ==len(df)-1:
            position.append(-1)
        else:
            if df.iloc[i]['trade'] == 1:
                buy=1
                position.append(1)
            elif df.iloc[i]['trade'] == 0 and buy ==1:
                position.append(10)
            elif (df.iloc[i]['trade'] == 0 or df.iloc[i]['trade'] == -1) and buy ==0 :
                position.append(0)
            else: 
                buy=0
                position.append(-1)
    return position


def test(df, *args):
    for i in args:
        exec(f"df['{i}_position'] = {i}_strategy(df)")
    position_list = [f'{i}_position' for i in args]
    df['position'] = df[position_list].apply(lambda x :1 if np.array([i for i in x]).sum() == len(args) else  (-1 if  abs(np.array([i for i in x]).sum())==len(args) else 0), axis = 1)
    

    price = 0
    position = []
    for i in range(len(df)):
        if df.iloc[i]['position'] == 1:
            price =  df.iloc[i]['adjClose']
            position.append(1)
        elif df.iloc[i]['position'] == 0 and price >0:  
            position.append(10)
        elif (df.iloc[i]['position'] == 0 or df.iloc[i]['position'] == -1) and price ==0 :
            position.append(0)
        elif df.iloc[i]['adjClose'] / price > 1.07 or df.iloc[i]['adjClose'] / price < 0.97:
            price=0
            position.append(-1)
        else:
            price=0
            position.append(-1)

    a=1
    k=[]
    for i in position:
        if a==1:
            if i == -1:
                a=0
            k.append(i)
        else:
            k.append(0)

    df['position']= k
    # return position
    return df








# def coin_strategy(df, A = 1.07 , B = 0.98 ,**kwrgs):

#     df["trade"] =[1] + [-1 if df.iloc[i]['Low'] > df.iloc[i-1]['Adj Close'] * A or df.iloc[i]['Adj Close'] > df.iloc[i-1]['Adj Close'] * A or df.iloc[i]['High'] > df.iloc[i-1]['Adj Close'] * A or df.iloc[i]['Open'] > df.iloc[i-1]['Adj Close'] * A 
#                             or df.iloc[i]['Open'] < df.iloc[i-1]['Adj Close'] * B  or df.iloc[i]['Low'] < df.iloc[i-1]['Adj Close'] * B or df.iloc[i]['High'] < df.iloc[i-1]['Adj Close'] * B or df.iloc[i]['Adj Close'] < df.iloc[i-1]['Adj Close'] * B else 0 for i in range(1, len(df))]
#     buy =0
#     position=[]
#     for i in range(len(df)):

#         if i ==len(df)-1:
#             position.append(-1)
         
#         else:
#             if df.iloc[i]['trade'] == 1:
#                 buy=1
#                 position.append(1)
#             elif df.iloc[i]['trade'] == 0 and buy ==1:
#                 position.append(10)
#             elif (df.iloc[i]['trade'] == 0 or df.iloc[i]['trade'] == -1) and buy ==0 :
#                 position.append(0)
#             else: 
#                 buy=0
#                 position.append(-1)
#     a=1
#     k=[]
#     for i in position:
#         if a==1:
#             if i == -1:
#                 a=0
#             k.append(i)
#         else:
#             k.append(0)


#     return position

# def coin_strategy(df, **kwrgs):
#     df["trade"] =[1] + [-1 if df.iloc[i]['Open'] * 1.10 > df.iloc[i]['High']  or df.iloc[i]['Open'] > df.iloc[i-1]['Adj Close'] * 1.05 or df.iloc[i]['Open'] > df.iloc[i-1]['Adj Close'] * 0.97  or df.iloc[i]['Low'] > df.iloc[i]['Open'] * 0.97 else 0 for i in range(1, len(df))] 
#     buy =0
#     position=[]
#     for i in range(len(df)):

#         if i ==len(df)-1:
#             position.append(-1)
         
#         else:
#             if df.iloc[i]['trade'] == 1:
#                 buy=1
#                 position.append(1)
#             elif df.iloc[i]['trade'] == 0 and buy ==1:
#                 position.append(10)
#             elif (df.iloc[i]['trade'] == 0 or df.iloc[i]['trade'] == -1) and buy ==0 :
#                 position.append(0)
#             else: 
#                 buy=0
#                 position.append(-1)
#     a=1
#     k=[]
#     for i in position:
#         if a==1:
#             if i == -1:
#                 a=0
#             k.append(i)
#         else:
#             k.append(0)


#     return position