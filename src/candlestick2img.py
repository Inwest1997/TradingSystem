from tqdm import trange, tqdm
from scipy import stats
import pandas as pd
import numpy as np
import time

# from utils import util_multi as mul
import multiprocessing as mp


def auto_multi(data, tasks_ls, pro_num):
    '''
    Args:
        data (dataframe): csv data after `process_data` function.
    Returns:
        res_ls (dict):
            keys: 'evening', 'morning', ...
            values: [0, 0, 1, 0, 0, ...], [], ...
    '''
    tasks_num = len(tasks_ls.copy())
    
    # Create a queue for all tasks
    q = mp.Queue()

    # Maximum of ps_ls & tmp_ls depends on `pro_num`
    # Maximum of res_ls depends on number of `tasks_num`
    ps_ls = []
    res_ls, tmp_ls = [], []
    while True:
        # termination condition
        if len(res_ls) >= tasks_num:
            break

        if (len(ps_ls) >= pro_num):
            
            for p in ps_ls:
                p.start()

            while True:
                # termination condition
                if len(tmp_ls) >= pro_num:
                    res_ls.extend(tmp_ls)
                    tmp_ls.clear()
                    break

                if q.qsize():
                    tmp_res = q.get()
                    tmp_ls.append(tmp_res)
                    
            for p in ps_ls:
                p.join()

            ps_ls.clear()

        if tasks_ls:
            quotient = len(tasks_ls) // pro_num
            remainder = len(tasks_ls) % pro_num
            if quotient:
                for _ in range(pro_num):
                    task = tasks_ls.pop()
                    ps_ls.append(mp.Process(target=task, args=(data, q, True,)))
            else:
                for _ in range(remainder):
                    task = tasks_ls.pop()
                    ps_ls.append(mp.Process(target=task, args=(data, q, True,)))
                pro_num = remainder
    
    return res_ls


# from utils import util_process as pro
from sklearn.linear_model import LinearRegression
import numpy as np
import pickle


def load_pkl(pkl_name):
    '''
    Args:
        pkl_name (string): path for pickle.
    
    Returns:
        (dict): including following structure
            `raw time-series data` (N, 32, 4):
                'train_data', 'val_data', 'test_data'
            `gasf data` (N, 32, 32, 4):
                'train_gaf', 'val_gaf', 'test_gaf'
            `label data` (N, 3):
                'train_label', 'val_label', 'test_label',
            `one-hot label data` (N, 9):
                'train_label_arr', 'val_label_arr', 'test_label_arr'
    '''
    # load data from data folder
    with open(pkl_name, 'rb') as f:
        data = pickle.load(f)
    return data


def ts2gasf(ts, max_v, min_v):
    '''
    Args:
        ts (numpy): (N, )
        max_v (int): max value for normalization
        min_v (int): min value for normalization
    Returns:
        gaf_m (numpy): (N, N)
    '''
    # Normalization : 0 ~ 1
    if max_v == min_v:
        gaf_m = np.zeros((len(ts), len(ts)))
    else:
        ts_nor = np.array((ts-min_v) / (max_v-min_v))
        # Arccos
        ts_nor_arc = np.arccos(ts_nor)
        # GAF
        gaf_m = np.zeros((len(ts_nor), len(ts_nor)))
        for r in range(len(ts_nor)):
            for c in range(len(ts_nor)):
                gaf_m[r, c] = np.cos(ts_nor_arc[r] + ts_nor_arc[c])
    return gaf_m


def get_gasf(arr):
    '''Convert time-series to gasf    
    Args:
        arr (numpy): (N, ts_n, 4)
    Returns:
        gasf (numpy): (N, ts_n, ts_n, 4)
    Todos:
        add normalization together version
    '''
    arr = arr.copy()
    gasf = np.zeros((arr.shape[0], arr.shape[1], arr.shape[1], arr.shape[2]))
    for i in range(arr.shape[0]):
        for c in range(arr.shape[2]):
            each_channel = arr[i, :, c]
            c_max = np.amax(each_channel)
            c_min = np.amin(each_channel)
            each_gasf = ts2gasf(each_channel, max_v=c_max, min_v=c_min)
            gasf[i, :, :, c] = each_gasf
    return gasf


def gasf2ts(arr):
    '''
    Args:
        arr (numpy array):  (32, 32)
    Returns:
        numpy.series: (1d)
    '''
    # Get element from diagonal
    diag_v = np.zeros((arr.shape[0],))
    for i in range(arr.shape[0]):
        diag_v[i] = arr[i, i]
    # Inverse to Arc
    diag_v_arc = np.arccos(diag_v) / 2
    # Inverse to Normalized ts
    ts = np.cos(diag_v_arc)
    return ts


def ohlc2culr(ohlc):
    '''
    Args:
        ohlc (numpy): (N, ts_n, 4)
    Returns:
        culr (numpy): (N, ts_n, 4)
    '''
    culr = np.zeros((ohlc.shape[0], ohlc.shape[1], ohlc.shape[2]))
    culr[:, :, 0] =  ohlc[:, :, -1]
    culr[:, :, 1] = ohlc[:, :, 1] - np.maximum(ohlc[:, :, 0], ohlc[:, :, -1])
    culr[:, :, 2] = np.minimum(ohlc[:, :, 0], ohlc[:, :, -1]) - ohlc[:, :, 2]
    culr[:, :, 3] = ohlc[:, :, -1] - ohlc[:, :, 0]
    return culr


def culr2ohlc(culr_n, culr):
    '''
    Args:
        culr_n (numpy): (N, ts_n, 4)
        culr (numpy): (N, ts_n, 4)
    Returns:
        ohlc (numpy): (N, ts_n, 4)
    '''
    ohlc = np.zeros((*culr_n.shape, ))
    for i in range(culr_n.shape[0]):
        for c in range(culr_n.shape[-1]):
            # get min & max from data before normalized
            each_culr = culr[i, :, c]
            min_v = np.amin(each_culr)
            max_v = np.amax(each_culr)
            # inverse normalization
            each_culr_n = culr_n[i, :, c]
            culr_n[i, :, c] = (each_culr_n * (max_v - min_v)) + min_v
        # convert culr to ohlc
        ohlc[i, :, -1] = culr_n[i, :, 0]
        ohlc[i, :, 0] = ohlc[i, :, -1] - culr_n[i, :, -1]
        ohlc[i, :, 1] = culr_n[i, :, 1] + np.maximum(ohlc[i, :, 0], ohlc[i, :, -1])
        ohlc[i, :, 2] = np.minimum(ohlc[i, :, 0], ohlc[i, :, -1]) - culr_n[i, :, 2]
    return ohlc


def get_slope(series):
    y = series.values.reshape(-1, 1)
    x = np.array(range(1, series.shape[0] + 1)).reshape(-1,1)
    model = LinearRegression()
    model.fit(x, y)
    slope = model.coef_
    return slope


def get_trend(slope):
    '''Need to run `process_data` first with slope only, then calculate by yourself.
    25 percentile: 7.214285714286977e-05
    '''
    slope = np.array(slope)
    thres = 7.214285714286977e-05
    if (slope >= thres):
        return 1
    elif (slope <= -thres):
        return -1
    else:
        return 0

def rename(data):
    rename_dc = {'Gmt time': 'timestamp'}
    data.rename(columns=rename_dc, inplace=True)
    data.columns = [c.lower() for c in data.columns]
    return data


def process_data(data, slope=True):
    '''Including calculation of CLUR, Quartiles, and cus trend
    Args:
        data (dataframe): csv data from assets. With column names open, high, low, close.
    Returns:
        dataframe.
    '''
    if slope:
        # process slpoe
        data['diff'] = data['adj close'] - data['open']
        data = data.query('diff != 0').reset_index(drop=True)
        data['direction'] = np.sign(data['diff'])
        data['ushadow_width'] = 0
        data['lshadow_width'] = 0

        for idx in trange(len(data)):
            if data.loc[idx, 'direction'] == 1:
                data.loc[idx, 'ushadow_width'] = data.loc[idx, 'high'] - data.loc[idx, 'adj close']
                data.loc[idx, 'lshadow_width'] = data.loc[idx, 'open'] - data.loc[idx, 'low']
            else:
                data.loc[idx, 'ushadow_width'] = data.loc[idx, 'high'] - data.loc[idx, 'open']
                data.loc[idx, 'lshadow_width'] = data.loc[idx, 'close'] - data.loc[idx, 'low']

            if idx <= 50:
                data.loc[idx, 'body_per'] = stats.percentileofscore(abs(data['diff']), abs(data.loc[idx,'diff']), 'rank')
                data.loc[idx, 'upper_per'] = stats.percentileofscore(data['ushadow_width'], data.loc[idx,'ushadow_width'], 'rank')
                data.loc[idx, 'lower_per'] = stats.percentileofscore(data['lshadow_width'], data.loc[idx,'lshadow_width'], 'rank')
            else:
                data.loc[idx, 'body_per'] = stats.percentileofscore(abs(data.loc[idx-50:idx, 'diff']),abs(data.loc[idx, 'diff']), 'rank')
                data.loc[idx, 'upper_per'] = stats.percentileofscore(data.loc[idx-50:idx, 'ushadow_width'], data.loc[idx, 'ushadow_width'], 'rank')
                data.loc[idx, 'lower_per'] = stats.percentileofscore(data.loc[idx-50:idx, 'lshadow_width'], data.loc[idx, 'lshadow_width'], 'rank')

        data['slope'] = data['adj close'].rolling(7).apply(get_slope, raw=False)
        data.dropna(inplace=True)
    else:
        # process trend
        data['trend'] = data['slope'].rolling(1).apply(get_trend, raw=False)
        data['previous_trend'] = data['trend'].shift(1).fillna(0)
    return data



def detect_evening_star(data, q=None, multi=False, short_per=35, long_per=65):
    '''Detect evening star pattern    
    Args:
        short_per (int): percentile for determination.
        long_per (int): percentile for determination.
    Returns:
        dataframe.
    '''
    print('[ Info ] : detecting evening star')
    temp = data[(data['previous_trend'] == 1) & (data['direction'] == 1)].index
    data['evening'] = 0
    try:
        for idx in tqdm(temp):
            cond1 = (data.loc[idx, 'body_per'] >= long_per)
            cond2 = (data.loc[idx+1, 'body_per'] <= short_per)
            cond3 = (data.loc[idx+2, 'direction'] == -1)
            cond4 = (data.loc[idx+1, 'close'] + data.loc[idx+1, 'open'])/2 >= data.loc[idx, 'close']
            cond5 = data.loc[idx+2, 'close'] <= ((data.loc[idx, 'open'] + data.loc[idx, 'close'])/2)
            # cond6 = (data.loc[idx+2, 'body_per'] >= long_per)
            cond7 = (data.loc[idx+2, 'open'] <= (data.loc[idx+1, 'open'] + data.loc[idx+1, 'close'])/2)
            if cond1 & cond2 & cond3 & cond4 & cond5 & cond7:
                data.loc[idx+2, 'evening'] = 1
    except:
        pass

    if multi:
        q.put({'evening': np.array(data['evening'])})
    else:
        return data


def detect_morning_star(data, q=None, multi=False, short_per=35, long_per=65):
    '''Detect morning star pattern    
    Args:
        short_per (int): percentile for determination.
        long_per (int): percentile for determination.
    Returns:
        dataframe.
    '''
    print('[ Info ] : detecting morning star')
    temp = data[(data['previous_trend'] == -1) & (data['direction'] == -1)].index
    data['morning'] = 0
    try:
        for idx in tqdm(temp):
            cond1 = (data.loc[idx, 'body_per'] >= long_per)
            cond2 = (data.loc[idx+1, 'body_per'] <= short_per)
            cond3 = (data.loc[idx+2, 'direction'] == 1)
            # cond4 = max(data.loc[idx+1, 'close'], data.loc[idx+1, 'open']) <= data.loc[idx, 'close']
            cond4 = (data.loc[idx+1, 'close'] + data.loc[idx+1, 'open'])/2 <= data.loc[idx, 'close']
            cond5 = data.loc[idx+2, 'close'] >= ((data.loc[idx, 'open'] + data.loc[idx, 'close'])/2)
            # cond6 = (data.loc[idx+2, 'body_per'] >= long_per)
            cond7 = (data.loc[idx+2, 'open'] >= (data.loc[idx+1, 'open'] + data.loc[idx+1, 'close'])/2)
            if cond1 & cond2 & cond3 & cond4 & cond5 & cond7:
                data.loc[idx+2, 'morning'] = 1
    except:
        pass

    if multi:
        q.put({'morning': np.array(data['morning'])})
    else:
        return data


def detect_shooting_star(data, q=None, multi=False, short_per=35, long_per=65):
    '''Detect shooting star pattern    
    Args:
        short_per (int): percentile for determination.
        long_per (int): percentile for determination.
    Returns:
        dataframe.
    '''
    print('[ Info ] : detecting shooting star')
    data['shooting_star'] = 0
    temp = data[(data['previous_trend'] == 1) & (data['direction'] == 1)].index
    try:
        for idx in tqdm(temp):
            cond1 = (data.loc[idx, 'body_per'] >= long_per)
            cond2 = (data.loc[idx, 'direction'] == 1)
            cond3 = (data.loc[idx+1, 'ushadow_width'] > 2 * abs(data.loc[idx+1, 'diff']))
            cond4 = (min(data.loc[idx+1, 'open'], data.loc[idx+1, 'close']) > ((data.loc[idx, 'close'] + data.loc[idx, 'open']) / 2))
            cond5 = (data.loc[idx+1, 'lower_per'] <= short_per - 10)  # 25
            cond6 = (data.loc[idx+1, 'upper_per'] >= long_per)
            if cond1 & cond2 & cond3 & cond4 & cond5 & cond6:
                data.loc[idx+1, 'shooting_star'] = 1
    except:
        pass

    if multi:
        q.put({'shooting_star': np.array(data['shooting_star'])})
    else:
        return data


def detect_hanging_man(data, q=None, multi=False, short_per=35, long_per=65):
    '''Detect hanging man pattern    
    Args:
        short_per (int): percentile for determination.
        long_per (int): percentile for determination.
    Returns:
        dataframe.
    '''
    print('[ Info ] : detecting hanging man')
    data['hanging_man'] = 0
    temp = data[(data['previous_trend'] == 1) & (data['direction'] == 1)].index
    try:
        for idx in tqdm(temp):
            cond1 = (data.loc[idx, 'lshadow_width'] > 2 * abs(data.loc[idx, 'diff']))
            cond2 = (data.loc[idx, 'body_per'] <= short_per)
            cond3 = (data.loc[idx, 'upper_per'] <= (short_per - 10))
            cond4 = (data.loc[idx, 'lower_per'] >= long_per)
            if cond1 & cond2 & cond3 & cond4:
                data.loc[idx, 'hanging_man'] = 1
    except:
        pass

    if multi:
        q.put({'hanging_man': np.array(data['hanging_man'])})
    else:
        return data


def detect_bullish_engulfing(data, q=None, multi=False, short_per=35, long_per=65):
    '''Detect bullish engulfing pattern
    Args:
        short_per (int): percentile for determination.
        long_per (int): percentile for determination.
    
    Returns:
        dataframe.
    '''
    print('[ Info ] : detecting bullish engulfing')
    data['bullish_engulfing'] = 0
    temp = data[(data['previous_trend'] == -1) & (data['direction'] == -1)].index
    try:
        for idx in tqdm(temp):
            cond1 = (data.loc[idx, 'direction'] == -1)
            cond2 = (data.loc[idx, 'body_per'] >= long_per)
            cond3 = (data.loc[idx+1, 'direction'] == 1)
            cond4 = (data.loc[idx+1, 'close'] > data.loc[idx, 'open'])
            cond5 = (data.loc[idx+1, 'open'] < data.loc[idx, 'close'])
            if cond1 & cond2 & cond3 & cond4 & cond5:
                data.loc[idx+1, 'bullish_engulfing'] = 1
    except:
        pass

    if multi:
        q.put({'bullish_engulfing': np.array(data['bullish_engulfing'])})
    else:
        return data


def detect_bearish_engulfing(data, q=None, multi=False, short_per=35, long_per=65):
    '''Detect bearish engulfing pattern
    Args:
        short_per (int): percentile for determination.
        long_per (int): percentile for determination.
    Returns:
        dataframe.
    '''
    print('[ Info ] : detecting bearish engulfing')
    data['bearish_engulfing'] = 0
    temp = data[(data['previous_trend'] == 1) & (data['direction'] == 1)].index
    try:
        for idx in tqdm(temp):
            cond1 = (data.loc[idx, 'direction'] == 1)
            cond2 = (data.loc[idx, 'body_per'] >= long_per)
            cond3 = (data.loc[idx+1, 'direction'] == -1)
            cond4 = (data.loc[idx+1, 'close'] < data.loc[idx, 'open'])
            cond5 = (data.loc[idx+1, 'open'] > data.loc[idx, 'close'])
            if cond1 & cond2 & cond3 & cond4 & cond5:
                data.loc[idx+1, 'bearish_engulfing'] = 1
    except:
        pass

    if multi:
        q.put({'bearish_engulfing': np.array(data['bearish_engulfing'])})
    else:
        return data


def detect_hammer(data, q=None, multi=False, short_per=35, long_per=65):
    '''Detect hammer pattern    
    Args:
        short_per (int): percentile for determination.
        long_per (int): percentile for determination.
    Returns:
        dataframe.
    '''
    print('[ Info ] : detecting hammer')
    data['hammer'] = 0
    temp = data[(data['previous_trend'] == -1) & (data['direction'] == -1)].index
    try:
        for idx in tqdm(temp):
            cond1 = (data.loc[idx, 'lshadow_width'] > 2 * abs(data.loc[idx, 'diff']))
            cond2 = (data.loc[idx, 'body_per'] <= short_per)
            cond3 = (data.loc[idx, 'upper_per'] <= (short_per - 15))
            cond4 = (data.loc[idx, 'lower_per'] >= long_per)
            if cond1 & cond2 & cond3 & cond4:
                data.loc[idx, 'hammer'] = 1
    except:
        pass

    if multi:
        q.put({'hammer': np.array(data['hammer'])})
    else:
        return data    


def detect_inverted_hammer(data, q=None, multi=False, short_per=35, long_per=65):
    '''Detect inverted hammer pattern    
    Args:
        short_per (int): percentile for determination.
        long_per (int): percentile for determination.
    Returns:
        dataframe.
    '''
    print('[ Info ] : detecting inverted hammer')
    data['inverted_hammer'] = 0
    temp = data[(data['previous_trend'] == -1) & (data['direction'] == -1)].index
    try:
        for idx in tqdm(temp):
            cond1 = (data.loc[idx, 'direction'] == -1)
            cond2 = (data.loc[idx, 'body_per'] >= long_per)
            cond3 = (data.loc[idx+1, 'ushadow_width'] > 2 * abs(data.loc[idx+1, 'diff']))
            cond4 = (max(data.loc[idx+1, 'open'], data.loc[idx+1, 'close']) < ((data.loc[idx, 'close'] + data.loc[idx, 'open']) / 2))
            cond5 = (data.loc[idx+1, 'lower_per'] <= short_per)
            cond6 = (data.loc[idx+1, 'upper_per'] >= long_per)
            if cond1 & cond2 & cond3 & cond4 & cond5 & cond6:
                data.loc[idx+1, 'inverted_hammer'] = 1
    except:
        pass

    if multi:
        q.put({'inverted_hammer': np.array(data['inverted_hammer'])})
    else:
        return data    


def detect_bullish_harami(data, q=None, multi=False, short_per=35, long_per=65):
    '''Detect inverted bullish harami pattern    
    Args:
        short_per (int): percentile for determination.
        long_per (int): percentile for determination.
        
    Returns:
        dataframe.
    '''
    print('[ Info ] : detecting bullish harami')
    data['bullish_harami'] = 0
    temp = data[(data['previous_trend'] == -1) & (data['direction'] == -1)].index
    try:
        for idx in tqdm(temp):
            cond1 = (data.loc[idx, 'direction'] == -1)
            cond2 = (data.loc[idx, 'body_per'] >= long_per)
            cond3 = (data.loc[idx+1, 'direction'] == 1)
            cond4 = (data.loc[idx+1, 'close'] >= ((data.loc[idx, 'open'] + data.loc[idx, 'close'])/2))
            cond5 = (data.loc[idx+1, 'close'] < data.loc[idx, 'open'])
            cond6 = (data.loc[idx+1, 'open'] > data.loc[idx, 'close'])
            cond7 = (data.loc[idx+1, 'open'] <= ((data.loc[idx, 'open'] + data.loc[idx, 'close'])/2))
            cond8 = (data.loc[idx+1, 'body_per'] >= long_per)
            if cond1 & cond2 & cond3 & cond4 & cond5 & cond6 & cond7 & cond8:
                data.loc[idx+1, 'bullish_harami'] = 1
    except:
        pass

    if multi:
        q.put({'bullish_harami': np.array(data['bullish_harami'])})
    else:
        return data    


def detect_bearish_harami(data, q=None, multi=False, short_per=35, long_per=65):
    '''Detect inverted bearish harami pattern    
    Args:
        short_per (int): percentile for determination.
        long_per (int): percentile for determination.
    Returns:
        dataframe.
    '''
    print('[ Info ] : detecting bearish harami')
    data['bearish_harami'] = 0
    temp = data[(data['previous_trend'] == 1) & (data['direction'] == 1)].index
    try:
        for idx in tqdm(temp):
            cond1 = (data.loc[idx, 'direction'] == 1)
            cond2 = (data.loc[idx, 'body_per'] >= long_per)
            cond3 = (data.loc[idx+1, 'direction'] == -1)
            cond4 = (data.loc[idx+1, 'close'] <= ((data.loc[idx, 'open'] + data.loc[idx, 'close'])/2))
            cond5 = (data.loc[idx+1, 'close'] > data.loc[idx, 'open'])
            cond6 = (data.loc[idx+1, 'open'] < data.loc[idx, 'close'])
            cond7 = (data.loc[idx+1, 'open'] >= ((data.loc[idx, 'open'] + data.loc[idx, 'close'])/2))
            cond8 = (data.loc[idx+1, 'body_per'] >= long_per)
            if cond1 & cond2 & cond3 & cond4 & cond5 & cond6 & cond7 & cond8:
                data.loc[idx+1, 'bearish_harami'] = 1
    except:
        pass

    if multi:
        q.put({'bearish_harami': np.array(data['bearish_harami'])})
    else:
        return data       


def detect_all(data, tasks_ls=None, multi=False, pro_num=2):
    '''
    Args:
        data (dataframe): csv data after `process_data` function.
        multi (bool): use multiprocessing or not.
        pro_num (int): how many processes to be used.
    Returns:
        data (dataframe): dataframe with detections.
    '''
    if multi:
        res_ls = auto_multi(data, tasks_ls, pro_num)
        print('[ Info ] join finished !')

        dc = {}
        for res in res_ls:
            for key, value in res.items():
                dc[key] = value
        df = pd.DataFrame(dc)
        data = pd.concat([data, df], axis=1)
    else:
        data = detect_evening_star(data)
        data = detect_morning_star(data)
        data = detect_shooting_star(data)
        data = detect_hanging_man(data)
        data = detect_bullish_engulfing(data)
        data = detect_bearish_engulfing(data)
        data = detect_hammer(data)
        data = detect_inverted_hammer(data)
        data = detect_bullish_harami(data)
        data = detect_bearish_harami(data)
    return data


def detection_result(data):
    '''Print numbers of detection    
    Args:
        data (dataframe): csv data after `process_data` function.
    Returns:
        data (dataframe): dataframe with detections.
    '''
    print('\n[ Info ] : number of evening star is %s' % np.sum(data['evening']))
    print('[ Info ] : number of morning star is %s' % np.sum(data['morning']))
    print('[ Info ] : number of shooting star is %s' % np.sum(data['shooting_star']))
    print('[ Info ] : number of hanging man is %s' % np.sum(data['hanging_man']))
    print('[ Info ] : number of bullish engulfing is %s' % np.sum(data['bullish_engulfing']))
    print('[ Info ] : number of bearish engulfing is %s' % np.sum(data['bearish_engulfing']))
    print('[ Info ] : number of hammer is %s' % np.sum(data['hammer']))
    print('[ Info ] : number of inverted hammer is %s' % np.sum(data['inverted_hammer']))
    print('[ Info ] : number of bullish harami is %s' % np.sum(data['bullish_harami']))
    print('[ Info ] : number of bearish harami is %s' % np.sum(data['bearish_harami']))
