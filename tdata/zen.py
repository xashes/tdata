import numpy as np
import pandas as pd
import talib


def contain(k0, k1, k2):
    """
    ki: (di, gi)
    if up: return (maxdi, maxgi)
    if down: return (mindi, mingi)
    """
    if is_contain(k1, k2):
        if is_up(k0, k1):
            return (max(k1[0], k2[0]), max(k1[1], k2[1]))
        else:
            return (min(k1[0], k2[0]), min(k1[1], k2[1]))


def is_contain(k1, k2):
    return (k1[0] <= k2[0] and k1[1] >= k2[1]) or (k2[0] <= k1[0]
                                                   and k2[1] >= k1[1])


def is_up(k0, k1):
    return k0[1] <= k1[1]


def inter(brush_df):
    brushes = brush_df.loc[:, ['startpoint', 'endpoint']].values
    brushes = [(min(x), max(x)) for x in brushes]
    share = max(list(zip(*brushes))[0]), min(list(zip(*brushes))[1])
    if share[0] <= share[1]:
        return share


def combine(df):
    df = df.copy()
    seq = df.loc[:, ['low', 'high']]
    i = 2
    for i in range(seq.shape[0]):
        if is_contain(seq.iloc[i - 1], seq.iloc[i]):
            seq.iloc[i] = contain(seq.iloc[i - 2], seq.iloc[i - 1],
                                  seq.iloc[i])
            seq.iloc[i - 1] = np.nan
    df.loc[:, ['low', 'high']] = seq
    df = df.dropna()
    return df


def preparting(df, limit=4):
    '''
    Recieve combined df as parameter.
    '''
    df = df.copy()
    df['parting'] = np.nan
    df['count'] = list(range(df.shape[0]))
    seq = df.loc[:, ['low', 'high', 'parting', 'count']]
    for i in range(2, df.shape[0]):
        if seq.iloc[i - 2:i + 1, 1].max() == seq.iat[i - 1, 1]:
            seq.iat[i - 1, 2] = 1.0
        if seq.iloc[i - 2:i + 1, 0].min() == seq.iat[i - 1, 0]:
            seq.iat[i - 1, 2] = -1.0
    seq.iat[-1, 2] = 0
    seq = seq.dropna()

    end = len(seq) - 1
    i = 1
    while i < end:
        if seq.iat[i, -1] - seq.iat[i - 1, -1] < limit:
            seq = seq.drop(seq.index[i])
            end -= 1
        else:
            i += 1

    df.loc[:, 'parting'] = seq.loc[:, 'parting']
    df = df.dropna()
    df['highpoint'] = df[df['parting'] > 0]['high']
    df['lowpoint'] = df[df['parting'] < 0]['low']
    df = df.fillna(0)
    df['endpoint'] = df['highpoint'] + df['lowpoint']
    df.loc[df.index[-1], 'endpoint'] = df.loc[df.index[-1], 'close']
    df['startpoint'] = df['endpoint'].shift(1)
    df = df.dropna()
    return df


def parting(df, limit=4):
    '''
    Recieve combined df as parameter.
    '''
    df = df.copy()
    df['parting'] = np.nan
    df['count'] = list(range(df.shape[0]))
    seq = df.loc[:, ['low', 'high', 'parting', 'count']]
    for i in range(2, df.shape[0]):
        if seq.iloc[i - 2:i + 1, 1].max() == seq.iat[i - 1, 1]:
            seq.iat[i - 1, 2] = 1.0
        if seq.iloc[i - 2:i + 1, 0].min() == seq.iat[i - 1, 0]:
            seq.iat[i - 1, 2] = -1.0
    seq.iat[-1, 2] = 0
    seq = seq.dropna()

    end = len(seq) - 1
    i = 1
    while i < end:
        if (seq.iat[i, 2] != seq.iat[i - 1, 2]) and (
                seq.iat[i, -1] - seq.iat[i - 1, -1] < limit):
            seq = seq.drop(seq.index[i])
            end -= 1
        elif seq.iat[i, 2] == seq.iat[i - 1, 2]:
            if seq.iat[i, 2] < 0:
                if seq.iat[i, 0] <= seq.iat[i - 1, 0]:
                    seq = seq.drop(seq.index[i - 1])
                else:
                    seq = seq.drop(seq.index[i])
            else:
                if seq.iat[i, 1] >= seq.iat[i - 1, 1]:
                    seq = seq.drop(seq.index[i - 1])
                else:
                    seq = seq.drop(seq.index[i])
            end -= 1
        else:
            i += 1

    df.loc[:, 'parting'] = seq.loc[:, 'parting']
    df = df.dropna()
    df['highpoint'] = df[df['parting'] > 0]['high']
    df['lowpoint'] = df[df['parting'] < 0]['low']
    df = df.fillna(0)
    df['endpoint'] = df['highpoint'] + df['lowpoint']
    df.loc[df.index[-1], 'endpoint'] = df.loc[df.index[-1], 'close']
    df['startpoint'] = df['endpoint'].shift(1)
    df = df.dropna()
    return df


def add_macd(df):
    df = df.copy()
    macd, signal, hist = talib.MACD(df['close'].values)
    df.loc[:, 'hist'] = hist
    df = df.dropna()
    return df


def merge_parting(df):
    pt = parting(combine(df)).loc[:, ['parting', 'count', 'endpoint']]
    df = add_macd(df)
    # df.loc[:, 'pct'] = df['close'].pct_change()
    # df.loc[:, 'turnover'] = df.where(df['pct'] > 0, -df['turnover'], axis=0)
    df = pd.merge(df, pt, left_index=True, right_index=True, how='left')
    df = df.fillna(method='bfill')
    return df


def hist_sum(df):
    pt = parting(combine(df))
    merged = merge_parting(df)
    pt['hist_sum'] = [0] * (
        len(pt) - len(list(merged.groupby('count')))) + list(
            merged.groupby('count')['hist'].sum() * 2)
    pt['brush_amount'] = [0] * (
        len(pt) - len(list(merged.groupby('count')))) + list(
            merged.groupby('count')['turnover'].sum())
    pt['pct_change'] = pt['endpoint'] / pt['startpoint'] - 1
    return pt