import numpy as np
import pandas as pd
import talib


def add_columns(df):
    # add log return and moving vol
    df['return'] = np.log(df['close'] / df['close'].shift(1))

    # add macd related
    macd, macdsignal, macdhist = talib.MACD(df.close.values)
    df['macd'] = macd
    df['macdsignal'] = macdsignal
    df['macdhist'] = macdhist
    df = df.dropna()

    return df


def sign_grp(se):
    grps = [0]
    for i in range(1, len(se)):
        if se[i] == se[i - 1]:
            grps.append(grps[i - 1])
        else:
            grps.append(grps[i - 1] + 1)

    return grps


def brush(df):
    df = add_columns(df).copy()
    df['macdgrps'] = sign_grp(df.macd > 0)
    macdgrps = df.groupby('macdgrps')

    def brush_index(grp):
        if grp['macd'].sum() > 0:
            return grp['high'].idxmax()
        else:
            return grp['low'].idxmin()

    def brush_point(grp):
        if grp['macd'].sum() > 0:
            return grp['high'].max()
        else:
            return grp['low'].min()

    return pd.DataFrame(
        data=macdgrps.apply(brush_point).values,
        index=macdgrps.apply(brush_index),
        columns=['endpoint'])
