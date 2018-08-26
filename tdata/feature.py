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
    df['macdgrps'] = sign_grp(df.macd > 0)
    df = df.dropna()

    # add brush data
    df = df.merge(brush(df), how='outer', left_index=True, right_index=True)

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
    """
    Need add columns first
    """
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
        data={'brushend': macdgrps.apply(brush_point).values},
        index=macdgrps.apply(brush_index))


def center(df):
    """
    input: df with columns added by add_columns()
    """
    brush = df[df.brushend.notna()].copy()
    brush['brushstart'] = brush.brushend.shift(1)
    brush = brush.dropna()

    # generate feature vector
    vecs = brush[['brushstart', 'brushend']].copy().apply(
        sorted, axis=1,
        result_type='broadcast').rename(columns={
            'brushstart': 'bottom',
            'brushend': 'top'
        })

    center = pd.DataFrame(columns='bottom top'.split())

    while len(vecs) > 3:
        center_map = {'bottom': 'cummax', 'top': 'cummin'}
        is_intersect = 'bottom < top'
        is_out = 'bottom > top'

        candi = vecs.transform(center_map).query(is_intersect)

        while len(vecs) - len(candi) > 2:
            outlier_index = vecs.transform(center_map).query(is_out).index[:2]
            candi2 = vecs.drop(outlier_index).transform(center_map).query(
                is_intersect)
            if len(candi) == len(candi2):
                if len(candi) > 3:
                    center = center.append(candi.iloc[-1])
                vecs = vecs.drop(candi.index)
                break
            else:
                vecs = vecs.drop(outlier_index)
                candi = candi2.copy()
                if len(vecs) - len(candi) < 3:
                    center = center.append(candi.iloc[-1])
                    vecs = vecs.drop(candi.index)

    return center
