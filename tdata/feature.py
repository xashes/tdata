import numpy as np
import pandas as pd
import talib


def add_columns(df):

    # deal with 停牌 data
    try:
        df = df.drop(
            df[(df.trade_status == '停牌') | (df.close == 0) | (df.open == 0)
               | (df.low == 0) | (df.high == 0)].index)
    except:
        pass

    # add log return and moving vol
    # df['return'] = np.log(df['close'] / df['close'].shift(1))

    # add macd related
    macd, macdsignal, macdhist = talib.MACD(df.close.values)
    df['macd'] = macd
    df['macdsignal'] = macdsignal
    df['macdhist'] = macdhist
    df.dropna(inplace=True)
    df['macdgrps'] = sign_grp(df.macd > 0)
    df['macdhistgrps'] = sign_grp(df.macdhist > 0)

    # add brush data
    df = df.merge(brush(df), how='outer', left_index=True, right_index=True)
    df = df.merge(center(df), how='outer', left_index=True, right_index=True)

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
    # TODO center bounds and center combining

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
        is_intersect = 'bottom <= top'
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
        else:
            if len(candi) > 3:
                center = center.append(candi.iloc[-1])
            vecs = vecs.drop(candi.index)

    return center


def last_center(df):
    """
    input: df with columns added
    """
    centers = df[df.top.notna()]
    last_center = df.loc[df.macdgrps >= centers.iloc[-2].macdgrps]

    data = dict()

    data['symbol'] = last_center.symbol.values[0]
    data['brush'] = last_center.brushend.dropna().values

    data['bottom'] = last_center.bottom.dropna().values[-1]

    data['top'] = last_center.top.dropna().values[-1]

    # TODO: whether use brush instead of macd to group
    data['macdgrps'] = last_center.loc[:, ['macd', 'macdhist', 'macdhistgrps'
                                           ]].groupby(last_center.macdgrps)
    data['lastgrp'] = data['macdgrps'].get_group(
        last_center.macdgrps.iloc[-1]).groupby('macdhistgrps')

    data['macd_length'] = data['macdgrps']['macd'].agg(
        lambda x: max(x, key=abs)).values
    data['hist_length'] = data['macdgrps']['macdhist'].agg(
        lambda x: max(x, key=abs)).values

    return pd.DataFrame.from_dict(data, orient='index').T
