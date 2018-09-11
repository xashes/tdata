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
    df['return'] = np.log(df['close'] / df['close'].shift(1))

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

    brush = df[df.brushend.notnull()].copy()
    brush['brushstart'] = brush.brushend.shift(1)
    brush = brush.dropna()

    # generate feature vector
    if pd.__version__ < str(0.23):
        vecs = brush[['brushstart', 'brushend']].copy().apply(
            sorted, axis=1, broadcast=True).rename(columns={
                'brushstart': 'bottom',
                'brushend': 'top'
            })
    else:
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


def last_center(df, target=None):
    """
    input: df without columns added
           target -> number or None
    """
    data = dict()

    if target:
        target_df = df.iloc[-target:]
        df = df.drop(df.index[-target:])
        data['max_return'] = target_df.high.max() / df.close[-1] - 1
        data['max_loss'] = target_df.low.min() / df.close[-1] - 1
        data['return'] = target_df.close[-1] / df.close[-1] - 1

    df = add_columns(df)
    centers = df[df.top.notnull()]
    last_center = df.loc[df.macdgrps >= centers.iloc[-2].macdgrps]

    data['symbol'] = last_center.symbol.values[0]
    brushends = last_center.brushend.dropna().values

    # center
    data['bottom'] = last_center.bottom.dropna().values[-1]
    data['top'] = last_center.top.dropna().values[-1]

    # brush
    data['brush_count'] = len(brushends) - 1
    data['brush_1'] = brushends[-1]
    data['brush_2'] = brushends[-2]
    data['brush_3'] = brushends[-3]
    data['brush_4'] = brushends[-4]

    # macd
    data['macd_mean'] = last_center.macd.mean()
    data['hist_mean'] = last_center.macdhist.mean()

    # TODO: whether use brush instead of macd to group
    macdgrps = last_center.loc[:, ['macd', 'macdhist', 'macdhistgrps'
                                   ]].groupby(last_center.macdgrps)
    lastgrp = macdgrps.get_group(last_center.macdgrps.iloc[-1])
    tail_histgrps = lastgrp.groupby('macdhistgrps')

    # macd groups
    macd_length = macdgrps['macd'].agg(lambda x: max(x, key=abs)).values
    data['macd_max'] = macd_length.max()
    data['macd_min'] = macd_length.min()
    data['macd_1'] = macd_length[-1]
    data['macd_2'] = macd_length[-2]
    data['macd_3'] = macd_length[-3]
    data['macd_4'] = macd_length[-4]

    hist_length = macdgrps['macdhist'].agg(lambda x: max(x, key=abs)).values
    data['hist_max'] = hist_length.max()
    data['hist_min'] = hist_length.min()
    data['hist_1'] = hist_length[-1]
    data['hist_2'] = hist_length[-2]
    data['hist_3'] = hist_length[-3]
    data['hist_4'] = hist_length[-4]

    # last group
    # TODO: data['tail_macd_length'] - how to compute this depend on hist sign
    data['lastgrp_kcount'] = len(lastgrp)
    tail_hist_length = tail_histgrps['macdhist'].agg(
        lambda x: max(x, key=abs)).values
    data['tail_hist_max'] = tail_hist_length.max()
    data['tail_hist_min'] = tail_hist_length.min()
    data['tail_hist_1'] = tail_hist_length[-1]

    return pd.DataFrame.from_dict(data, orient='index').T.set_index('symbol')
