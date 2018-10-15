from datetime import datetime

import numpy as np
import pandas as pd
import talib

from tdata.dbreader import get_price


class Features:
    """
    Useful information for certain instrument.
    """

    def __init__(self,
                 sid='000001.XSHG',
                 start_date='2018-01-01',
                 end_date=datetime.today(),
                 frequency='1d'):
        self.sid = sid
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency

    def bar(self):
        return get_price(self.sid, self.start_date, self.end_date,
                         self.frequency)

    def data(self):

        df = self.bar().copy()

        # add log return
        df['return'] = np.log(df['close'] / df['close'].shift(1))

        # add macd related
        macd, macdsignal, macdhist = talib.MACD(df.close.values)
        df['macd'] = macd
        df['macdsignal'] = macdsignal
        df['macdhist'] = macdhist
        df.dropna(inplace=True)
        df['macdgrps'] = self.sign_grp(df.macd > 0)
        df['macdhistgrps'] = self.sign_grp(df.macdhist > 0)

        # add brush and center data
        df = df.merge(
            self.brush(df), how='outer', left_index=True, right_index=True)
        df = df.merge(
            self.center(df), how='outer', left_index=True, right_index=True)

        # should after adding brush and center, or before adding macd
        # for dropna reason
        df['EMA120'] = talib.EMA(df.close.values, timeperiod=120)
        df['EMA250'] = talib.EMA(df.close.values, timeperiod=250)

        return df

    @staticmethod
    def sign_grp(se):
        grps = [0]
        for i in range(1, len(se)):
            if se.iloc[i] == se.iloc[i - 1]:
                grps.append(grps[i - 1])
            else:
                grps.append(grps[i - 1] + 1)

        return grps

    @staticmethod
    def brush(df):
        """
        Input: Need add macd column first
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

    @staticmethod
    def center(df):
        """
        Input: Need add brush column first
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
                outlier_index = vecs.transform(center_map).query(
                    is_out).index[:2]
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
