# version 1.0
# Local operations without network connection

# TODO: query concept comp
# TODO: add columns to database - pct_change, MACD...
# TODO: concept pct_change
# TODO: pct_change current and pct_change period

import os
from datetime import datetime

import pandas as pd
import jaqs.util as jutil
import tdata.util as tutil

from arctic import Arctic

today = jutil.convert_datetime_to_int(datetime.today())
arctic = Arctic('pi3')
BASEDATA_LIB = arctic['basedata']
DAILY_LIB = arctic['daily']
MINUTE_LIB = arctic['minute']
ZEN_LIB = arctic['zen']


def daily_last_date(symbol: str = '000001.SH') -> int:
    return DAILY_LIB.read(symbol).metadata['last_date']


def daily(symbol: str = '000001.SH',
          start_date: int = jutil.shift(today, n_weeks=-156),
          end_date: int = today) -> pd.DataFrame:
    day_bar = DAILY_LIB.read(symbol).data.loc[start_date:end_date]
    day_bar.index = jutil.convert_int_to_datetime(day_bar.index)
    return day_bar.loc[:, [
        'close', 'high', 'low', 'open', 'symbol', 'trade_status', 'turnover',
        'volume'
    ]]


def weekly(symbol: str = '000001.SH',
           start_date: int = jutil.shift(today, n_weeks=-626),
           end_date: int = today) -> pd.DataFrame:
    day_bar = daily(symbol, start_date, end_date)
    return tutil.resample_bar('W', bar=day_bar)


def monthly(symbol: str = '000001.SH',
            start_date: int = 19901219,
            end_date: int = today) -> pd.DataFrame:
    day_bar = daily(symbol, start_date, end_date)
    return tutil.resample_bar('M', bar=day_bar)


# TODO: fix problem with more than 120T
# TODO: test with other source
def minute(symbol: str = '000001.SH',
           start_date: int = jutil.shift(today, n_weeks=-4),
           end_date: int = today,
           freq: int = 1) -> pd.DataFrame:
    bar = MINUTE_LIB.read(symbol).data.loc[start_date:end_date]
    bar = tutil.combine_date_time_column(bar).set_index('datetime')
    freq = str(freq) + 'T'
    return tutil.resample_bar(freq, bar)

def brush(symbol: str = '000001.SH') -> pd.DataFrame:
    return ZEN_LIB.read(symbol).data


if __name__ == '__main__':
    import fire
    fire.Fire()
