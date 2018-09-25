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
from arctic.date import DateRange

today = jutil.convert_datetime_to_int(datetime.today())
arctic = Arctic('localhost')
BASEDATA_LIB = arctic['basedata']
DAILY_LIB = arctic['day']
MINUTE_LIB = arctic['minute1']
SYMBOLS = DAILY_LIB.list_symbols()


def add_suffix_for_symbol(symbol):
    if len(symbol) == 6:
        if symbol.startswith('6'):
            symbol = symbol + '.XSHG'
        else:
            symbol = symbol + '.XSHE'
    return symbol


def daily_last_date(symbol: str = '000001.SH') -> int:
    return DAILY_LIB.read(symbol).metadata['last_date']


def minute_last_date(symbol: str = '000001.SH') -> int:
    return MINUTE_LIB.read(symbol).metadata['last_date']


def daily(symbol: str = '000001',
          start_date: int = jutil.shift(today, n_weeks=-208),
          end_date: int = today) -> pd.DataFrame:
    symbol = add_suffix_for_symbol(symbol)
    day_bar = DAILY_LIB.read(
        symbol, date_range=DateRange(
            start_date,
            end_date)).data.rename(columns={'total_turnover': 'turnover'})
    day_bar.index = jutil.convert_int_to_datetime(day_bar.index)
    return day_bar.loc[:,
                       ['close', 'high', 'low', 'open', 'turnover', 'volume']]


def weekly(symbol: str = '000001.SH',
           start_date: int = jutil.shift(today, n_weeks=-826),
           end_date: int = today) -> pd.DataFrame:
    symbol = add_suffix_for_symbol(symbol)
    day_bar = daily(symbol, start_date, end_date)
    return tutil.resample_bar('W', bar=day_bar)


def monthly(symbol: str = '000001.SH',
            start_date: int = 19901219,
            end_date: int = today) -> pd.DataFrame:
    symbol = add_suffix_for_symbol(symbol)
    day_bar = daily(symbol, start_date, end_date)
    return tutil.resample_bar('M', bar=day_bar)


# TODO: fix problem with more than 120T
# TODO: test with other source
def minute(symbol: str = '000001.SH',
           start_date: int = jutil.shift(today, n_weeks=-4),
           end_date: int = today,
           freq: int = 1) -> pd.DataFrame:
    # TODO fix resample period bigger than 30

    symbol = add_suffix_for_symbol(symbol)
    bar = MINUTE_LIB.read(symbol).data.loc[start_date:end_date]
    bar = tutil.combine_date_time_column(bar).set_index('datetime')
    freq = str(freq) + 'T'
    return tutil.resample_bar(freq, bar)


def bar(symbol: str = '000001.XSHG',
        period: int = 1000,
        end_date: int = today,
        freq='D'):
    symbol = add_suffix_for_symbol(symbol)
    if freq == 'D':
        start_date = jutil.shift(end_date, n_weeks=-int(period / 5))
        return daily(symbol, start_date, end_date)
    elif freq == 'W':
        start_date = jutil.shift(end_date, n_weeks=-period)
        return weekly(symbol, start_date, end_date)
    elif freq == 'M':
        start_date = 19901219
        return monthly(symbol, start_date, end_date)
    else:
        try:
            freq = int(freq)
            weeks_delta = int(period * freq / (240 * 5) + 1)
            start_date = jutil.shift(today, n_weeks=-weeks_delta)
            return minute(symbol, start_date, end_date, freq=freq)
        except Exception as e:
            print(f'Error on {symbol} with frequency: {str(freq)}')
            print(f'Error message: {str(e)}')


if __name__ == '__main__':
    import fire
    fire.Fire()
