# version 1.0
# Local operations without network connection

# TODO: query concept comp
# TODO: add columns to database - pct_change, MACD...
# TODO: concept pct_change
# TODO: pct_change current and pct_change period

import os
from datetime import datetime

import fire
import pandas as pd
from sqlalchemy import create_engine
import jaqs.util as jutil
import tdata.util as tutil

from tdata.consts import HISTORY_DIR, HISTORY_DB, DAILY_TABLE, MINUTE_TABLE, INDEX_TABLE, STOCK_TABLE, SH_INDEX

db_path = os.path.join(HISTORY_DIR, HISTORY_DB)
engine = create_engine('sqlite:///{}'.format(db_path))

today = jutil.convert_datetime_to_int(datetime.today())


def query_index_table():
    return pd.read_sql_table(INDEX_TABLE, engine)


def query_stock_table():
    return pd.read_sql_table(STOCK_TABLE, engine)


def query_index_symbols() -> pd.Series:
    return query_index_table().loc[:, 'symbol']


def query_stock_symbols() -> pd.Series:
    return query_stock_table().loc[:, 'symbol']


def query_all_symbols() -> str:
    indexes = ','.join(query_index_symbols())
    stocks = ','.join(query_stock_symbols())
    return indexes + stocks


def query_all_symbol_names() -> pd.Series:
    indexes = query_index_table()
    indexes = indexes.symbol + ' ' + indexes.name
    stocks = query_stock_table()
    stocks = stocks.symbol + ' ' + stocks.name
    return indexes.append(stocks, ignore_index=True)


def daily_last_date(symbol=SH_INDEX) -> int:
    last_date = daily(symbol).index[-1]
    return jutil.convert_datetime_to_int(last_date)


def daily_first_date(symbol=SH_INDEX) -> int:
    first_date = daily(symbol, start_date=19901219).index[0]
    return jutil.convert_datetime_to_int(first_date)


def daily(symbol: str = SH_INDEX,
          start_date=jutil.shift(today, n_weeks=-156),
          end_date=today,
          fields='*') -> pd.DataFrame:
    props = {
        'table': DAILY_TABLE,
        'symbol': symbol,
        'start_date': start_date,
        'end_date': end_date,
        'fields': fields
    }
    return pd.read_sql_query(
        "SELECT {fields} FROM {table} WHERE symbol = '{symbol}' AND trade_date >= {start_date} AND trade_date <= {end_date} ORDER BY trade_date;".
        format(**props),
        engine,
        parse_dates={
            'trade_date': {
                'format': '%Y%m%d'
            }
        }).set_index('trade_date').drop(columns=['index'])


def weekly(symbol: str = SH_INDEX,
           start_date: int = jutil.shift(today, n_weeks=-626),
           end_date: int = today,
           fields: str = '*') -> pd.DataFrame:
    day_bar = daily(symbol, start_date, end_date, fields)
    return tutil.resample_bar('W', bar=day_bar)


def monthly(symbol: str = SH_INDEX,
            start_date: int = 19901219,
            end_date: int = today,
            fields: str = '*') -> pd.DataFrame:
    day_bar = daily(symbol, start_date, end_date, fields)
    return tutil.resample_bar('M', bar=day_bar)


def bar(symbol: str = SH_INDEX,
        start_date: int = jutil.shift(today, n_weeks=-4),
        end_date: int = today,
        freq: int = 1,
        fields: str = '*') -> pd.DataFrame:
    props = dict(
        table=MINUTE_TABLE,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        fields=fields)
    bar = pd.read_sql_query(
        "SELECT {fields} FROM {table} WHERE symbol = '{symbol}' AND trade_date >= {start_date} AND trade_date <= {end_date} ORDER BY trade_date, time;".
        format(**props), engine)
    bar = tutil.combine_date_time_column(bar).set_index('datetime').drop(
        columns=['index'])
    freq = str(freq) + 'T'
    return tutil.resample_bar(freq, bar)


def bar_last_date() -> int:
    last_date = bar().index[-1]
    return jutil.convert_datetime_to_int(last_date)


def bar_first_date() -> int:
    first_date = bar(start_date=20120101).index[0]
    return jutil.convert_datetime_to_int(first_date)


if __name__ == '__main__':
    fire.Fire()
