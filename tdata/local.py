# version 1.0
# Local operations without network connection

# TODO: query concept comp
# TODO: add columns to database - pct_change, MACD...
# TODO: concept pct_change
# TODO: pct_change current and pct_change period
# TODO: week bar

import os
from datetime import datetime

import fire
import pandas as pd
from sqlalchemy import create_engine
import jaqs.util as jutil

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


def daily_last_date(symbol=SH_INDEX) -> int:
    local_data = daily(symbol)
    last_date = local_data['trade_date'].iloc[-1]
    return last_date


def daily_first_date(symbol=SH_INDEX) -> int:
    local_data = daily(symbol)
    return local_data['trade_date'].iloc[0]


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
        })


def weekly(symbol: str = SH_INDEX,
           start_date: int = jutil.shift(today, n_weeks=-626),
           end_date: int = today,
           fields: str = '*') -> pd.DataFrame:
    day_bar = daily(symbol, start_date, end_date, fields)
    resampled = day_bar.resample('W', on='trade_date')
    weekly = pd.DataFrame({
        'close': resampled['close'].last(),
        'high': resampled['high'].max(),
        'low': resampled['low'].min(),
        'open': resampled['open'].first(),
        'symbol': symbol,
        'turnover': resampled['turnover'].sum(),
        'volume': resampled['volume'].sum()
    })
    return weekly


def bar(symbol: str = SH_INDEX,
        start_date: int = jutil.shift(today, n_weeks=-4),
        end_date: int = today,
        fields: str = '*') -> pd.DataFrame:
    props = dict(
        table=MINUTE_TABLE,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        fields=fields)
    return pd.read_sql_query(
        "SELECT {fields} FROM {table} WHERE symbol = '{symbol}' AND trade_date >= {start_date} AND trade_date <= {end_date} ORDER BY trade_date, time;".
        format(**props), engine)


def bar_last_date() -> int:
    return bar()['trade_date'].iloc[-1]


def bar_first_date() -> int:
    return bar(start_date=20120101)['trade_date'].iloc[0]


if __name__ == '__main__':
    fire.Fire()
