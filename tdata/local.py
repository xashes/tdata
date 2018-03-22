# version 1.0
# Local operations without network connection

import os
from datetime import datetime, timedelta

import fire
import pandas as pd
from sqlalchemy import create_engine
import jaqs.util as jutil

from tdata.config_path import HISTORY_DIR, HISTORY_DB, DAILY_TABLE, INDEX_TABLE, STOCK_TABLE

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


def db_last_date(symbol='000001.SH'):
    local_data = daily(symbol)
    last_date = local_data['trade_date'].iloc[-1]
    return last_date


def db_first_date(symbol='000001.SH'):
    local_data = daily(symbol)
    first_date = local_data['trade_date'].iloc[0]
    return first_date


def daily(symbol: str,
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
        "SELECT {fields} FROM {table} WHERE symbol = '{symbol}' AND trade_date >= {start_date} AND trade_date <= {end_date};".
        format(**props), engine)


if __name__ == '__main__':
    fire.Fire()
