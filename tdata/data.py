# version 2.0
# DONE: append update
# TODO: write index_df and stock_df into database, use 'replace' method
# TODO: adjust mode = post history

import os
from datetime import datetime, timedelta

import jaqs.util as jutil
import pandas as pd
from sqlalchemy import create_engine

from tdata.config_path import HISTORY_PATH
from tdata.quantos import ds

history_db = 'history.db'
db_path = os.path.join(HISTORY_PATH, history_db)
engine = create_engine('sqlite:///{}'.format(db_path))
daily_table = 'daily'


def newest_trade_date():
    today = jutil.dtutil.convert_datetime_to_int(datetime.today())
    if ds.is_trade_date(today):
        return today
    else:
        return ds.query_last_trade_date(today)


# get index and stocks list
index_df, msg = ds.query(
    view='jz.instrumentInfo',
    fields='status,list_date,name,market',
    filter='inst_type=100&status=1&symbol=',
    data_format='pandas')
stock_df, msg = ds.query(
    view='jz.instrumentInfo',
    fields='status,list_date,name,market',
    filter='inst_type=1&status=1&symbol=',
    data_format='pandas')

indexes = ','.join(index_df.loc[:, 'symbol'])
stocks = ','.join(stock_df.loc[:, 'symbol'])
symbols = indexes + stocks
today = newest_trade_date()


def local_daily(symbol, start_date=19900101, end_date=today, fields='*'):
    props = {
        'table': daily_table,
        'symbol': symbol,
        'start_date': start_date,
        'end_date': end_date,
        'fields': fields
    }
    return pd.read_sql_query(
        "SELECT {fields} FROM {table} WHERE symbol = '{symbol}' AND trade_date >= {start_date} AND trade_date <= {end_date};".
        format(**props), engine)


def local_last_date(symbol='000001.SH'):
    local_data = local_daily(symbol)
    last_date = local_data['trade_date'].iloc[-1]
    return last_date


def next_trade_date():
    return ds.query_next_trade_date(local_last_date())


def test_new_data():
    symbol = '000001.SH'
    props = {
        'symbol': symbol,
        'start_date': local_last_date(),
        'end_date': today,
        'fields': 'symbol,trade_date'
    }
    local_data = local_daily(**props)
    remote_data = ds.daily(**props)
    print('\nLocal Data:\n{}'.format(local_data))
    print('\nRemote Data:\n{}'.format(remote_data))


remote_fields = 'symbol,freq,close,high,low,open,trade_date,trade_status,turnover,volume'


def update_daily_table():
    props = {
        'symbol': symbols,
        'start_date': next_trade_date(),
        'end_date': today,
        'fields': remote_fields
    }
    print('Downloading daily data from {} to {}.'.format(next_trade_date(), today))
    df, msg = ds.daily(**props)
    print('Writing to the Database.')
    df.to_sql(daily_table, engine, if_exists='append', chunksize=1000)


def init_daily_table():
    if not engine.dialect.has_table(engine, daily_table):
        start_date = 19901219
    else:
        start_date = next_trade_date()

    while start_date < 20180101:  # TODO: while not sync as remote data
        delta = timedelta(days=100)
        end_date = jutil.convert_datetime_to_int(
            jutil.convert_int_to_datetime(start_date) + delta)
        props = {
            'symbol': symbols,
            'start_date': start_date,
            'end_date': end_date,
            'fields': remote_fields
        }
        print('Downloading daily data from {} to {}.'.format(
            start_date, end_date))
        df, msg = ds.daily(**props)
        print('Writing to the Database.')
        df.to_sql(daily_table, engine, if_exists='append', chunksize=100000)
        start_date = next_trade_date()
