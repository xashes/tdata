# version 2.0
# DONE: append update
# DONE: write index_df and stock_df into database, use 'replace' method
# TODO: adjust mode = post history
# TODO: move local operation to another module to improve performance
# TODO: set primary key for daily table

import os
from datetime import datetime, timedelta

import fire
import jaqs.util as jutil
import pandas as pd
from sqlalchemy import create_engine

from tdata.config_path import HISTORY_DIR, HISTORY_DB, DAILY_TABLE, INDEX_TABLE, STOCK_TABLE
from tdata.quantos import ds

db_path = os.path.join(HISTORY_DIR, HISTORY_DB)
engine = create_engine('sqlite:///{}'.format(db_path))


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


def update_index_table():
    index_df, msg = ds.query(
        view='jz.instrumentInfo',
        fields='status,list_date,name,market',
        filter='inst_type=100&status=1&symbol=',
        data_format='pandas')
    index_df.to_sql(INDEX_TABLE, engine, if_exists='replace')


def update_stock_table():
    stock_df, msg = ds.query(
        view='jz.instrumentInfo',
        fields='status,list_date,name,market',
        filter='inst_type=1&status=1&symbol=',
        data_format='pandas')
    stock_df.to_sql(STOCK_TABLE, engine, if_exists='replace')


def local_daily(symbol, start_date=19900101, end_date=today, fields='*'):
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


def local_last_date(symbol='000001.SH'):
    local_data = local_daily(symbol)
    last_date = local_data['trade_date'].iloc[-1]
    return last_date


def local_first_date(symbol='000001.SH'):
    local_data = local_daily(symbol)
    first_date = local_data['trade_date'].iloc[0]
    return first_date


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
    print('Downloading daily data from {} to {}.'.format(
        next_trade_date(), today))
    df, msg = ds.daily(**props)
    print('Writing to the Database.')
    df.to_sql(DAILY_TABLE, engine, if_exists='append', chunksize=1000)


def init_daily_table():
    if not engine.dialect.has_table(engine, DAILY_TABLE):
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
        df.to_sql(DAILY_TABLE, engine, if_exists='append', chunksize=100000)
        start_date = next_trade_date()
    print('Database initialization complete.')


if __name__ == '__main__':
    fire.Fire()
