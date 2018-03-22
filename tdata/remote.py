# version 2.0
# DONE: append update
# DONE: write index_df and stock_df into database, use 'replace' method
# TODO: adjust mode = post history
# TODO: move local operation to another module to improve performance
# TODO: set primary key for daily table
# TODO: improve query performance, write one symbol per table?

import os
from datetime import datetime

import fire
import jaqs.util as jutil
from sqlalchemy import create_engine

import tdata.local as local
from tdata.config_path import (DAILY_TABLE, HISTORY_DB, HISTORY_DIR,
                               INDEX_TABLE, STOCK_TABLE)
from tdata.quantos import ds

db_path = os.path.join(HISTORY_DIR, HISTORY_DB)
engine = create_engine('sqlite:///{}'.format(db_path))

indexes = ','.join(local.query_index_symbols())
stocks = ','.join(local.query_stock_symbols())
symbols = indexes + stocks


def newest_trade_date():
    today = jutil.dtutil.convert_datetime_to_int(datetime.today())
    if ds.is_trade_date(today):
        return today
    else:
        return ds.query_last_trade_date(today)


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


def db_next_date():
    return ds.query_next_trade_date(local.db_last_date())


def test_new_data():
    symbol = '000001.SH'
    props = {
        'symbol': symbol,
        'start_date': local.db_last_date(),
        'end_date': today,
        'fields': 'symbol,trade_date'
    }
    local_data = local.daily(**props)
    remote_data = ds.daily(**props)
    print('\nLocal Data:\n{}'.format(local_data))
    print('\nRemote Data:\n{}'.format(remote_data))


remote_fields = 'symbol,freq,close,high,low,open,trade_date,trade_status,turnover,volume'


def update_daily_table():
    props = {
        'symbol': symbols,
        'start_date': db_next_date(),
        'end_date': today,
        'fields': remote_fields
    }
    print('Downloading daily data from {} to {}.'.format(
        db_next_date(), today))
    df, msg = ds.daily(**props)
    print('Writing to the Database.')
    df.to_sql(DAILY_TABLE, engine, if_exists='append', chunksize=1000)


def update_database():
    update_index_table()
    update_stock_table()
    update_daily_table()


def init_daily_table():
    if not engine.dialect.has_table(engine, DAILY_TABLE):
        start_date = 19901219
    else:
        start_date = db_next_date()

    while start_date < 20180101:  # TODO: while not sync as remote data
        end_date = jutil.shift(start_date, n_weeks=56)
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
        start_date = db_next_date()
    print('Database initialization complete.')


if __name__ == '__main__':
    fire.Fire()
