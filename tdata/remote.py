# version 2.0
# DONE: append update
# DONE: write index_df and stock_df into database, use 'replace' method
# DONE: set primary key for daily table
# TODO: adjust mode = post history or get dividen parameter
# TODO: figure out how to calculate adjust price

import os
from datetime import datetime

import fire
import jaqs.util as jutil
from sqlalchemy import create_engine

import tdata.local as local
from tdata.consts import (DAILY_TABLE, HISTORY_DB, HISTORY_DIR, INDEX_TABLE,
                          MINUTE_TABLE, SH_INDEX, STOCK_TABLE)
from tdata.quantos import ds

db_path = os.path.join(HISTORY_DIR, HISTORY_DB)
engine = create_engine('sqlite:///{}'.format(db_path))


def newest_trade_date():
    today = jutil.dtutil.convert_datetime_to_int(datetime.today())
    if ds.is_trade_date(today):
        return today
    else:
        return ds.query_last_trade_date(today)


today = newest_trade_date()
remote_fields = 'symbol,freq,close,high,low,open,trade_date,trade_status,turnover,volume'


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


def next_bar_date():
    return ds.query_next_trade_date(local.last_bar_date())


def remote_sample_bar():
    props = dict(symbol=SH_INDEX, trade_date=today, fields=remote_fields)
    bar, msg = ds.bar(**props)
    return bar.tail()


def remote_uptodate() -> bool:
    bar = remote_sample_bar()
    if len(bar):
        return True
    return False


def test_new_data():
    symbol = SH_INDEX
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


def update_daily_table():
    props = {
        'symbol': local.query_all_symbols(),
        'start_date': db_next_date(),
        'end_date': today,
        'fields': remote_fields
    }
    print('Downloading daily data from {} to {}.'.format(
        db_next_date(), today))
    df, msg = ds.daily(**props)
    print('Writing to the Database.')
    df.to_sql(DAILY_TABLE, engine, if_exists='append', chunksize=1000)


def init_daily_table():
    if not engine.dialect.has_table(engine, DAILY_TABLE):
        start_date = 19901219
    else:
        start_date = db_next_date()

    while start_date <= 20180101:  # TODO: while not sync as remote data
        end_date = jutil.shift(start_date, n_weeks=56)
        props = {
            'symbol': local.query_all_symbols(),
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


# TODO: complete this function
def update_minute_table() -> None:
    if not remote_uptodate():
        print('The remote data is not up-to-date.')
        return
    if local.last_bar_date() == today:
        print('The Minute Table is already up-to-date.')
        return
    if not engine.dialect.has_table(engine, MINUTE_TABLE):
        # start_date = 20120104
        start_date = jutil.shift(today, n_weeks=-10)
    else:
        start_date = next_bar_date()

    end_date = today

    trade_dates = ds.query_trade_dates(start_date, end_date)
    for date in trade_dates:
        props = dict(
            symbol=local.query_all_symbols(),
            trade_date=date,
            freq='1M',
            fields=remote_fields)
        print('Downloading minute data of {}.'.format(date))
        bar, msg = ds.bar(**props)
        print('Writing to the Database.')
        bar.to_sql(MINUTE_TABLE, engine, if_exists='append', chunksize=10000)


def update_database():
    update_index_table()
    update_stock_table()
    update_daily_table()
    update_minute_table()


if __name__ == '__main__':
    fire.Fire()
