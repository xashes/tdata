# version 2.0
# TODO: adjust mode = post history or get dividen parameter - consider using dataview
# TODO: figure out how to calculate adjust price

import os
from datetime import datetime

import jaqs.util as jutil
from sqlalchemy import create_engine

import tdata.local as local
from tdata.consts import (DAILY_TABLE, HISTORY_DB, HISTORY_DIR, INDEX_TABLE,
                          MINUTE_TABLE, SH_INDEX, STOCK_TABLE)
from tdata.remote_service import ds

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
    print('Downloading index table.')
    index_df, msg = ds.query(
        view='jz.instrumentInfo',
        fields='status,list_date,name,market',
        filter='inst_type=100&status=1&symbol=',
        data_format='pandas')
    print('Writing to the database.')
    index_df.to_sql(INDEX_TABLE, engine, if_exists='replace')


def update_stock_table():
    print('Downloading stock table.')
    stock_df, msg = ds.query(
        view='jz.instrumentInfo',
        fields='status,list_date,name,market',
        filter='inst_type=1&status=1&symbol=',
        data_format='pandas')
    print('Writing to the database.')
    stock_df.to_sql(STOCK_TABLE, engine, if_exists='replace')


def daily_next_date():
    return ds.query_next_trade_date(local.daily_last_date())


def bar_next_date():
    return ds.query_next_trade_date(local.bar_last_date())


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
    props = {
        'symbol': SH_INDEX,
        'trade_date': today,
        'fields': 'symbol,trade_date,close'
    }
    remote_bar, msg = ds.bar(**props)
    print('\nLocal Bar:\n{}'.format(
        local.bar(start_date=local.bar_last_date()).tail()))
    print('\nRemote Bar:\n{}'.format(remote_bar.tail()))


def update_daily_table(end_date: int = today):
    if local.daily_last_date() == today:
        print('The Daily Table is already up-to-date.')
        return
    if not engine.dialect.has_table(engine, DAILY_TABLE):
        start_date = 19901219
    else:
        start_date = daily_next_date()

    props = {
        'symbol': local.query_all_symbols(),
        'start_date': start_date,
        'end_date': end_date,
        'fields': remote_fields
    }
    print('Downloading daily data from {} to {}.'.format(start_date, end_date))
    df, msg = ds.daily(**props)
    print('Writing to the database.')
    df.to_sql(DAILY_TABLE, engine, if_exists='append', chunksize=100000)
    print('Daily table updating complete.')


# TODO: complete this function
def update_minute_table(end_date: int = today) -> None:
    if local.bar_last_date() == today:
        print('The Minute Table is already up-to-date.')
        return
    if not engine.dialect.has_table(engine, MINUTE_TABLE):
        # start_date = 20120104
        start_date = jutil.shift(today, n_weeks=-10)
    else:
        start_date = bar_next_date()

    trade_dates = ds.query_trade_dates(start_date, end_date)
    for date in trade_dates:
        props = dict(
            symbol=local.query_all_symbols(),
            trade_date=date,
            freq='1M',
            fields=remote_fields)
        print('Downloading minute data of {}.'.format(date))
        bar, msg = ds.bar(**props)
        print('Writing to the database.')
        bar.to_sql(MINUTE_TABLE, engine, if_exists='append', chunksize=100000)
        print(f'Minute data of {date} updated.')


def update_database():
    update_index_table()
    update_stock_table()
    update_daily_table()
    update_minute_table()


if __name__ == '__main__':
    import fire
    fire.Fire()
