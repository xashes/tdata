# version 2.0
# TODO: adjust mode = post history or get dividen parameter - consider using dataview
# TODO: figure out how to calculate adjust price

from datetime import datetime

import jaqs.util as jutil
from arctic import Arctic
from tqdm import tqdm
import pandas as pd

from tdata.remote_service import ds
from tdata import local

arctic = Arctic('pi3')
BASEDATA_LIB = arctic['basedata']
DAILY_LIB = arctic['daily']
MINUTE_LIb = arctic['minute']


def newest_trade_date():
    today = jutil.dtutil.convert_datetime_to_int(datetime.today())
    if ds.is_trade_date(today):
        return today
    else:
        return ds.query_last_trade_date(today)


today = newest_trade_date()

def download_index_table():
    print('Downloading index table.')
    index_df, _ = ds.query(
        view='jz.instrumentInfo',
        fields='list_date,name,market,inst_type',
        filter='inst_type=100&status=1&symbol=',
        data_format='pandas')
    return index_df[index_df['market'].str.contains(r'SH|SZ')]


def download_stock_table():
    print('Downloading stock table.')
    stock_df, _ = ds.query(
        view='jz.instrumentInfo',
        fields='list_date,name,market,inst_type',
        filter='inst_type=1&status=1&symbol=',
        data_format='pandas')
    return stock_df[stock_df['symbol'].str.contains(r'SH|SZ')]


def update_instruments_document():
    stocks = download_stock_table()
    idx = download_index_table()
    instruments = pd.concat([stocks, idx], ignore_index=True)

    BASEDATA_LIB.write(
        'instruments',
        instruments,
        metadata={
            'source': 'jaqs',
            'updated': datetime.today()
        })


def remote_sample_bar():
    props = dict(symbol='000001.SH', trade_date=today)
    bar, msg = ds.bar(**props)
    return bar.tail()


def remote_uptodate() -> bool:
    bar = remote_sample_bar()
    if len(bar):
        return True
    return False


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
    if local.minute_last_date() == today:
        print('The Minute Table is already up-to-date.')
        return

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
    update_instruments_document()
    update_daily_table()
    update_minute_table()


if __name__ == '__main__':
    import fire
    fire.Fire()
