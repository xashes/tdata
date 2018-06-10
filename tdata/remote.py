# version 2.0
# TODO: adjust mode = post history or get dividen parameter - consider using dataview
# TODO: figure out how to calculate adjust price

from datetime import datetime

import pandas as pd

import jaqs.util as jutil
from arctic import Arctic
from tdata import local
from tdata.remote_service import ds
from tqdm import tqdm

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


def remote_sample_bar():
    props = dict(symbol='000001.SH', trade_date=today)
    bar, msg = ds.bar(**props)
    return bar.tail()


def remote_uptodate() -> bool:
    bar = remote_sample_bar()
    if len(bar):
        return True
    return False


if __name__ == '__main__':
    import fire
    fire.Fire()
