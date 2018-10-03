# Local db operations without network connection
# provide API to get data for instruments
# requirements: arctic and pandas

import pandas as pd
from arctic import Arctic
from arctic.date import DateRange
from datetime import datetime
import tdata.util as tutil

arctic = Arctic('localhost')
basedata_lib = arctic['basedata']
day_lib = arctic['day']
minute_lib = arctic['minute1']


def get_price(sid: str = '000001.XSHG',
              start_date='2018-01-01',
              end_date=datetime.today(),
              frequency='1d'):
    if frequency.endswith('m'):
        lib = minute_lib
    else:
        lib = day_lib

    bar = lib.read(
        sid, date_range=DateRange(
            start_date,
            end_date)).data.rename(columns={'total_turnover': 'turnover'})

    if frequency.endswith('m') and frequency != '1m':
        frequency = frequency.replace('m', 'T')
        bar = tutil.resample_bar(frequency, bar)

    return bar
