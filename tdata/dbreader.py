# Local db operations without network connection
# provide API to get data for instruments
# requirements: arctic and pandas

import pandas as pd
from arctic import Arctic
from arctic.date import DateRange
from datetime import datetime, date

arctic = Arctic('localhost')
basedata_lib = arctic['basedata']
day_lib = arctic['day']
minute_lib = arctic['minute1']


def get_price(sid, start_date='2018-01-01', end_date=date.today(), frequency='1d'):
    pass
