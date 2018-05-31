from arctic import Arctic
import pandas as pd
from tqdm import tqdm

import local
import remote
from remote_service import ds

arctic = Arctic('192.168.199.217')
basedata = arctic['basedata']

def init_libraries():
    arctic.initialize_library('basedata')
    arctic.initialize_library('daily')
    arctic.initialize_library('minute')


def update_instruments_document():
    stocks = remote.download_stock_table()
    idx = remote.download_index_table()
    instruments = pd.concat([stocks, idx], ignore_index=True)

    basedata.write('instruments', instruments, metadata={'source': 'jaqs'})

def init_daily_lib():
    symbols = basedata.read('instruments').data['symbol']
    symbols = tqdm(symbols)
    daily_lib = arctic['daily']

    for symbol in symbols:
        try:
            df, _ = ds.daily(symbol, 19900101, 20180601, adjust_mode='post')
            df = df.set_index('trade_date')
            last_date = df.index[-1]
            daily_lib.write(symbol, df, metadata={'source': 'jaqs', 'last_date': int(last_date)})
        except Exception as e:
            print(f'Failed for {symbol}: {str(e)}')


def convert_daily_collection():
    daily = arctic['daily']

    symbols = basedata.read('instruments').data['symbol']
    symbols = tqdm(symbols)

    for symbol in symbols:
        try:
            data = local.daily(symbol, 19900101)
            daily.write(symbol, data, metadata={'source': 'jaqs'})
        except Exception as e:
            print(f'Failed for {symbol}: {str(e)}')

def convert_minute_collection():
    minute = arctic['minute']

    symbols = basedata.read('instruments').data['symbol']
    symbols = tqdm(symbols)

    for symbol in symbols:
        try:
            data = local.bar(symbol, 20171201)
            minute.write(symbol, data, metadata={'source': 'jaqs'})
        except Exception as e:
            print(f'Failed for {symbol}: {str(e)}')

if __name__ == '__main__':
    # init_libraries()
    init_daily_lib()
    # update_instruments_document()
    # convert_daily_collection()
    # convert_minute_collection()
