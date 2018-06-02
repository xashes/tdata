from arctic import Arctic
from arctic.exceptions import NoDataFoundException
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
    arctic.delete_library('daily')
    arctic.initialize_library('daily')
    symbols = basedata.read('instruments').data['symbol']
    symbols = tqdm(symbols)
    daily_lib = arctic['daily']

    for symbol in symbols:
        try:
            df, _ = ds.daily(symbol, 19900101, 20180526, adjust_mode='post')
            df = df.set_index('trade_date')
            last_date = df.index[-1]
            daily_lib.write(
                symbol,
                df,
                metadata={
                    'source': 'jaqs',
                    'last_date': int(last_date)
                })
        except Exception as e:
            print(f'Failed for {symbol}: {str(e)}')


def init_minute_lib():
    arctic.delete_library('minute')
    arctic.initialize_library('minute')
    symbols = basedata.read('instruments').data['symbol']
    symbols = tqdm(symbols)
    minute_lib = arctic['minute']

    errors = []
    for symbol in symbols:
        try:
            df, _ = ds.bar(symbol, trade_date=20180502)
            df = df.set_index('trade_date')
            last_date = df.index[-1]
            minute_lib.write(
                symbol,
                df,
                metadata={
                    'source': 'jaqs',
                    'last_date': int(last_date)
                })
        except Exception as e:
            error = f'Failed for {symbol}: {str(e)}'
            print(error)
            errors.append(error)

    # TODO: store in database
    with open('minute_log.txt', 'w') as lh:
        lh.writelines(errors)


def update_daily_lib():
    symbols = basedata.read('instruments').data['symbol']
    symbols = tqdm(symbols)
    daily_lib = arctic['daily']
    newest_trade_date = remote.newest_trade_date()

    for symbol in symbols:
        try:
            try:
                start_date = ds.query_next_trade_date(
                    daily_lib.read(symbol).metadata.get('last_date'))
            except NoDataFoundException as e:
                print(f'Can not get last date with {symbol}: {str(e)}')
                start_date = 19900101
            df, _ = ds.daily(
                symbol, start_date, newest_trade_date, adjust_mode='post')
            df = df.set_index('trade_date')
            last_date = df.index[-1]
            daily_lib.append(
                symbol,
                df,
                metadata={
                    'source': 'jaqs',
                    'last_date': int(last_date)
                })
        except Exception as e:
            print(f'Failed for {symbol}: {str(e)}')


def update_minute_lib():
    symbols = basedata.read('instruments').data['symbol']
    symbols = tqdm(symbols)
    minute_lib = arctic['minute']

    for symbol in symbols:
        try:
            start_date = ds.query_next_trade_date(
                minute_lib.read(symbol).metadata.get('last_date'))
        except NoDataFoundException as e:
            print(f'There is no data for {symbol}: {str(e)}')
            start_date = 20180502
        date_range = ds.query_trade_dates(start_date,
                                            remote.newest_trade_date())

        # TODO: deal with IOError
        # -- reset date_range from date to today, sleep(60), continue to next loop
        # -- or just break the loop, continue with next symbol
        for date in date_range:
            try:
                df, _ = ds.bar(symbol, trade_date=date)
            except IOError as e:
                print(f'{symbol}-{date}: {str(e)}')
                break
            try:
                df = df.set_index('trade_date')
                last_date = df.index[-1]
            except IndexError as e:
                print(f'{symbol}-{date}: {str(e)}')
                continue
            minute_lib.append(
                symbol,
                df,
                metadata={
                    'source': 'jaqs',
                    'last_date': int(last_date)
                })


if __name__ == '__main__':
    # init_libraries()
    # init_daily_lib()
    # update_instruments_document()
    # update_daily_lib()
    # init_minute_lib( )
    update_minute_lib()
