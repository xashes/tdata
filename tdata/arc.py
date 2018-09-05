from arctic import Arctic
from arctic.exceptions import NoDataFoundException
import pandas as pd
from tqdm import tqdm

import local
import remote
from remote_service import ds
from datetime import datetime
import jaqs.util as jutil

arctic = Arctic('pi3')
basedata = arctic['basedata']
SYMBOLS = basedata.read('instruments').data['symbol']

# TODO: replace print with logging
# TODO: refactor update and init functions
# TODO: write test before refactor


def init_libraries():
    arctic.initialize_library('basedata')
    arctic.initialize_library('daily')
    arctic.initialize_library('minute')


def update_instruments_document():
    if jutil.convert_datetime_to_int(
            basedata.read('instruments').metadata.get(
                'updated')) >= remote.newest_trade_date():
        print('Instruments document is up to date.')
        return

    stocks = remote.download_stock_table()
    idx = remote.download_index_table()
    instruments = pd.concat([stocks, idx], ignore_index=True)

    basedata.write(
        'instruments',
        instruments,
        metadata={
            'source': 'jaqs',
            'updated': datetime.today()
        })


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


def init_zen_lib():
    arctic.delete_library('zen')
    arctic.initialize_library('zen')


def init_center_lib():
    arctic.delete_library('center')
    arctic.initialize_library('center')


def update_daily_lib():
    symbols = basedata.read('instruments').data['symbol']
    symbols = tqdm(symbols)
    daily_lib = arctic['daily']

    if daily_lib.read('000001.SH').metadata.get(
            'last_date') == remote.newest_trade_date():
        print('The daily lib is up to date.')
        return

    for symbol in symbols:
        try:
            start_date = ds.query_next_trade_date(
                daily_lib.read(symbol).metadata.get('last_date'))
        except NoDataFoundException as e:
            print(f'There is no data for {symbol}: {str(e)}')
            start_date = 19900101
        try:
            df, _ = ds.daily(
                symbol,
                start_date,
                remote.newest_trade_date(),
                adjust_mode='post')
        except IOError as e:
            print(f'{symbol}: {str(e)}')
            continue
        try:
            df = df.set_index('trade_date')
            last_date = df.index[-1]
        except IndexError as e:
            print(f'{symbol}: {str(e)}')
            continue
        daily_lib.append(
            symbol,
            df,
            metadata={
                'source': 'jaqs',
                'last_date': int(last_date)
            })


def update_minute_lib():
    symbols = tqdm(SYMBOLS)
    minute_lib = arctic['minute']

    for symbol in symbols:
        try:
            start_date = ds.query_next_trade_date(
                minute_lib.read(symbol).metadata.get('last_date'))
        except NoDataFoundException as e:
            print(f'There is no data for {symbol}: {str(e)}')
            start_date = ds.query_next_trade_date(
                minute_lib.read('000001.SH').metadata.get('last_date'))
        date_range = ds.query_trade_dates(start_date,
                                          remote.newest_trade_date())

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


def update_zen_lib():
    import zen
    symbols = tqdm(SYMBOLS)
    zen_lib = arctic['zen']

    for symbol in symbols:
        try:
            day_bar = local.daily(symbol).loc[:, [
                'close', 'high', 'low', 'open', 'symbol', 'trade_status',
                'turnover', 'volume'
            ]]
            brush = zen.hist_sum(day_bar)
            zen_lib.write(
                symbol, brush, metadata={'updated': datetime.today()})
        except Exception as e:
            print(f'{symbol}: {str(e)}')


def update_center_lib():
    import scanner
    center_lib = arctic['center']

    freqs = [30, 15, 5]
    for freq in freqs:
        matrix = scanner.last_center_matrix(freq=freq)
        center_lib.write(str(freq), matrix)


if __name__ == '__main__':
    import fire
    fire.Fire()
    update_instruments_document()
    update_daily_lib()
    update_minute_lib()
