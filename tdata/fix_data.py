from arctic import Arctic
from tqdm import tqdm

arctic = Arctic('pi3')
basedata = arctic['basedata']
SYMBOLS = basedata.read('instruments').data['symbol']
minute_lib = arctic['minute']
daily_lib = arctic['daily']


def drop_broken_data(start_date, lib):
    symbols = tqdm(SYMBOLS)

    for symbol in symbols:
        try:
            data = lib.read(symbol).data
            after = data.index[data.index >= start_date]
            data = data.drop(after)
            last_date = data.index[-1]
            lib.write(
                symbol,
                data,
                metadata={
                    'source': 'jaqs',
                    'last_date': int(last_date)
                })
        except Exception as e:
            print(f'{symbol}: {str(e)}')


if __name__ == '__main__':
    # drop_broken_data(20180823, lib=daily_lib)
    drop_broken_data(20180830, lib=minute_lib)
