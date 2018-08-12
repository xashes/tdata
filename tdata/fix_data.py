from arctic import Arctic
from tqdm import tqdm

arctic = Arctic('pi3')
basedata = arctic['basedata']
SYMBOLS = basedata.read('instruments').data['symbol']


def drop_broken_data(start_date):
    symbols = tqdm(SYMBOLS)
    minute_lib = arctic['minute']

    for symbol in symbols:
        try:
            data = minute_lib.read(symbol).data
            after = data.index[data.index >= start_date]
            data = data.drop(after)
            last_date = data.index[-1]
            minute_lib.write(
                symbol,
                data,
                metadata={
                    'source': 'jaqs',
                    'last_date': int(last_date)
                })
        except Exception as e:
            print(f'{symbol}: {str(e)}')


if __name__ == '__main__':
    drop_broken_data(20180807)
