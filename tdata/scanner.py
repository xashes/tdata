from tdata import filter as tfilter
from tdata import local, feature
from tqdm import tqdm
from collections import defaultdict

from arctic import Arctic

arctic = Arctic('pi3')
basedata = arctic['basedata']
SYMBOLS = basedata.read('instruments').data['symbol'].iloc[5:500]


def scan_first_buy(end_date=local.today):
    # TODO 排除停牌股票
    symbols = tqdm(SYMBOLS)

    freqs = ['M', 'W', 'D', 30]
    targets = defaultdict(list)
    for freq in freqs:
        for symbol in symbols:
            try:
                df = local.bar(symbol, end_date=end_date, freq=freq)
                df = feature.add_columns(df)
                if tfilter.first_buy(df):
                    targets[freq].append(symbol)
            except Exception as e:
                pass

    return targets


if __name__ == '__main__':
    scan_first_buy()
