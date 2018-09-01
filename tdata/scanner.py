import pandas as pd
from tdata import filter as tfilter
from tdata import local, feature
from tqdm import tqdm
from collections import defaultdict


def scan_first_buy(end_date=local.today):
    # TODO 排除停牌股票
    symbols = tqdm(local.SYMBOLS)

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


def last_center_matrix(end_date=local.today, freq='D'):
    symbols = tqdm(local.SYMBOLS)
    matrix = pd.DataFrame()

    for s in symbols:
        try:
            df = local.bar(s, end_date=end_date, freq=freq)
            if df.trade_status.iloc[-1] != '交易':
                continue
            df = feature.add_columns(df)
            matrix = matrix.append(feature.last_center(df), ignore_index=True)
        except:
            pass
    return matrix


if __name__ == '__main__':
    scan_first_buy()
