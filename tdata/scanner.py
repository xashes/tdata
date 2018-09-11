import pandas as pd
from tdata import filter as tfilter
from tdata import local, feature
from tqdm import tqdm
from collections import defaultdict
import jaqs.util as jutil


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


def drop_symbol(symbol):
    pass


def last_center_matrix(symbols=local.SYMBOLS,
                       end_date=local.today,
                       freq='D',
                       target=None):
    symbols = tqdm(symbols)
    matrix = pd.DataFrame()

    for s in symbols:
        try:
            df = local.bar(s, end_date=end_date, freq=freq)
            if df.high.iloc[-1] == 0:
                continue

            matrix = matrix.append(feature.last_center(df, target=target))
        except:
            pass
    return matrix


def matrix_from_different_date():
    dates = pd.date_range(end='2018-09-08', periods=5, freq='3M')
    dataset = pd.DataFrame()
    for dt in dates:
        dataset = dataset.append(
            last_center_matrix(
                end_date=jutil.convert_datetime_to_int(dt), target=20))
    return dataset


if __name__ == '__main__':
    scan_first_buy()
