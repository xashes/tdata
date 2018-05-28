import remote
from pymongo import MongoClient
import local
from tqdm import tqdm

CLIENT = MongoClient()
DB = CLIENT['tdata']


def update_instruments_collection():
    stocks = remote.download_stock_table().to_dict(orient='records')
    collection = DB['instruments']
    collection.insert_many(stocks, ordered=False)
    idx = remote.download_index_table().to_dict(orient='records')
    collection.insert_many(idx, ordered=False)


def update_daily_collection():
    df = local.daily().reset_index().to_dict(orient='records')
    collection = DB['daily']
    collection.insert_many(df)


def convert_minute_collection_old():
    df = local.bar('000001.SZ').reset_index()
    symbol = df.symbol[0]
    df = df.to_dict(orient='records')
    collection = DB[f'minute.{symbol}']
    collection.insert_many(df)


def convert_minute_collection():
    collection = DB['minute']
    error_instruments = []
    cursor = DB.instruments.find({}, {'symbol': 1, '_id': 0})
    symbols = [c['symbol'] for c in cursor]
    symbols = tqdm(symbols)
    for symbol in symbols:
        try:
            df = local.bar(symbol)
            df = df.reset_index().to_dict(orient='records')
            collection.insert_many(df)
        except (TypeError, IndexError):
            print(symbol)
            error_instruments.append(symbol)
        with open('log.txt', 'a') as logger:
            logger.writelines(error_instruments)

def convert_daily_collection():
    collection = DB['daily']
    error_instruments = []
    cursor = DB.instruments.find({}, {'symbol': 1, '_id': 0})
    symbols = [c['symbol'] for c in cursor]
    for symbol in symbols:
        try:
            df = local.daily(symbol)
            df = df.reset_index().to_dict(orient='records')
            collection.insert_many(df)
        except (TypeError, IndexError):
            print(symbol)
            error_instruments.append(symbol)
        with open('log.txt', 'a') as logger:
            logger.writelines(error_instruments)

def main():
    convert_minute_collection()
    # convert_daily_collection()
    # update_instruments_collection()


if __name__ == '__main__':
    main()
