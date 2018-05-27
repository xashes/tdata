import remote
from pymongo import MongoClient
import local

CLIENT = MongoClient('mongodb://localhost:27017/')
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

def convert_minute_collection():
    df = local.bar('000001.SZ').reset_index()
    symbol = df.symbol[0]
    df = df.to_dict(orient='records')
    collection = DB[f'minute.{symbol}']
    collection.insert_many(df)



def main():
    # convert_minute_collection()
    print(DB.minute['000001.SH'].find_one())

if __name__ == '__main__':
    main()