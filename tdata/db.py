import remote
from pymongo import MongoClient

CLIENT = MongoClient('mongodb://localhost:27017/')
DB = CLIENT['tdata']

def update_stock_table():
    stocks = remote.download_stock_table()
    documents = stocks.to_dict(orient='records')
    collection = DB['stocks']
    collection.insert_many(documents, ordered=False)


def update_index_table():
    idx = remote.download_index_table()
    documents = idx.to_dict(orient='records')
    collection = DB['indexes']
    collection.insert_many(documents, ordered=False)
