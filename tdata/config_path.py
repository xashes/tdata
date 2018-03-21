# encoding: UTF-8

import os

DATA_CONFIG_PATH = os.path.expanduser('~/ownCloud/jaqs_data/config/data_config.json')
TRADE_CONFIG_PATH = os.path.expanduser('~/ownCloud/jaqs_data/config/trade_config.json')

HISTORY_PATH = os.path.expanduser('~/ownCloud/jaqs_data/history')

print("Current data config file path: {}".format(DATA_CONFIG_PATH))
print("Current trade config file path: {}".format(TRADE_CONFIG_PATH))
print("Current history dataview file path: {}".format(HISTORY_PATH))
