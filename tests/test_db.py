from arctic import Arctic

arctic = Arctic('pi3')
basedata_lib = arctic['basedata']
daily_lib = arctic['daily']
minute_lib = arctic['minute']

dc = daily_lib.read('600016.SH')
print(dc.version)
print(dc.metadata)
print(dc.data.head())
print(dc.data.tail(10))
print(daily_lib.list_symbols()[:5])
