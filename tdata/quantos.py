import jaqs.util as jutil
from jaqs.data import RemoteDataService

from tdata.consts import DATA_CONFIG_PATH

data_config = jutil.read_json(DATA_CONFIG_PATH)

ds = RemoteDataService()
ds.init_from_config(data_config)
