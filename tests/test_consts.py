import os

import tdata.consts as ct


def test_path_exists():
    assert os.path.exists(ct.DATA_CONFIG_PATH)
    assert os.path.exists(ct.TRADE_CONFIG_PATH)
    assert os.path.exists(ct.HISTORY_DIR)
