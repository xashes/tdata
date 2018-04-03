from tdata import local
from tdata.remote_service import ds


def test_daily_table():
    assert len(local.daily('000001.SH', start_date=19901219)) == len(
        ds.query_trade_dates(19901219, local.daily_last_date()))
