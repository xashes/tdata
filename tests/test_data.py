import tdata.data as data


def test_daily_table():
    assert len(data.local_daily('000001.SH')) == len(
        data.ds.query_trade_dates(19901219, data.local_last_date()))
