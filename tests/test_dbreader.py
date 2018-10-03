from tdata.dbreader import get_price
from datetime import datetime


class TestGetPrice():
    def test_freq_1d(self):
        df = get_price(
            '000001.XSHE',
            start_date='2018-01-01',
            end_date='2018-08-31',
            frequency='1d')

        assert df.index[0] == datetime(2018, 1, 2)
        assert df.index[-1] == datetime(2018, 8, 31)
        assert df.shape == (164, 8)

        return

    def test_freq_1m(self):
        df = get_price(
            '000001.XSHE',
            start_date='2018-08-20',
            end_date=datetime(2018, 8, 31, 15),
            frequency='1m')

        assert df.index[0] == datetime(2018, 8, 20, 9, 31)
        assert df.index[-1] == datetime(2018, 8, 31, 15)
        assert df.shape == (2400, 6)

        return

    def test_freq_5m(self):
        df = get_price(
            '000001.XSHE',
            start_date='2018-08-20',
            end_date=datetime(2018, 8, 31, 15),
            frequency='5m')

        assert df.index[0] == datetime(2018, 8, 20, 9, 35)
        assert df.index[-1] == datetime(2018, 8, 31, 15)
        assert df.shape == (480, 6)

        return

    def test_freq_15m(self):
        df = get_price(
            '000001.XSHE',
            start_date='2018-08-20',
            end_date=datetime(2018, 8, 31, 15),
            frequency='15m')

        assert df.index[0] == datetime(2018, 8, 20, 9, 45)
        assert df.index[-1] == datetime(2018, 8, 31, 15)
        assert df.shape == (160, 6)

        return

    def test_freq_30m(self):
        df = get_price(
            '000001.XSHE',
            start_date='2018-08-20',
            end_date=datetime(2018, 8, 31, 15),
            frequency='30m')

        assert df.index[0] == datetime(2018, 8, 20, 10)
        assert df.index[-1] == datetime(2018, 8, 31, 15)
        assert df.shape == (80, 6)

        return
