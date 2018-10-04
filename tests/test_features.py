import pytest
from tdata.features import Features
from datetime import datetime


@pytest.fixture(scope='module')
def features():
    return Features(start_date='2014-01-01', end_date='2018-08-01')


def test_bar(features):
    df = features.bar()

    assert df.index[0] == datetime(2014, 1, 2)
    assert df.index[-1] == datetime(2018, 8, 1)
    assert df.shape == (1119, 6)


def test_data(features):
    df = features.data()

    assert df.index[0] == datetime(2014, 2, 25)
    assert df.index[-1] == datetime(2018, 8, 1)
    assert df.shape == (1086, 14)
    assert 'brushend' in df.columns
    assert 'bottom' in df.columns
    assert 'top' in df.columns
