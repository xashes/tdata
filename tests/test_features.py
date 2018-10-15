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


def test_shape(features):
    df = features.data()

    assert df.index[0] == datetime(2014, 2, 25)
    assert df.index[-1] == datetime(2018, 8, 1)
    assert df.shape == (1086, 17)

    assert round(df.loc['2018-08-01', 'EMA120']) == 3049
    assert round(df.loc['2018-08-01', 'EMA250']) == 3139 + 2


def test_brushend(features):
    df = features.data()
    brushends = df.brushend.dropna()

    assert brushends.shape == (34, )
    assert brushends.min() == 1974.382
    assert brushends.max() == 5178.191
    assert brushends.loc['2014-02-25'] == 2087.6160
    assert brushends.loc['2015-12-23'] == 3684.5674
    assert brushends.loc['2018-07-06'] == 2691.0208


def test_inner_center(features):
    df = features.data()
    bottoms = df.bottom.dropna()
    tops = df.top.dropna()

    assert bottoms.shape == (4, )
    assert bottoms.loc['2015-01-23'] == 2018.3600
    assert bottoms.loc['2016-01-27'] == 3399.2803
    assert bottoms.loc['2016-08-16'] == 2916.3744
    assert bottoms.loc['2018-07-06'] == 3044.2912

    assert tops.shape == (4, )
    assert tops.loc['2018-07-06'] == 3140.4408


@pytest.mark.skip(reason='not implemented')
def test_center_bound(features):
    df = features.data()
    lbound = df.lbound.dropna()
    ubound = df.ubound.dropna()

    assert lbound.shape == (4, )
    assert round(lbound.loc['2018-07-06']) == 2969
    assert round(lbound.loc['2016-08-16']) == 2638
    assert round(ubound.loc['2018-07-06']) == 3587
    assert round(ubound.loc['2016-08-16']) == 3097
