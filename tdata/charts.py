from pyecharts import Kline, Bar, Line
from pyecharts import Grid, Overlap
from tdata import feature


def brush(data):
    # data preprocessing
    data = feature.full_data(data)

    # top grid
    kline = Kline()
    kline.add(
        'Kline',
        data.index,
        data.loc[:, ['open', 'close', 'low', 'high']].values,
        mark_line=['max', 'min'],
        mark_line_valuedim=['highest', 'lowest'],
        is_datazoom_show=True,
        datazoom_xaxis_index=[0, 1, 2],
        datazoom_type='both',
        tooltip_axispointer_type='cross',
        is_more_utils=True,
    )

    brush = Line()
    brush.add('Brush', data.index, data.endpoint)
    overlap = Overlap()
    overlap.add(kline)
    overlap.add(brush)

    # middle grid
    macdhist = Bar()
    macdhist.add(
        'macdhist',
        data.index,
        data.macdhist.values,
        mark_line=['max', 'min'], legend_pos='68%'
    )

    macd = Line()
    macd.add('macd', data.index, data.macd)
    macd.add('macd signal', data.index, data.macdsignal)

    middle = Overlap()
    middle.add(macdhist)
    middle.add(macd)

    # bottom grid
    turnover = Bar()
    turnover.add(
        '',
        data.index,
        data.turnover.values / pow(10, 6),
        mark_line=['max', 'min'])

    grid = Grid('', width='100%', height=1200)
    grid.add(overlap, grid_bottom='45%')
    grid.add(middle, grid_top='57%', grid_bottom='17%')
    grid.add(turnover, grid_top='85%')

    return grid
