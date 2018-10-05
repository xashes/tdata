import altair as alt
from tdata.features import Features


def chart(df):
    """
    Input columns: 'open close low high turnover volume macd macdsignal
    macdhist macdgrps macdhistgrps brushend bottom top'
    """
    df = df.reset_index()

    # top
    kline_color = alt.condition("datum.open < datum.close", alt.value('red'),
                                alt.value('green'))
    xaxis = alt.X('index:T', axis=alt.Axis(title='Date', grid=True))
    rule = alt.Chart().mark_rule().encode(
        xaxis,
        alt.Y(
            'low', scale=alt.Scale(zero=False), axis=alt.Axis(title='Price')),
        alt.Y2('high'),
        color=kline_color)

    bar = alt.Chart().mark_bar().encode(
        xaxis, y='open', y2='close', color=kline_color)

    kline = alt.layer(rule, bar).properties(data=df, width=1200, height=800)
    kline.save('chart.html')
    return kline


if __name__ == '__main__':
    df = Features().data()
    chart(df)
