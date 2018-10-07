import altair as alt
from tdata.features import Features
from datetime import datetime


def chart(sid='000001.XSHG',
          start_date='2018-01-01',
          end_date=datetime.today(),
          frequency='1d'):
    """
    Input columns: 'open close low high turnover volume macd macdsignal
    macdhist macdgrps macdhistgrps brushend bottom top'
    """
    df = Features(
        sid, start_date=start_date, end_date=end_date,
        frequency=frequency).data().reset_index()
    brushes = df[df.brushend.notna()]
    brushes = brushes.fillna(method='bfill')
    brushes = brushes.melt(
        id_vars='index', value_vars=['brushend', 'top', 'bottom'])

    nearest = alt.selection(
        type='single',
        nearest=True,
        on='mouseover',
        fields=['index', 'value'],
        empty='none')

    center = alt.Chart().mark_line().encode(
        alt.X('index:T'),
        alt.Y('value:Q', scale=alt.Scale(zero=False)),
        color='variable:N')

    selectors = alt.Chart().mark_point().encode(
        x='index:T',
        opacity=alt.value(0),
    ).add_selection(nearest)

    points = center.mark_point().encode(
        opacity=alt.condition(nearest, alt.value(1), alt.value(0)))

    text = center.mark_text(
        align='left', dx=5,
        dy=-5).encode(text=alt.condition(nearest, 'value:Q', alt.value(' ')))

    rules = alt.Chart().mark_rule(color='gray').encode(
        x='index:T', ).transform_filter(nearest)

    y_indicator = alt.Chart().mark_rule(color='pink').encode(
        y='value:Q').transform_filter(nearest)

    return alt.layer(
        center,
        selectors,
        points,
        rules,
        y_indicator,
        text,
        data=brushes,
        width=1680,
        height=800).interactive()


if __name__ == '__main__':
    chart(start_date='2000-01-01').save('chart.html')
