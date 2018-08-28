# util.py - some tool functions for data process

import pandas as pd
import jaqs.util as jutil


def resample_bar(rule: str, bar: pd.DataFrame) -> pd.DataFrame:
    try:
        resampled = bar.resample(rule, closed='right', label='right')
        result = pd.DataFrame({
            'close': resampled['close'].last(),
            'high': resampled['high'].max(),
            'low': resampled['low'].min(),
            'open': resampled['open'].first(),
            'symbol': bar['symbol'].iloc[0],
            'turnover': resampled['turnover'].sum(),
            'volume': resampled['volume'].sum()
        })
        return result.dropna()
    except Exception as e:
        print('Resample Error: {str(e)}')


def combine_date_time_column(bar,
                             date_column: str = 'date',
                             time_column: str = 'time') -> pd.DataFrame:
    bar['datetime'] = jutil.combine_date_time(bar[date_column],
                                              bar[time_column])
    bar['datetime'] = pd.to_datetime(bar['datetime'], format='%Y%m%d%H%M%S')
    bar = bar.drop(columns=[date_column, time_column])
    return bar
