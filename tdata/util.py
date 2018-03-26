# util.py - some tool functions for data process

import pandas as pd


def resample_bar(rule: str, on: str, symbol: str,
                 bar: pd.DataFrame) -> pd.DataFrame:
    resampled = bar.resample(rule, on=on)
    result = pd.DataFrame({
        'close': resampled['close'].last(),
        'high': resampled['high'].max(),
        'low': resampled['low'].min(),
        'open': resampled['open'].first(),
        'symbol': symbol,
        'turnover': resampled['turnover'].sum(),
        'volume': resampled['volume'].sum()
    })
    return result
