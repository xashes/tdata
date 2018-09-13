import pandas as pd
from tdata import scanner, local


def first_buy_filter(matrix):
    """
    input: last center matrix
    """
    return matrix.query('macd_1 < 0').query('macd_1 > macd_3').query(
        'macd_3 > macd_min').query('hist_1 > hist_3').query(
            'hist_3 > hist_min')

def first_buy_targets(symbols=local.SYMBOLS, end_date=local.today, freqs=['W', 'D', 60, 15]):
    for fq in freqs:
        matrix = scanner.last_center_matrix(symbols, end_date, freq=fq)
        result = first_buy_filter(matrix)
        symbols = result.index
    return result
