from tdata import feature, local


def exhausted(df):
    try:
        center_start_grp = df.loc[df.bottom.notna(), 'macdgrps'].iloc[-2]
    except IndexError as e:
        print(str(e))
        center_start_grp = 0
    macdgrps = df[df.macdgrps >= center_start_grp].groupby('macdgrps')

    extremes = macdgrps[['macd', 'macdhist']].agg(lambda x: max(abs(x)))
    areas = macdgrps[['macd', 'macdhist']].agg(sum)

    last_hist_length = extremes['macdhist'].iloc[-1]
    last_hist_area = areas['macdhist'].iloc[-1]

    last_macd_length = extremes['macd'].iloc[-1]

    if (last_macd_length < extremes.macd.max()) and (
            last_hist_length < extremes.macdhist.max()) and (
                last_hist_area < areas.macdhist.max()):
        return True


def first_buy(df):
    """
    input: pd.DataFrame with columns added
    """
    try:
        symbol = df.symbol.iloc[0]
        print(f'Start processing {symbol}')
    except Exception as e:
        print(f'{symbol}: {str(e)}')
        return
    # if df.macd.pct_change().iloc[-1] < 0:
    #     print('macd is down')
    #     return False

    try:
        center_start_grp = df.loc[df.bottom.notna(), 'macdgrps'].iloc[-2]
    except Exception as e:
        print(f'{symbol}: {str(e)}')
        center_start_grp = 0
    # except Exception as e:
    #     print(f'{symbol}: {str(e)}')
    #     return

    # TODO lower than center low bounder

    def fail(groups):
        macd_length = groups['macd'].agg(lambda grp: max(grp, key=abs))
        if macd_length.iloc[-1] > 0:
            print('macd already > 0')
            return True
        if macd_length.iloc[-1] / macd_length.min() > 2 / 3:
            print('macd is too long')
            return True

        areas = groups['macdhist'].agg(sum)
        if areas.iloc[-1] / areas.min() > 1 / 2:
            print('the area of macdhist is too big')
            return True

        hist_length = groups['macdhist'].agg(lambda grp: max(grp, key=abs))
        if hist_length.iloc[-1] / hist_length.min() > 1 / 2 or (hist_length.iloc[-1] == hist_length.max()):
            print('the macd hist is too long to the down side')
            return True


    # outer macd groups
    macdgrps = df[df.macdgrps >= center_start_grp].groupby('macdgrps')
    print('Start processing the outer macd groups')
    if fail(macdgrps):
        return False

    # inner groups
    print('Start processing the inner macdhist groups')
    last_grp = macdgrps.get_group(df.macdgrps.iloc[-1])
    if last_grp.macdhist.iloc[-1] > 0:
        drop_index = last_grp[last_grp.macdhistgrps == last_grp.macdhistgrps.iloc[-1]].index
        last_grp = last_grp.drop(drop_index)
    histgrps = last_grp.groupby('macdhistgrps')

    if fail(histgrps):
        return False

    return True
