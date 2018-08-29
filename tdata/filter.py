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
    except Exception as e:
        print(f'{symbol}: {str(e)}')
        return
    try:
        if df.macd.iloc[-1] < df.macd.iloc[-2]:
            return False
    except:
        pass

    try:
        center_start_grp = df.loc[df.bottom.notna(), 'macdgrps'].iloc[-2]
    except Exception as e:
        print(f'{symbol}: {str(e)}')
        center_start_grp = 0

    # TODO lower than center low bounder

    def fail(groups):
        macd_length = groups['macd'].agg(lambda grp: max(grp, key=abs))
        if macd_length.iloc[-1] > 0:
            return True
        if macd_length.iloc[-1] / macd_length.min() > 2 / 3:
            return True

        areas = groups['macdhist'].agg(sum)
        if areas.iloc[-1] / areas.min() > 1 / 2:
            return True

        hist_length = groups['macdhist'].agg(lambda grp: max(grp, key=abs))
        if hist_length.iloc[-1] / hist_length.min() > 1 / 2 or (hist_length.iloc[-1] == hist_length.max()):
            return True


    # outer macd groups
    macdgrps = df[df.macdgrps >= center_start_grp].groupby('macdgrps')
    if fail(macdgrps):
        return False

    # inner groups
    last_grp = macdgrps.get_group(df.macdgrps.iloc[-1])
    if last_grp.macdhist.iloc[-1] > 0:
        drop_index = last_grp[last_grp.macdhistgrps == last_grp.macdhistgrps.iloc[-1]].index
        last_grp = last_grp.drop(drop_index)
    histgrps = last_grp.groupby('macdhistgrps')

    if fail(histgrps):
        return False

    return True
