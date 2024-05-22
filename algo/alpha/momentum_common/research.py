import pandas as pd
import matplotlib.pyplot as plt


def investigate_symbol(dfst_feature, symbol_investigate, add_trading_columns_func, trading_param, figsize=None):
    df_feature = dfst_feature.xs(symbol_investigate, level='symbol')
    df_trading = add_trading_columns_func(df_feature, trading_param)

    if len(df_trading[df_trading.in_position == 1]) == 0:
        print(f'no trading happens')
        df_feature[['close']].plot()
        return df_feature, df_trading

    figsize = figsize if figsize else (6, 9)
    fig, (ax_close, ax_profit, ax_in_position, ax_chs, ax_ranks, ax_profit_in_position) = plt.subplots(6, figsize=figsize)
    ax_close.plot(df_feature[['close', 'ema']])
    ax_profit.plot(df_trading[['profit']].cumsum())
    ax_chs.plot(df_trading[['ch', 'ch_ema', 'momentum']])
    ax_ranks.plot(df_trading[['rank', 'rank_descending']])
    ax_in_position.plot(df_trading[['in_position']])
    ax_profit_in_position.plot(df_trading[(df_trading.in_position.shift() != 0)][['profit']].cumsum())

    i_head = df_trading.index.get_loc(df_trading[df_trading.position_changed == +1].index[0])
    i_tail = df_trading.index.get_loc(df_trading[df_trading.position_changed == -1].index[-1])
    df_plot = df_trading.iloc[i_head - 10:i_tail + 10]
    ax = df_plot[['value']].plot(figsize=(9, 2))
    ymin, ymax = df_plot[['value']].min(), df_trading[['value']].max()
    ax.vlines(x=list(df_plot[df_plot.position_changed == +1].index), ymin=ymin, ymax=ymax, color='b',
              linestyles='dashed', label='enter')
    ax.vlines(x=list(df_plot[df_plot.position_changed == -1].index), ymin=ymin, ymax=ymax, color='r',
              linestyles='dashed', label='enter')
    plt.show()

    i_head = df_trading.index.get_loc(df_trading[df_trading.position_changed == +1].index[0])
    i_tail = df_trading.index.get_loc(df_trading[df_trading.position_changed == -1].index[-1])
    if not hasattr(trading_param, 'is_long_term') or not trading_param.is_long_term:
        df_plot = df_trading.iloc[i_head - 2 * 60:i_tail + 2 * 60]
    else:
        df_plot = df_trading.iloc[i_head - 12 * 60:i_tail + 12 * 60]
    ax = df_plot[['value']].plot(figsize=(9, 2))
    ymin, ymax = df_plot[['value']].min(), df_trading[['value']].max()
    ax.vlines(x=list(df_plot[df_plot.position_changed == +1].index), ymin=ymin, ymax=ymax, color='b',
              linestyles='dashed', label='enter')
    ax.vlines(x=list(df_plot[df_plot.position_changed == -1].index), ymin=ymin, ymax=ymax, color='r',
              linestyles='dashed', label='enter')
    plt.show()

    return df_feature, df_trading
