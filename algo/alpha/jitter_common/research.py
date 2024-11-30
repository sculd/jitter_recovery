import algo.feature.jitter.calculate
import numpy as np
import matplotlib.pyplot as plt


def investigate_trading(dfst_trading):
    print(f'profit sum: {dfst_trading.profit.sum()}')
    df_position_changed = dfst_trading.groupby('timestamp').sum()[['position_changed']]

    if len(df_position_changed[df_position_changed.position_changed != 0]) == 0:
        print(f'no trading happens')
        return

    fig, ax_profit = plt.subplots(1, figsize=(16, 2))
    ax_profit.plot(dfst_trading.groupby('timestamp').sum().cumsum()[['profit']])


def investigate_symbol(df, symbol_investigate, add_trading_columns_func, trading_param, figsize=None):
    dfi = df.set_index(['timestamp', 'symbol'])
    dfs = dfi.xs(symbol_investigate, level=1)
    df_feature = algo.feature.jitter.calculate.get_feature_df(dfs, trading_param.feature_param)
    df_trading = add_trading_columns_func(df_feature, trading_param)

    if len(df_trading[(df_trading.in_position == 1) | (df_trading.in_position == -1)]) == 0:
        print(f'no trading happens')
        dfs[['close']].plot()
        return df_feature, df_trading

    figsize = figsize if figsize else (6, 9)
    fig, (ax_close, ax_volume, ax_profit, ax_chs, ax_in_position, ax_profit_in_position) = plt.subplots(6, figsize=figsize)
    ax_close.plot(dfs[['close']])

    ymin, ymax = df_trading[['value']].min(), df_trading[['value']].max()
    ax_close.vlines(x=list(df_trading[df_trading.position_changed == +1].index), ymin=ymin, ymax=ymax, color='b',
              linestyles='dashed', label='enter')
    ax_close.vlines(x=list(df_trading[df_trading.position_changed == -1].index), ymin=ymin, ymax=ymax, color='r',
              linestyles='dashed', label='exit')

    ax_volume.plot(dfs[['volume']])
    ax_profit.plot(df_trading[df_trading.in_position != 0][['profit']].cumsum())
    ax_chs.plot(df_trading[['ch_max', 'ch_since_max']])

    ymin, ymax = df_trading[['ch_since_max']].min(), df_trading[['ch_max']].max()
    ax_chs.vlines(x=list(df_trading[df_trading.position_changed == +1].index), ymin=ymin, ymax=ymax, color='b',
              linestyles='dashed', label='enter')
    ax_chs.vlines(x=list(df_trading[df_trading.position_changed == -1].index), ymin=ymin, ymax=ymax, color='r',
              linestyles='dashed', label='exit')

    ax_in_position.plot(df_trading[['in_position']])
    ax_profit_in_position.plot(df_trading[(df_trading.in_position.shift() != 0)][['profit']].cumsum())

    indices_position_changed = df_trading[(df_trading.position_changed != 0) & ~(np.isnan(df_trading.position_changed))].index
    i_head = df_trading.index.get_loc(indices_position_changed[0])
    i_tail = df_trading.index.get_loc(indices_position_changed[-1])
    if not hasattr(trading_param, 'is_long_term') or not trading_param.is_long_term:
        df_plot = df_trading.iloc[i_head - 1 * 60:i_tail + 1 * 60]
    else:
        df_plot = df_trading.iloc[i_head - 12 * 60:i_tail + 12 * 60]
    ax = df_plot[['value']].plot(figsize=(figsize[0], 2))
    ymin, ymax = df_plot[['value']].min(), df_trading[['value']].max()
    ax.vlines(x=list(df_plot[df_plot.position_changed == +1].index), ymin=ymin, ymax=ymax, color='b',
              linestyles='dashed', label='enter')
    ax.vlines(x=list(df_plot[df_plot.position_changed == -1].index), ymin=ymin, ymax=ymax, color='r',
              linestyles='dashed', label='exit')
    plt.show()

    return df_feature, df_trading
