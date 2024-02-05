import pandas as pd
import matplotlib.pyplot as plt
import algo.jitter_recovery.calculate


def get_dfsts(df, trading_param):
    dfi = df.set_index(['timestamp', 'symbol'])
    all_symbols = df.symbol.unique()
    all_symbols = [s for s in all_symbols if 'USDT' in s]

    if not trading_param.is_long_term:        
        initial_run_resolution = 2
    else:
        initial_run_resolution = 5

    jump_window_ = int(trading_param.jitter_recovery_feature_param.jump_window / initial_run_resolution)
    dfst_feature = df.set_index(['symbol', 'timestamp'])
    dfst_trading = df.set_index(['symbol', 'timestamp'])
    symbol_with_jumps = []

    for i, symbol in enumerate(all_symbols):
        if 'USDT' not in symbol: continue
        dfs = dfi.xs(symbol, level=1)
        dfs_ = dfs.resample(f'{initial_run_resolution}min').last()
        jitter_recovery_trading_param_ = algo.jitter_recovery.calculate.JitterRecoveryFeatureParam(jump_window_)
        
        df_feature = algo.jitter_recovery.calculate.get_feature_df(dfs_, jitter_recovery_trading_param_)

        print(f'{i} symbol: {symbol}: {len(df_feature[df_feature.ch_max >= trading_param.jump_threshold * 0.9])}')
        if len(df_feature[df_feature.ch_max >= trading_param.jump_threshold * 0.9]) == 0: continue

        symbol_with_jumps.append(symbol)
        
        df_feature = algo.jitter_recovery.calculate.get_feature_df(dfs, trading_param.jitter_recovery_feature_param)
        del dfs
        del dfs_
        df_trading = algo.jitter_recovery.calculate.add_trading_columns(df_feature, trading_param)
        
        for column in df_feature.columns:
            dfst_feature.loc[symbol, column] = df_feature[column].values

        for column in df_trading.columns:
            dfst_trading.loc[symbol, column] = df_trading[column].values

        del df_feature
        del df_trading

    print(f'symbol_with_jumps: {len((symbol_with_jumps))}')
    return dfst_feature, dfst_trading


def investigate_symbol(df, symbol_investigate, trading_param, figsize=None):
    dfi = df.set_index(['timestamp', 'symbol'])
    dfs = dfi.xs(symbol_investigate, level=1)
    df_feature = algo.jitter_recovery.calculate.get_feature_df(dfs, trading_param.jitter_recovery_feature_param)
    df_trading = algo.jitter_recovery.calculate.add_trading_columns(df_feature, trading_param)
    figsize = figsize if figsize else (6, 9)
    fig, (ax_close, ax_profit, ax_chs, ax_in_position, ax_profit_in_position) = plt.subplots(5, figsize=figsize)
    ax_close.plot(dfs[['close']])
    ax_profit.plot(df_trading[['profit']].cumsum())
    ax_chs.plot(df_trading[['ch_max', 'ch_since_max']])
    ax_in_position.plot(df_trading[['in_position']])
    ax_profit_in_position.plot(df_trading[(df_trading.in_position.shift() != 0)][['profit']].cumsum())


    i_head = df_trading.index.get_loc(df_trading[df_trading.position_changed == +1].index[0])
    i_tail = df_trading.index.get_loc(df_trading[df_trading.position_changed == -1].index[-1])
    df_plot = df_trading.iloc[i_head-10:i_tail+10]
    ax = df_plot[['value']].plot(figsize=(9,2))
    ymin, ymax = df_plot[['value']].min(), df_trading[['value']].max()
    ax.vlines(x=list(df_plot[df_plot.position_changed == +1].index), ymin=ymin, ymax=ymax, color='b', linestyles='dashed', label='enter')
    ax.vlines(x=list(df_plot[df_plot.position_changed == -1].index), ymin=ymin, ymax=ymax, color='r', linestyles='dashed', label='enter')
    plt.show()

    i_head = df_trading.index.get_loc(df_trading[df_trading.position_changed == +1].index[0])
    i_tail = df_trading.index.get_loc(df_trading[df_trading.position_changed == -1].index[-1])
    if not trading_param.is_long_term:
        df_plot = df_trading.iloc[i_head-2*60:i_tail+2*60]
    else:
        df_plot = df_trading.iloc[i_head-12*60:i_tail+12*60]
    ax = df_plot[['value']].plot(figsize=(9,2))
    ymin, ymax = df_plot[['value']].min(), df_trading[['value']].max()
    ax.vlines(x=list(df_plot[df_plot.position_changed == +1].index), ymin=ymin, ymax=ymax, color='b', linestyles='dashed', label='enter')
    ax.vlines(x=list(df_plot[df_plot.position_changed == -1].index), ymin=ymin, ymax=ymax, color='r', linestyles='dashed', label='enter')
    plt.show()    

    return df_feature, df_trading