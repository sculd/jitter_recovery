import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt
import algo.jitter_recovery.calculate




def get_dfsts(df, trading_param, symbol_filter=None):
    dfi = df.set_index(['timestamp', 'symbol'])
    all_symbols = df.symbol.unique()
    if symbol_filter is None:
        symbol_filter = lambda s: 'USDT' in s

    all_symbols = [s for s in all_symbols if symbol_filter(s)]

    if not trading_param.is_long_term:        
        initial_run_resolution = 4
    else:
        initial_run_resolution = 5

    window_ = int(trading_param.feature_param.window / initial_run_resolution)
    dfst_feature_approximate = None
    symbol_with_jumps = []

    for i, symbol in enumerate(all_symbols):
        dfs = dfi.xs(symbol, level=1)
        dfs_ = dfs.resample(f'{initial_run_resolution}min').last()
        jitter_recovery_trading_param_ = algo.jitter_recovery.calculate.JitterRecoveryFeatureParam(window_)
        
        df_feature_ = algo.jitter_recovery.calculate.get_feature_df(dfs_, jitter_recovery_trading_param_)
        del dfs
        del dfs_
        
        print(f'{i} symbol: {symbol}: {len(df_feature_[df_feature_.ch_max >= trading_param.jump_threshold * 0.9])} (approx)')
        if len(df_feature_[df_feature_.ch_max >= trading_param.jump_threshold * 0.9]) > 0:
            symbol_with_jumps.append(symbol)
        
        df_feature_['symbol'] = symbol
        if dfst_feature_approximate is None:
            dfst_feature_approximate = df_feature_.copy()
        else:
            dfst_feature_approximate = pd.concat([dfst_feature_approximate, df_feature_])

        del df_feature_

    dfst_feature_approximate = dfst_feature_approximate.reset_index().set_index(['symbol', 'timestamp'])
    print(f'symbol_with_jumps: {len((symbol_with_jumps))}')

    dfst_feature = df.set_index(['symbol', 'timestamp'])
    dfst_trading = df.set_index(['symbol', 'timestamp'])
    for i, symbol in enumerate(symbol_with_jumps):
        dfs = dfi.xs(symbol, level=1)
        
        df_feature = algo.jitter_recovery.calculate.get_feature_df(dfs, trading_param.feature_param)
        del dfs

        print(f'{i} symbol: {symbol}: {len(df_feature[df_feature.ch_max >= trading_param.jump_threshold * 0.9])}')
        df_trading = add_trading_columns(df_feature, trading_param)
        
        for column in df_feature.columns:
            dfst_feature.loc[symbol, column] = df_feature[column].values

        for column in df_trading.columns:
            dfst_trading.loc[symbol, column] = df_trading[column].values

        del df_feature
        del df_trading

    return dfst_feature_approximate, dfst_feature, dfst_trading


def add_trading_columns(df_feature, trading_param):
    status = algo.jitter_recovery.calculate.Status()
    trading_dict = defaultdict(list)

    for i in df_feature.index:
        features = df_feature.loc[i].to_dict()
        all_features = features
        status.update(all_features, trading_param)
        for k, v in {**all_features, **algo.jitter_recovery.calculate.status_as_dict(status)}.items():
            trading_dict[k].append(v)

    df_feature_trading = pd.DataFrame(trading_dict, index=df_feature.index)
    df_feature_trading['position_changed'] = df_feature_trading.in_position.diff()
    df_feature_trading['profit_raw'] = -df_feature_trading.value.diff() * df_feature_trading.in_position.shift()
    df_feature_trading['profit'] = -df_feature_trading.value.pct_change() * df_feature_trading.in_position.shift()

    return df_feature_trading


def investigate_trading(dfst_feature_approximate, dfst_trading):
    fig, ax_profit = plt.subplots(1, figsize=(16,2))
    ax_profit.plot(dfst_trading[['profit']].groupby('timestamp').sum().cumsum())

    df_position_changed = dfst_trading.groupby('timestamp').sum()[['position_changed']]

    if len(df_position_changed[df_position_changed.position_changed != 0]) == 0:
        print(f'no trading happens')


def investigate_symbol(df, symbol_investigate, trading_param, figsize=None):
    dfi = df.set_index(['timestamp', 'symbol'])
    dfs = dfi.xs(symbol_investigate, level=1)
    df_feature = algo.jitter_recovery.calculate.get_feature_df(dfs, trading_param.feature_param)
    df_trading = add_trading_columns(df_feature, trading_param)
    
    if len(df_trading[df_trading.in_position == 1]) == 0:
        print(f'no trading happens')
        return df_feature, df_trading
        
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
