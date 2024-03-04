import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt
import algo.jitter_recovery.calculate
import algo.jitter_recovery.calculate_collective


collective_feature_columns_no_rolling = ['ch', 'ch_max', 'ch_min', 'ch_since_max', 'ch_since_min']
collective_feature_columns = collective_feature_columns_no_rolling + ['ch_window30_min']


def get_dfsts(df, trading_param):
    dfst_feature = get_dfst_feature(df, trading_param.feature_param)
    dfst_trading = get_dfst_trading(df, dfst_feature, trading_param)

    return dfst_feature, dfst_trading


def get_dfst_feature(df, feature_param):
    dfi = df.set_index(['timestamp', 'symbol'])
    all_symbols = df.symbol.unique()
    all_symbols = [s for s in all_symbols if 'USDT' in s]
      
    dfst_feature = df.set_index(['symbol', 'timestamp'])
    for i, symbol in enumerate(all_symbols):
        if 'USDT' not in symbol: continue
        dfs = dfi.xs(symbol, level=1)
        
        df_feature = algo.jitter_recovery.calculate.get_feature_df(dfs, feature_param)
        del dfs
        
        print(f'{i} symbol: {symbol} (feature)')
        
        for column in df_feature.columns:
            dfst_feature.loc[symbol, column] = df_feature[column].values
        del df_feature

    return dfst_feature


def get_dfst_trading(df, dfst_feature, trading_param):
    all_symbols = dfst_feature.index.get_level_values('symbol').unique()
    all_symbols = [s for s in all_symbols if 'USDT' in s]
      
    df_collective_feature = dfst_feature.dropna().groupby('timestamp')[collective_feature_columns_no_rolling].median().resample('1min').asfreq().ffill()
    df_collective_feature['ch_window30_min'] = df_collective_feature.ch.rolling(window=30).min() 
    df_collective_feature['ch_window30_max'] = df_collective_feature.ch.rolling(window=30).max() 

    dfst_trading = df.set_index(['symbol', 'timestamp'])
    for i, symbol in enumerate(all_symbols):
        if 'USDT' not in symbol: continue
        
        df_feature = dfst_feature.xs(symbol, level=0)
        
        if trading_param.collective_drop_recovery_trading_param  is not None:
            l = len(df_feature[df_feature.ch_min <= trading_param.collective_drop_recovery_trading_param.drop_threshold * 0.9])
        elif trading_param.collective_jump_recovery_trading_param  is not None:
            l = len(df_feature[df_feature.ch_max >= trading_param.collective_jump_recovery_trading_param.jump_threshold * 0.9])
        print(f'{i} symbol: {symbol} ({l}):(trading)')
        df_trading = add_trading_columns(df_feature, df_collective_feature, trading_param)
        
        for column in df_trading.columns:
            dfst_trading.loc[symbol, column] = df_trading[column].values
        del df_trading
    
    del df_collective_feature

    return dfst_trading


def add_trading_columns(df_feature, df_collective_feature, trading_param):
    status = algo.jitter_recovery.calculate_collective.Status()
    trading_dict = defaultdict(list)

    for i in df_feature.index:
        features = df_feature.loc[i].to_dict()
        if i in df_collective_feature.index:
            collective_featuers = df_collective_feature.loc[i].to_dict()
        else:
            collective_featuers = {c: 0.0 for c in collective_feature_columns}
        status.update(collective_featuers, features, trading_param)

        for k, v in {**features, **algo.jitter_recovery.calculate_collective.status_as_dict(status)}.items():
            trading_dict[k].append(v)
        for k, v in collective_featuers.items():
            trading_dict[f'{k}_collective'].append(v)

    df_feature_trading = pd.DataFrame(trading_dict, index=df_feature.index)
    df_feature_trading['position_changed'] = df_feature_trading.in_position.diff()
    df_feature_trading['profit_raw'] = df_feature_trading.value.diff() * df_feature_trading.in_position.shift()
    df_feature_trading['profit'] = df_feature_trading.value.pct_change() * df_feature_trading.in_position.shift()

    return df_feature_trading

def investigate_symbol(df, df_collective_feature, symbol_investigate, trading_param, figsize=None):
    dfi = df.set_index(['timestamp', 'symbol'])
    dfs = dfi.xs(symbol_investigate, level=1)
    df_feature = algo.jitter_recovery.calculate.get_feature_df(dfs, trading_param.feature_param)
    df_trading = add_trading_columns(df_feature, df_collective_feature, trading_param)
    
    if len(df_trading[df_trading.in_position != 0]) == 0:
        print(f'no trading happens')
        fig, (ax_close, ax_collective) = plt.subplots(2, figsize=figsize)
        ax_close.plot(dfs[['close']])
        ax_collective.plot(df_collective_feature[['ch', 'ch_window30_min', 'ch_window30_max']])
        return df_feature, df_trading

    figsize = figsize if figsize else (6, 9)
    fig, (ax_close, ax_collective, ax_profit, ax_chs, ax_in_position, ax_profit_in_position) = plt.subplots(6, figsize=figsize)
    ax_close.plot(dfs[['close']])
    ax_collective.plot(df_collective_feature[['ch', 'ch_window30_min', 'ch_window30_max']])
    ax_profit.plot(df_trading[['profit']].cumsum())
    ax_chs.plot(df_trading[['ch_min', 'ch_since_min']])
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
    df_plot = df_trading.iloc[i_head-12*60:i_tail+12*60]
    ax = df_plot[['value']].plot(figsize=(9,2))
    ymin, ymax = df_plot[['value']].min(), df_trading[['value']].max()
    ax.vlines(x=list(df_plot[df_plot.position_changed == +1].index), ymin=ymin, ymax=ymax, color='b', linestyles='dashed', label='enter')
    ax.vlines(x=list(df_plot[df_plot.position_changed == -1].index), ymin=ymin, ymax=ymax, color='r', linestyles='dashed', label='enter')
    plt.show()    

    ix_head = df_trading[df_trading.position_changed == +1].index[0]
    ix_tail = df_trading[df_trading.position_changed == -1].index[-1]
    df_plot = df_collective_feature[['ch', 'ch_window30_min', 'ch_window30_max']].loc[ix_head:ix_tail]
    ax = df_plot.plot(figsize=(9,2))
    ymin, ymax = df_plot[['ch']].min(), df_trading[['ch']].max()
    ax.vlines(x=list(df_trading[df_trading.position_changed == +1].index), ymin=ymin, ymax=ymax, color='b', linestyles='dashed', label='enter')
    ax.vlines(x=list(df_trading[df_trading.position_changed == -1].index), ymin=ymin, ymax=ymax, color='r', linestyles='dashed', label='enter')
    plt.show()    

    return df_feature, df_trading