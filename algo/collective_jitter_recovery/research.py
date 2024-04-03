import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt
import algo.jitter_recovery.calculate
import algo.collective_jitter_recovery.calculate
from algo.collective_jitter_recovery.calculate import CollectiveRecoveryFeatureParam, CollectiveDropRecoveryTradingParam

collective_feature_columns_no_rolling = ['ch', 'ch_max', 'ch_min', 'ch_since_max', 'ch_since_min']
collective_feature_columns = collective_feature_columns_no_rolling + ['ch_window30_min']

_feature_label_prefix = '(collectivechanges)'
_trading_label_prefix = '(collectivechanges_trading)'

_primitives = (bool, str, int, float, type(None))

def _is_primitive(obj):
    return isinstance(obj, _primitives)

def _param_as_label(param):
    if _is_primitive(param):
        return str(param)
    # new directory is used to avoid the file name limit (256) violation.
    return '/'.join([f'{k}({_param_as_label(v)})' for k, v in vars(param).items()])

def _get_param_label_for_caching(param, label_prefix, label_suffix=None) -> str:
    raw_label = _param_as_label(param)
    label_tokens = raw_label.split('/')
    label_dirs = []
    label_dir = ''
    for label_token in label_tokens:
        label_dir += f'_{label_token}'
        if len(label_dir) > 200:
            label_dirs.append(label_dir[1:])
            label_dir = ''

    if len(label_dir) > 1:
        label_dirs.append(label_dir[1:])

    label = '/'.join(label_dirs)
    ret = f"{label_prefix}_{label}"
    if label_suffix is not None:
        ret = f"{ret}_{label_suffix}"
    return ret


def get_feature_label_for_caching(feature_param: algo.jitter_recovery.calculate.JitterRecoveryFeatureParam, label_suffix=None) -> str:
    return _get_param_label_for_caching(feature_param, _feature_label_prefix, label_suffix=label_suffix)

def get_trading_label_for_caching(trading_param: CollectiveDropRecoveryTradingParam, label_suffix=None) -> str:
    return _get_param_label_for_caching(trading_param, _trading_label_prefix, label_suffix=label_suffix)


def _get_usdt_symbol_filter():
    return lambda s: 'USDT' in s


def get_dfst_feature(df, feature_param: CollectiveRecoveryFeatureParam, symbol_filter=None):
    dfi = df.set_index(['timestamp', 'symbol'])
    all_symbols = df.symbol.unique()
    if symbol_filter is None:
        symbol_filter = _get_usdt_symbol_filter()
    all_symbols = [s for s in all_symbols if symbol_filter(s)]
    print(f'all_symbols: {len(all_symbols)}')
      
    dfst_feature = df.set_index(['symbol', 'timestamp'])
    for i, symbol in enumerate(all_symbols):
        dfs = dfi.xs(symbol, level=1)
        
        df_feature = algo.jitter_recovery.calculate.get_feature_df(dfs, feature_param)
        del dfs
        
        print(f'{i} symbol: {symbol} (feature)')
        
        for column in df_feature.columns:
            dfst_feature.loc[symbol, column] = df_feature[column].values
        del df_feature

    dfst_with_collective_feature = _append_collective_feature(df, dfst_feature, feature_param, symbol_filter=symbol_filter)
    return dfst_with_collective_feature


def _append_collective_feature(df, dfst_feature, feature_param: CollectiveRecoveryFeatureParam, symbol_filter=None):
    all_symbols = dfst_feature.index.get_level_values('symbol').unique()
    if symbol_filter is None:
        symbol_filter = _get_usdt_symbol_filter()
    all_symbols = [s for s in all_symbols if symbol_filter(s)]

    df_collective_feature = _get_df_collective_feature(dfst_feature, feature_param)

    dfst_with_collective_feature = df.set_index(['symbol', 'timestamp'])
    for i, symbol in enumerate(all_symbols):
        print(f'{i} symbol: {symbol} (collective_feature)')

        df_feature = dfst_feature.xs(symbol, level=0)
        for column in df_feature.columns:
            dfst_with_collective_feature.loc[symbol, column] = df_feature[column].values

        df_collective_feature_index_aligned = df_collective_feature[
            (df_collective_feature.index >= dfst_feature.xs(symbol, level='symbol').index[0]) &
            (df_collective_feature.index <= dfst_feature.xs(symbol, level='symbol').index[-1])
            ]
        for column in df_collective_feature.columns:
            dfst_with_collective_feature.loc[symbol, f'{column}_collective'] = df_collective_feature_index_aligned[column].values
        del df_feature

    del df_collective_feature

    return dfst_with_collective_feature


def _get_df_collective_feature(dfst_feature, feature_param: CollectiveRecoveryFeatureParam):
    df_collective_feature = dfst_feature.dropna().groupby('timestamp')[
        collective_feature_columns_no_rolling].median().resample('1min').asfreq().ffill()
    df_collective_feature['ch_std'] = dfst_feature.dropna().groupby('timestamp')[
        ['ch']].std().resample('1min').asfreq().ffill().values
    df_collective_feature[f'ch_window{feature_param.collective_window}_min'] = df_collective_feature.ch.rolling(window=feature_param.collective_window).min()
    df_collective_feature[f'ch_window{feature_param.collective_window}_max'] = df_collective_feature.ch.rolling(window=feature_param.collective_window).max()
    return df_collective_feature


def get_dfst_trading(dfst_feature, trading_param):
    all_symbols = dfst_feature.index.get_level_values('symbol').unique()

    dfst_trading = dfst_feature.copy()
    for i, symbol in enumerate(all_symbols):
        df_feature = dfst_feature.xs(symbol, level=0)

        l = 0
        if trading_param.collective_drop_recovery_trading_param  is not None:
            l = len(df_feature[df_feature.ch_min <= trading_param.collective_drop_recovery_trading_param.drop_threshold * 0.99])
        elif trading_param.collective_jump_recovery_trading_param  is not None:
            l = len(df_feature[df_feature.ch_max >= trading_param.collective_jump_recovery_trading_param.jump_threshold * 0.99])
        print(f'{i} symbol: {symbol} ({l}):(trading)')
        df_trading = add_trading_columns(df_feature, trading_param)

        for column in df_trading.columns:
            if column in df_feature.columns:
                continue
            dfst_trading.loc[symbol, column] = df_trading[column].values
        del df_feature
        del df_trading

    return dfst_trading


def add_trading_columns(df_feature, trading_param):
    status = algo.collective_jitter_recovery.calculate.Status()
    trading_dict = defaultdict(list)

    for i in df_feature.index:
        features = df_feature.loc[i].to_dict()
        status.update(features, trading_param)

        for k, v in {**features, **algo.collective_jitter_recovery.calculate.status_as_dict(status)}.items():
            trading_dict[k].append(v)

    df_feature_trading = pd.DataFrame(trading_dict, index=df_feature.index)
    df_feature_trading['position_changed'] = df_feature_trading.in_position.diff()
    df_feature_trading['profit_raw'] = df_feature_trading.value.diff() * df_feature_trading.in_position.shift()
    df_feature_trading['profit'] = df_feature_trading.value.pct_change() * df_feature_trading.in_position.shift()

    return df_feature_trading


def investigate_symbol(df, df_collective_feature, symbol_investigate, trading_param, figsize=None):
    dfi = df.set_index(['timestamp', 'symbol'])
    dfs = dfi.xs(symbol_investigate, level=1)
    df_feature = algo.jitter_recovery.calculate.get_feature_df(dfs, trading_param.feature_param)
    df_trading = add_trading_columns(df_feature, trading_param)
    
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