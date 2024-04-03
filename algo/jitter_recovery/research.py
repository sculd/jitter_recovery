import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt
import algo.jitter_recovery.calculate
from algo.jitter_recovery.calculate import JitterRecoveryFeatureParam, JitterRecoveryTradingParam


_feature_label_prefix = '(changes)'
_trading_label_prefix = '(changes_trading)'

_primitives = (bool, str, int, float, type(None))

def _is_primitive(obj):
    return isinstance(obj, _primitives)

def _param_as_label(param):
    if _is_primitive(param):
        return str(param)
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


def get_feature_label_for_caching(feature_param: JitterRecoveryFeatureParam, label_suffix=None) -> str:
    return _get_param_label_for_caching(feature_param, _feature_label_prefix, label_suffix=label_suffix)

def get_trading_label_for_caching(trading_param: JitterRecoveryTradingParam, label_suffix=None) -> str:
    return _get_param_label_for_caching(trading_param, _trading_label_prefix, label_suffix=label_suffix)


def _get_usdt_symbol_filter():
    return lambda s: 'USDT' in s


def get_dfst_feature(df, feature_param, symbol_filter=None):
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

    return dfst_feature


def get_dfst_trading(dfst_feature, trading_param):
    symbol_with_jumps = dfst_feature[
        (dfst_feature.ch_max > trading_param.jump_threshold)
        & (dfst_feature.ch_since_max < trading_param.drop_from_jump_threshold)
        & (dfst_feature.distance_max_ch < 10)
        & (dfst_feature.distance_max_ch > 2)
        ].index.get_level_values('symbol').unique().values

    print(f'symbol_with_jumps: {len((symbol_with_jumps))}')

    dfst_trading = dfst_feature.copy()
    for i, symbol in enumerate(symbol_with_jumps):
        df_feature = dfst_feature.xs(symbol, level=0)

        print(f'{i} symbol: {symbol}: {len(df_feature[df_feature.ch_max > trading_param.jump_threshold])} (trading)')
        df_trading = add_trading_columns(df_feature, trading_param)

        for column in df_trading.columns:
            if column in df_feature.columns:
                continue
            dfst_trading.loc[symbol, column] = df_trading[column].values

        del df_feature
        del df_trading

    return dfst_trading


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

    del trading_dict
    return df_feature_trading


def investigate_trading(dfst_trading):
    print(f'profit sum: {dfst_trading.profit.sum()}')
    df_position_changed = dfst_trading.groupby('timestamp').sum()[['position_changed']]

    if len(df_position_changed[df_position_changed.position_changed != 0]) == 0:
        print(f'no trading happens')
        return

    fig, ax_profit = plt.subplots(1, figsize=(16,2))
    ax_profit.plot(dfst_trading.groupby('timestamp').sum().cumsum()[['profit']])


def investigate_symbol(df, symbol_investigate, trading_param, figsize=None):
    dfi = df.set_index(['timestamp', 'symbol'])
    dfs = dfi.xs(symbol_investigate, level=1)
    df_feature = algo.jitter_recovery.calculate.get_feature_df(dfs, trading_param.feature_param)
    df_trading = add_trading_columns(df_feature, trading_param)
    
    if len(df_trading[df_trading.in_position == 1]) == 0:
        print(f'no trading happens')
        dfs[['close']].plot()
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
