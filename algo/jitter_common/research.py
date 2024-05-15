import algo.jitter_common.calculate
from algo.jitter_common.calculate import JitterFeatureParam
import matplotlib.pyplot as plt

_primitives = (bool, str, int, float, type(None))

def _is_primitive(obj):
    return isinstance(obj, _primitives)


def _param_as_label(param):
    if _is_primitive(param):
        return str(param)
    # new directory is used to avoid the file name limit (256) violation.
    return '/'.join([f'{k}({_param_as_label(v)})' for k, v in vars(param).items()])


def get_param_label_for_caching(param, label_prefix, label_suffix=None) -> str:
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


_feature_label_prefix = '(changes)'


def get_feature_label_for_caching(feature_param: JitterFeatureParam, label_suffix=None) -> str:
    r = get_param_label_for_caching(feature_param, _feature_label_prefix, label_suffix=label_suffix)
    return f'feature/{r}'


def _get_usdt_symbol_filter():
    return lambda s: 'USDT' in s


def get_dfst_feature(df, feature_param: JitterFeatureParam, symbol_filter=None, value_column='close'):
    dfi = df.set_index(['timestamp', 'symbol'])
    all_symbols = df.symbol.unique()
    if symbol_filter is None:
        symbol_filter = _get_usdt_symbol_filter()
    all_symbols = [s for s in all_symbols if symbol_filter(s)]
    print(f'all_symbols: {len(all_symbols)}')

    dfst_feature = df.set_index(['symbol', 'timestamp'])
    for i, symbol in enumerate(all_symbols):
        dfs = dfi.xs(symbol, level=1)

        df_feature = algo.jitter_common.calculate.get_feature_df(dfs, feature_param, value_column=value_column)
        del dfs

        print(f'{i} symbol: {symbol} (feature)')

        for column in df_feature.columns:
            dfst_feature.loc[symbol, column] = df_feature[column].values

        del df_feature

    return dfst_feature


def investigate_trading(dfst_trading):
    print(f'profit sum: {dfst_trading.profit.sum()}')
    df_position_changed = dfst_trading.groupby('timestamp').sum()[['position_changed']]

    if len(df_position_changed[df_position_changed.position_changed != 0]) == 0:
        print(f'no trading happens')
        return

    fig, ax_profit = plt.subplots(1, figsize=(16, 2))
    ax_profit.plot(dfst_trading.groupby('timestamp').sum().cumsum()[['profit']])


def investigate_symbol(df, symbol_investigate, add_trading_columns, trading_param, figsize=None):
    dfi = df.set_index(['timestamp', 'symbol'])
    dfs = dfi.xs(symbol_investigate, level=1)
    df_feature = algo.jitter_common.calculate.get_feature_df(dfs, trading_param.feature_param)
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
    if not trading_param.is_long_term:
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
