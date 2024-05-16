import pandas as pd
from collections import defaultdict
import algo.feature.jitter.calculate
import algo.feature.util.research
from algo.feature.collective_jitter.calculate import CollectiveJitterFeatureParam

collective_feature_columns_no_rolling = ['ch', 'ch_max', 'ch_min', 'ch_since_max', 'ch_since_min']
collective_feature_columns = collective_feature_columns_no_rolling + ['ch_window30_min']


_feature_label_prefix = '(collectivechanges)'

def get_feature_label_for_caching(feature_param: algo.feature.jitter.calculate.JitterFeatureParam,
                                  label_suffix=None) -> str:
    r = algo.feature.util.research.get_param_label_for_caching(feature_param, _feature_label_prefix, label_suffix=label_suffix)
    return f'feature/{r}'


def _get_usdt_symbol_filter():
    return lambda s: 'USDT' in s


def get_dfst_feature(df, feature_param: CollectiveJitterFeatureParam, symbol_filter=None, value_column='close'):
    dfi = df.set_index(['timestamp', 'symbol'])
    all_symbols = df.symbol.unique()
    if symbol_filter is None:
        symbol_filter = _get_usdt_symbol_filter()
    all_symbols = [s for s in all_symbols if symbol_filter(s)]
    print(f'all_symbols: {len(all_symbols)}')

    dfst_feature = df.set_index(['symbol', 'timestamp'])
    if len(all_symbols) == 0:
        return dfst_feature
    for i, symbol in enumerate(all_symbols):
        dfs = dfi.xs(symbol, level=1)

        df_feature = algo.feature.jitter.calculate.get_feature_df(dfs, feature_param, value_column=value_column)
        del dfs

        print(f'{i} symbol: {symbol} (feature)')

        for column in df_feature.columns:
            dfst_feature.loc[symbol, column] = df_feature[column].values
        del df_feature

    dfst_with_collective_feature = _append_collective_feature(df, dfst_feature, feature_param,
                                                              symbol_filter=symbol_filter)
    return dfst_with_collective_feature


def _append_collective_feature(df, dfst_feature, feature_param: CollectiveJitterFeatureParam, symbol_filter=None):
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
            dfst_with_collective_feature.loc[symbol, f'{column}_collective'] = df_collective_feature_index_aligned[
                column].values
        del df_feature

    del df_collective_feature

    return dfst_with_collective_feature


def _get_df_collective_feature(dfst_feature, feature_param: CollectiveJitterFeatureParam):
    df_collective_feature = dfst_feature.dropna().groupby('timestamp')[
        collective_feature_columns_no_rolling].median().resample('1min').asfreq().ffill()
    df_collective_feature['ch_std'] = dfst_feature.dropna().groupby('timestamp')[
        ['ch']].std().resample('1min').asfreq().ffill().values
    df_collective_feature[f'ch_window{feature_param.collective_window}_min'] = df_collective_feature.ch.rolling(
        window=feature_param.collective_window).min()
    df_collective_feature[f'ch_window{feature_param.collective_window}_max'] = df_collective_feature.ch.rolling(
        window=feature_param.collective_window).max()
    return df_collective_feature

