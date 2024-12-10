import algo.feature.jitter.calculate
import algo.feature.util.research
from algo.feature.jitter.calculate import JitterFeatureParam
import matplotlib.pyplot as plt


_feature_label_prefix = '(changes)'


def get_feature_label_for_caching(feature_param: JitterFeatureParam, label_suffix=None) -> str:
    r = algo.feature.util.research.get_param_label_for_caching(feature_param, _feature_label_prefix, label_suffix=label_suffix)
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

        df_feature = algo.feature.jitter.calculate.get_feature_df(dfs, feature_param, value_column=value_column)
        del dfs

        print(f'{i} symbol: {symbol} ({_feature_label_prefix})')

        for column in df_feature.columns:
            dfst_feature.loc[symbol, column] = df_feature[column].values

        del df_feature

    return dfst_feature
