import pandas as pd
import numpy as np
import algo.feature.jitter.calculate
import algo.feature.util.research
from algo.feature.jitter.calculate import JitterFeatureParam


_feature_label_prefix = '(changes)'


def get_feature_label_for_caching(feature_param: JitterFeatureParam, label_suffix=None) -> str:
    r = algo.feature.util.research.get_param_label_for_caching(feature_param, _feature_label_prefix, label_suffix=label_suffix)
    return f'feature/{r}'


def _get_usdt_symbol_filter():
    return lambda s: 'USDT' in s


def get_dfst_feature(df, feature_param: JitterFeatureParam, symbol_filter=None, value_column='close'):
    all_symbols = df.symbol.unique()
    if symbol_filter is None:
        symbol_filter = _get_usdt_symbol_filter()
    all_symbols = [s for s in all_symbols if symbol_filter(s)]
    print(f'all_symbols: {len(all_symbols)}')

    df_values = df[df.symbol.isin(set(all_symbols))].reset_index().pivot(index="timestamp", columns="symbol", values=value_column)

    window = feature_param.window
    rows = [algo.feature.jitter.calculate.get_changes_1dim(
        np.array([v for v in df_.to_numpy(dtype=np.float64)], dtype=np.float64)
    ) for df_ in df_values.rolling(window, min_periods=window)]

    feature_keys = list(rows[0].keys())
    feature_values = sum([[r[k] for k in feature_keys] for r in rows], [])
    feature_index = pd.MultiIndex.from_product([df_values.index, feature_keys], names=[df_values.index.name, 'feature'])
    df_feature = pd.DataFrame(np.array(feature_values), index=feature_index, columns=df_values.columns)

    return df_feature

