import algo.feature.crypto_temperature.calculate
import algo.feature.util.research
from algo.feature.crypto_temperature.calculate import CryptoTemperatureFeatureParam


_feature_label_prefix = '(crypto_temperature)'


def get_feature_label_for_caching(feature_param: CryptoTemperatureFeatureParam, label_suffix=None) -> str:
    r = algo.feature.util.research.get_param_label_for_caching(feature_param, _feature_label_prefix, label_suffix=label_suffix)
    return f'feature/{r}'


def _get_usdt_symbol_filter():
    return lambda s: 'USDT' in s




def get_dfst_feature(df, feature_param: CryptoTemperatureFeatureParam, symbol_filter=None, value_column='close'):
    dfi = df.set_index(['timestamp', 'symbol'])
    symbols = feature_param.symbols
    if symbol_filter is None:
        symbol_filter = _get_usdt_symbol_filter()
    symbols = [s for s in symbols if symbol_filter(s)]
    print(f'symbols: {len(symbols)}')

    dfst_feature = df.set_index(['symbol', 'timestamp'])
    for i, symbol in enumerate(symbols):
        dfs = dfi.xs(symbol, level=1)

        df_feature = algo.feature.crypto_temperature.calculate.get_feature_df(dfs, feature_param, value_column=value_column)
        del dfs

        print(f'{i} symbol: {symbol} ({_feature_label_prefix})')

        for column in df_feature.columns:
            dfst_feature.loc[symbol, column] = df_feature[column].values

        del df_feature

    return dfst_feature

'''
def get_dfst_feature(df, feature_param: CryptoTemperatureFeatureParam, symbol_filter=None, value_column='close'):
    def _get_feature_symbol_filter():
        return lambda s: s in set(feature_param.symbols)

    return algo.feature.util.research.get_dfst_feature(
        df, algo.feature.crypto_temperature.calculate.get_feature_df, feature_param, _feature_label_prefix, symbol_filter=_get_feature_symbol_filter, value_column='close')
'''


