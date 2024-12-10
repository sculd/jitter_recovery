import algo.feature.timed_burst.calculate
import algo.feature.util.research
from algo.feature.timed_burst.calculate import TimedBurstFeatureParam


_feature_label_prefix = '(timed_burst)'


def get_feature_label_for_caching(feature_param: TimedBurstFeatureParam, label_suffix=None) -> str:
    r = algo.feature.util.research.get_param_label_for_caching(feature_param, _feature_label_prefix, label_suffix=label_suffix)
    return f'feature/{r}'


def _get_usdt_symbol_filter():
    return lambda s: 'USDT' in s


def get_dfst_feature(df, feature_param: TimedBurstFeatureParam, symbol_filter=None, value_column='close'):
    return algo.feature.util.research.get_dfst_feature(
        df, algo.feature.timed_burst.calculate.get_feature_df, feature_param, _feature_label_prefix, symbol_filter=symbol_filter, value_column='close')
