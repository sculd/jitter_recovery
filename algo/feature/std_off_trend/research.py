import algo.feature.std_off_trend.calculate
import algo.feature.util.research
from algo.feature.std_off_trend.calculate import StdOffTrendFeatureParam


_feature_label_prefix = '(std_off_trend)'


def get_feature_label_for_caching(feature_param: StdOffTrendFeatureParam, label_suffix=None) -> str:
    r = algo.feature.util.research.get_param_label_for_caching(feature_param, _feature_label_prefix, label_suffix=label_suffix)
    return f'feature/{r}'


def get_dfst_feature(df, feature_param: StdOffTrendFeatureParam, symbol_filter=None, value_column='close'):
    return algo.feature.util.research.get_dfst_feature(
        df, algo.feature.std_off_trend.calculate.get_feature_df, feature_param, _feature_label_prefix, symbol_filter=symbol_filter, value_column='close')
