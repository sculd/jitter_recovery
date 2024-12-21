import pandas as pd, numpy as np
from collections import defaultdict
import numba
from numba import njit
from numba.experimental import jitclass
import algo.feature.util.jitter_common

default_window = 30
default_window_longterm = 240

class JitterFeatureParam:
    def __init__(self, window):
        self.window = window

    @staticmethod
    def get_default_param():
        return JitterFeatureParam(
            default_window)

    @staticmethod
    def get_default_param_longterm():
        return JitterFeatureParam(
            default_window_longterm)

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])


@njit
def get_changes_1dim(values):
    '''
    values is a 1 dimensional array.
    '''
    l = values.shape[0]
    cols = values.shape[1]

    '''
    if len(values_argg.shape) == 2:
        values = np.array([v[0] for v in values_argg])
    else:
        values = values_argg
    '''

    ch_max = np.zeros(cols)
    ch_min = np.zeros(cols)
    distance_max_ch = np.ones(cols)
    distance_min_ch = np.ones(cols)
    ch_since_max, ch_since_min = np.zeros(cols), np.zeros(cols)

    first_v, last_v = values[0], values[-1]
    max_v, min_v = values[0], values[0]
    sum_v = np.zeros(cols)
    avg_v_before_max_ch, avg_v_before_min_ch = np.zeros(cols), np.zeros(cols)
    v_ch_max_is_to, v_ch_min_is_to = max_v, min_v
    v_ch_max_is_from, v_ch_min_is_from = max_v, min_v

    for i, v in enumerate(values):
        min_v, max_v = np.minimum(min_v, v), np.maximum(max_v, v)
        sum_v += v
        avg_v = sum_v * 1.0 / (i + 1)

        ch_jump = algo.feature.util.jitter_common.get_ch(min_v, v)
        ch_drop = algo.feature.util.jitter_common.get_ch(max_v, v)

        ch = algo.feature.util.jitter_common.get_ch(first_v, v)
        ch_since = algo.feature.util.jitter_common.get_ch(v, last_v)

        d = l - 1 - i

        where_max_to_be_updated = ch_max <= ch_jump
        distance_max_ch = np.where(where_max_to_be_updated, d, distance_max_ch)
        ch_since_max = np.where(where_max_to_be_updated, ch_since, ch_since_max)
        ch_max = np.where(where_max_to_be_updated, ch_jump, ch_max)
        v_ch_max_is_from = np.where(where_max_to_be_updated, min_v, v_ch_max_is_from)
        v_ch_max_is_to = np.where(where_max_to_be_updated, v, v_ch_max_is_to)
        avg_v_before_max_ch = np.where(where_max_to_be_updated, avg_v, avg_v_before_max_ch)

        where_min_to_be_updated = ch_min >= ch_drop
        distance_min_ch = np.where(where_min_to_be_updated, d, distance_max_ch)
        ch_since_min = np.where(where_min_to_be_updated, ch_since, ch_since_max)
        ch_min = np.where(where_min_to_be_updated, ch_drop, ch_max)
        v_ch_min_is_from = np.where(where_min_to_be_updated, max_v, v_ch_max_is_from)
        v_ch_min_is_to = np.where(where_min_to_be_updated, v, v_ch_max_is_to)
        avg_v_before_min_ch = np.where(where_min_to_be_updated, avg_v, avg_v_before_max_ch)

    return {
        'value': last_v,
        'ch': algo.feature.util.jitter_common.get_ch(first_v, last_v),
        'ch_max': ch_max, 'ch_min': ch_min,
        'avg_v_before_max_ch': avg_v_before_max_ch,
        'avg_v_before_min_ch': avg_v_before_min_ch,
        'v_ch_max_is_from': v_ch_max_is_from, 'v_ch_min_is_from': v_ch_min_is_from,
        'v_ch_max_is_to': v_ch_max_is_to, 'v_ch_min_is_to': v_ch_min_is_to,
        'ch_since_max': ch_since_max, 'ch_since_min': ch_since_min,
        'distance_max_ch': distance_max_ch, 'distance_min_ch': distance_min_ch,
    }


def get_feature_df(dfs, feature_param, value_column='close'):
    window = feature_param.window
    rows = [get_changes_1dim(np.array([v[0] for v in df_.to_numpy(dtype=np.float64)], dtype=np.float64)) for df_ in
         dfs[[value_column]].rolling(window, min_periods=window)]
    return algo.feature.util.jitter_common.rows_to_dataframe(rows, dfs.index)