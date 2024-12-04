import pandas as pd, numpy as np
from collections import defaultdict
import numba
from numba import njit

default_window = 30

class SimpleJitterFeatureParam:
    def __init__(self, window):
        self.window = window

    @staticmethod
    def get_default_param():
        return SimpleJitterFeatureParam(
            default_window)

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])

@njit
def _get_ch(v1: float, v2: float) -> float:
    if v1 == 0:
        return 0
    return (v2 - v1) / v1


@njit
def get_changes_1dim(values, window: int):
    '''
    values is a 1 dimensional array.
    '''
    l = values.shape[0]
    if l < window * 2:
        return None

    ch_max = 0
    ch_min = 0
    distance_max_ch = 1
    distance_min_ch = 1

    first_v, last_v = values[-window], values[-1]
    max_v, min_v = first_v, first_v
    sum_v = 0
    avg_v_before_max_ch, avg_v_before_min_ch = 0, 0
    v_ch_max_is_to, v_ch_min_is_to = max_v, min_v
    v_ch_max_is_from, v_ch_min_is_from = max_v, min_v

    for i, v in enumerate(values[-window:]):
        min_v, max_v = min(min_v, v), max(max_v, v)
        sum_v += v
        avg_v = sum_v * 1.0 / (i + 1)

        ch_jump = _get_ch(min_v, last_v)
        ch_drop = _get_ch(max_v, last_v)

        d = l - 1 - i

        if ch_max <= ch_jump:
            distance_max_ch, ch_max = d, ch_jump
            v_ch_max_is_from = min_v
            v_ch_max_is_to = last_v
            avg_v_before_max_ch = avg_v

        if ch_min >= ch_drop:
            distance_min_ch, ch_min = d, ch_drop
            v_ch_min_is_from = max_v
            v_ch_min_is_to = last_v
            avg_v_before_min_ch = avg_v

    smooth_window = 3
    smooth_window_half = smooth_window // 2
    past_v_smoothed = sum(values[:smooth_window]) / smooth_window
    first_v_smoothed = sum(values[-window - smooth_window_half:-window + smooth_window - smooth_window_half]) / smooth_window
    expected_v = first_v_smoothed + (first_v_smoothed - past_v_smoothed)

    return {
        'value': values[-1],
        'ch': _get_ch(values[0], values[-1]),
        'ch_max': ch_max, 'ch_min': ch_min,
        'avg_v_before_max_ch': avg_v_before_max_ch,
        'avg_v_before_min_ch': avg_v_before_min_ch,
        'v_ch_max_is_from': v_ch_max_is_from, 'v_ch_min_is_from': v_ch_min_is_from,
        'v_ch_max_is_to': v_ch_max_is_to, 'v_ch_min_is_to': v_ch_min_is_to,
        'distance_max_ch': distance_max_ch, 'distance_min_ch': distance_min_ch,
        'expected_v': expected_v,
    }


def get_feature_df(dfs, feature_param, value_column='close'):
    # twice the size to calculate extrapolation
    full_window = feature_param.window * 2
    rows = [get_changes_1dim(np.array([v[0] for v in df_.to_numpy(dtype=np.float64)], dtype=np.float64), feature_param.window) for df_ in
         dfs[[value_column]].rolling(full_window, min_periods=full_window)]
    null_row_vals = {}
    for r in rows:
        if r is None: continue
        null_row_vals = {k: None for k in r.keys()}
        break
    rows = [null_row_vals if r is None else r for r in rows]
    return pd.DataFrame(rows, index=dfs.index)
