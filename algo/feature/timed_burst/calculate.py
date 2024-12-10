import datetime

import pandas as pd, numpy as np
from collections import deque
import algo.feature.util.jitter_common
import numba
from numba import njit

default_window_minutes = 5

class TimedJitterFeatureParam:
    def __init__(self, window_minutes):
        self.window_minutes = window_minutes

    @staticmethod
    def get_default_param():
        return TimedJitterFeatureParam(default_window_minutes)

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])


@njit
def get_feature_for_window(values):
    '''
    values is a 1 dimensional array.
    '''
    l = values.shape[0]

    ch_max = 0
    ch_min = 0
    distance_max_ch = 1
    distance_min_ch = 1

    first_v, last_v = values[0], values[-1]
    max_v, min_v = first_v, first_v
    sum_v = 0
    avg_v_before_max_ch, avg_v_before_min_ch = 0, 0
    v_ch_max_is_to, v_ch_min_is_to = max_v, min_v
    v_ch_max_is_from, v_ch_min_is_from = max_v, min_v

    for i, v in enumerate(values):
        min_v, max_v = min(min_v, v), max(max_v, v)
        sum_v += v
        avg_v = sum_v * 1.0 / (i + 1)

        ch_jump = algo.feature.util.jitter_common.get_ch(min_v, last_v)
        ch_drop = algo.feature.util.jitter_common.get_ch(max_v, last_v)

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
    first_v_smoothed = sum(values[-smooth_window_half:smooth_window - smooth_window_half]) / smooth_window
    expected_v = first_v_smoothed + (first_v_smoothed - past_v_smoothed)

    return {
        'value': values[-1],
        'ch': algo.feature.util.jitter_common.get_ch(values[0], values[-1]),
        'ch_max': ch_max, 'ch_min': ch_min,
        'avg_v_before_max_ch': avg_v_before_max_ch,
        'avg_v_before_min_ch': avg_v_before_min_ch,
        'v_ch_max_is_from': v_ch_max_is_from, 'v_ch_min_is_from': v_ch_min_is_from,
        'v_ch_max_is_to': v_ch_max_is_to, 'v_ch_min_is_to': v_ch_min_is_to,
        'distance_max_ch': distance_max_ch, 'distance_min_ch': distance_min_ch,
        'expected_v': expected_v,
    }


def get_feature_df(dfs, feature_param: TimedJitterFeatureParam, value_column='close'):
    rows = []
    minimum_input_window_size = min(10, feature_param.window_minutes)
    input_window_rows = deque()
    for timestamp, row in dfs.iterrows():
        input_window_rows.append((timestamp, row[value_column],))
        last_timestamp = input_window_rows[-1][0]
        while last_timestamp - input_window_rows[0][0] > datetime.timedelta(minutes=feature_param.window_minutes):
            input_window_rows.popleft()

        if len(input_window_rows) < minimum_input_window_size:
            rows.append(None)
            continue

        rows.append(get_feature_for_window(np.array([p[1] for p in input_window_rows])))

    return algo.feature.util.jitter_common.rows_to_dataframe(rows, dfs.index)

