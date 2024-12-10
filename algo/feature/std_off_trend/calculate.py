import datetime

import pandas as pd, numpy as np
from collections import defaultdict
from collections import deque
import algo.feature.util.jitter_common

default_window_minutes = 60 * 1


class StdOffTrendFeatureParam:
    def __init__(self, window_minutes):
        self.window_minutes = window_minutes

    @staticmethod
    def get_default_param():
        return StdOffTrendFeatureParam(default_window_minutes)

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])


def get_std_off_trend(values):
    '''
    values is a 1 dimensional array.
    '''
    l = values.shape[0]

    first_v, last_v = values[0], values[-1]
    delta_v = last_v - first_v
    trend_vs = [first_v + delta_v * i / l for i in range(l)]

    dev_sum = 0
    for trend_v, v in zip(trend_vs, values):
        dev_sum += (v - trend_v) ** 2

    std_off_trend = np.sqrt(dev_sum / (l-1))
    return {
        'value': values[-1],
        'std_off_trend': std_off_trend,
        'std_off_trend_to_value': std_off_trend / values[-1],
    }


def get_feature_df(dfs, feature_param: StdOffTrendFeatureParam, value_column='close'):
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

        vs = [p[1] for p in input_window_rows]
        feature = get_std_off_trend(np.array(vs))
        rows.append(feature)

    return algo.feature.util.jitter_common.rows_to_dataframe(rows, dfs.index)

