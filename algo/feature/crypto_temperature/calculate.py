import datetime

import pandas as pd, numpy as np
from collections import defaultdict
from collections import deque
import algo.feature.util.jitter_common

default_symbols = ("BTC-USDT-SWAP", "ETH-USDT-SWAP")
default_window_minutes = 60 * 24


class CryptoTemperatureFeatureParam:
    def __init__(self, symbols, window_minutes):
        self.symbols = symbols
        self.window_minutes = window_minutes

    @staticmethod
    def get_default_param():
        return CryptoTemperatureFeatureParam(default_symbols, default_window_minutes)

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])


def get_feature_df(dfs, feature_param: CryptoTemperatureFeatureParam, value_column='close'):
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

        ch = algo.feature.util.jitter_common.get_ch(input_window_rows[0][1], input_window_rows[-1][1])
        values = {
            'ch': ch,
        }
        rows.append(values)

    return algo.feature.util.jitter_common.rows_to_dataframe(rows, dfs.index)

