import pandas as pd, numpy as np
from collections import defaultdict
import numba
from numba import njit
from numba.experimental import jitclass

default_window = 40


class MomentumFeatureParam:
    def __init__(self, window: int):
        self.window = window

    @staticmethod
    def get_default_param():
        return MomentumFeatureParam(default_window)

    def __str__(self):
        return ', '.join([f'{k}: {str(v)}' for k, v in vars(self).items()])


@njit
def _get_ch(v1: float, v2: float) -> float:
    if v1 == 0:
        return 0
    return (v2 - v1) / v1


@njit
def get_momentum_1dim(values):
    '''
    values is a 1 dimensional array.
    '''
    l = values.shape[0]
    if l < 1: return None

    return {
        'value': values[-1],
        'ch': _get_ch(values[0], values[-1]),
    }


def get_feature_df(dfs, feature_param, value_column='close'):
    window = feature_param.window
    return pd.DataFrame(
        [get_momentum_1dim(np.array([v[0] for v in df_.to_numpy(dtype=np.float64)], dtype=np.float64)) for df_ in
         dfs[[value_column]].rolling(window, min_periods=window)], index=dfs.index)
