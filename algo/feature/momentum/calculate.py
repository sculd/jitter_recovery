import pandas as pd, numpy as np
from collections import defaultdict
import numba
from numba import njit
from numba.experimental import jitclass

default_window = 180
default_ema_window = 60


class MomentumFeatureParam:
    def __init__(self, window: int, ema_window: int):
        self.window = window
        self.ema_window = ema_window

    @staticmethod
    def get_default_param():
        return MomentumFeatureParam(default_window, default_ema_window)

    def __str__(self):
        return ', '.join([f'{k}: {str(v)}' for k, v in vars(self).items()])


@njit
def _get_ch(v1: float, v2: float) -> float:
    if v1 == 0:
        return 0
    return (v2 - v1) / v1

def _get_ema(values, window=30):
    ret = np.empty((values.shape[0]))
    ret.fill(np.nan)
    e = values[0]
    alpha = 2 / float(window + 1)
    for s in range(values.shape[0]):
        e =  ((values[s]-e) *alpha ) + e
        ret[s] = e
    return ret

def _largest_change(values):
    '''
    return the change of the largest amplitude from certain past to the last.
    '''
    l = values.shape[0]
    if l < 1: return 0
    last = values[-1]
    largest = _get_ch(values[0], last)
    for v in values:
        ch = _get_ch(v, last)
        if abs(ch) > abs(largest):
            largest = ch
    return largest

def get_momentum_1dim(values, ema_window):
    '''
    values is a 1 dimensional array.
    '''
    l = values.shape[0]
    if l < 1: return None

    ewms = _get_ema(values, window=ema_window)
    return {
        'value': values[-1],
        'ewm': ewms[-1],
        'ch': _get_ch(values[0], values[-1]),
        'ch_ewms': _get_ch(ewms[0], ewms[-1]),
        #'ch': _largest_change(values),
        #'ch_ewms': _largest_change(ewms),
    }


def get_feature_df(dfs, feature_param: MomentumFeatureParam, value_column='close'):
    window = feature_param.window
    return pd.DataFrame(
        [get_momentum_1dim(np.array([v[0] for v in df_.to_numpy(dtype=np.float64)], dtype=np.float64), feature_param.ema_window) for df_ in
         dfs[[value_column]].rolling(window, min_periods=window)], index=dfs.index)
