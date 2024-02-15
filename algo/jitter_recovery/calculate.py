import pandas as pd, numpy as np
from collections import defaultdict
import numba
from numba import njit
from numba.experimental import jitclass


default_window = 60
default_window_longterm = 240

default_jump_threshold, default_drop_from_jump_threshold, default_exit_jumpt_threshold = 0.20, -0.04, 0.02
default_jump_threshold_longterm, default_drop_from_jump_threshold_longterm, default_exit_jumpt_threshold_longterm = 0.40, -0.10, 0.05


feature_param_spec = [
    ('window', numba.int32),
    ]

@jitclass(spec=feature_param_spec)
class JitterRecoveryFeatureParam:
    def __init__(self, window):
        self.window = window

    @staticmethod
    def get_default_param():
        return JitterRecoveryFeatureParam(
            default_window)

    @staticmethod
    def get_default_param_longterm():
        return JitterRecoveryFeatureParam(
            default_window_longterm)


trading_param_spec = [
    ('feature_param', numba.typeof(JitterRecoveryFeatureParam(20))),
    ('jump_threshold', numba.float64),
    ('drop_from_jump_threshold', numba.float64),
    ('exit_jumpt_threshold', numba.float64),
    ('is_long_term', numba.boolean),
    ]

class JitterRecoveryTradingParam:
    def __init__(self, feature_param, jump_threshold, drop_from_jump_threshold, exit_jumpt_threshold, is_long_term):
        self.feature_param = feature_param
        self.jump_threshold= jump_threshold
        self.drop_from_jump_threshold = drop_from_jump_threshold
        self.exit_jumpt_threshold = exit_jumpt_threshold
        self.is_long_term = is_long_term

    @staticmethod
    def get_default_param():
        return JitterRecoveryTradingParam(
            JitterRecoveryFeatureParam.get_default_param(), default_jump_threshold, default_drop_from_jump_threshold, default_exit_jumpt_threshold, is_long_term = False)

    @staticmethod
    def get_default_param_longterm():
        return JitterRecoveryTradingParam(
            JitterRecoveryFeatureParam.get_default_param_longterm(), default_jump_threshold_longterm, default_drop_from_jump_threshold_longterm, default_exit_jumpt_threshold_longterm, is_long_term = True)

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])

@njit
def _get_ch(v1: float, v2: float) -> float:
    if v1 == 0:
        return 0
    return (v2 - v1) / v1

@njit
def get_changes_1dim(values):
    '''
    values is a 1 dimensional array.
    '''
    l = values.shape[0]
    if l < 1: return None

    '''
    if len(values_argg.shape) == 2:
        values = np.array([v[0] for v in values_argg])
    else:
        values = values_argg
    '''

    ch_max = 0
    ch_min = 0
    distance_max_ch = 1
    distance_min_ch = 1
    ch_since_max, ch_since_min = 0, 0

    first_v, last_v = values[0], values[-1]
    max_v, min_v = values[0], values[0]
    sum_v = 0
    avg_v_before_max_ch, avg_v_before_min_ch = 0, 0
    v_ch_max_is_to, v_ch_min_is_to = max_v, min_v
    v_ch_max_is_from, v_ch_min_is_from = max_v, min_v

    for i, v in enumerate(values):
        min_v, max_v = min(min_v, v), max(max_v, v)
        sum_v += v
        avg_v = sum_v * 1.0 / (i+1)

        ch_jump = _get_ch(min_v, v)
        ch_drop = _get_ch(max_v, v)
        
        ch = _get_ch(first_v, v)
        ch_since = _get_ch(v, last_v)

        d =  l-1-i

        if ch_max <= ch_jump:
            distance_max_ch, ch_since_max, ch_max = d, ch_since, ch_jump
            v_ch_max_is_from = min_v
            v_ch_max_is_to = v
            avg_v_before_max_ch = avg_v


        if ch_min >= ch_drop:
            distance_min_ch, ch_since_min, ch_min = d, ch_since, ch_drop
            v_ch_min_is_from = max_v
            v_ch_min_is_to = v
            avg_v_before_min_ch = avg_v


    return {
        'value': values[-1],
        'ch': _get_ch(values[0], values[-1]),
        'ch_max': ch_max, 'ch_min': ch_min,
        'avg_v_before_max_ch': avg_v_before_max_ch,
        'avg_v_before_min_ch': avg_v_before_min_ch,
        'v_ch_max_is_from': v_ch_max_is_from, 'v_ch_min_is_from': v_ch_min_is_from,
        'v_ch_max_is_to': v_ch_max_is_to, 'v_ch_min_is_to': v_ch_min_is_to,
        'ch_since_max': ch_since_max, 'ch_since_min': ch_since_min,
        'distance_max_ch': distance_max_ch, 'distance_min_ch': distance_min_ch,
        }


def get_feature_df(dfs, feature_param):
    window = feature_param.window
    return pd.DataFrame([get_changes_1dim(np.array([v[0] for v in df_.to_numpy(dtype=np.float64)], dtype=np.float64)) for df_ in dfs[['close']].rolling(window, min_periods=window)], index=dfs.index)

class Status:
    def __init__(self):
        self.reset()

    def reset(self):
        self.in_position = 0
        self.value_at_enter = 0
        self.lowest_since_enter = 0
        self.timedelta_since_position_enter = 0
        self.v_ch_max_is_to_when_enter, self.v_ch_min_is_to_when_enter = 0, 0
        self.v_ch_max_is_from_when_enter, self.v_ch_min_is_from_when_enter = 0, 0
        self.ch_from_enter = 0
        self.ch_from_lowest_since_enter = 0

    def __str__(self):    
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])

    def update(self, features, trading_param: JitterRecoveryTradingParam) -> None:
        value = features['value']
        if self.in_position == 1:
            if value < self.lowest_since_enter:
                self.lowest_since_enter = value

            self.timedelta_since_position_enter += 1
            self.ch_from_enter = _get_ch(self.value_at_enter, value)
            self.ch_from_lowest_since_enter = _get_ch(self.lowest_since_enter, value)

            if not trading_param.is_long_term:
                if self.ch_from_lowest_since_enter > trading_param.exit_jumpt_threshold:
                    self.in_position = 0
            else:
                if self.ch_from_lowest_since_enter > trading_param.jump_threshold / 4.0 \
                    and self.timedelta_since_position_enter >= 5:
                    self.in_position = 0

                if self.ch_from_enter > trading_param.exit_jumpt_threshold:
                    self.in_position = 0

                if value < (self.v_ch_max_is_to_when_enter - self.v_ch_max_is_from_when_enter) / 3.0 + self.v_ch_max_is_from_when_enter:
                    self.in_position = 0
        else:
            should_enter_position = False

            if not trading_param.is_long_term:
                should_enter_position = features['ch_max'] > trading_param.jump_threshold \
                and features['ch_since_max'] < trading_param.drop_from_jump_threshold \
                and features['distance_max_ch'] < 10 \
                and features['distance_max_ch'] > 2

                #should_enter_position = should_enter_position and features['ch_max_collective'] > 0.05

            else:
                should_enter_position = features['ch_max'] > trading_param.jump_threshold \
                and features['ch_since_max'] < trading_param.drop_from_jump_threshold \
                and features['distance_max_ch'] < 60 \
                and features['distance_max_ch'] > 2

            if should_enter_position:
                self.in_position = 1
                self.value_at_enter = value
                self.lowest_since_enter = value
                self.timedelta_since_position_enter = 0
                self.v_ch_max_is_to_when_enter = features['v_ch_max_is_to']
                self.v_ch_min_is_to_when_enter = features['v_ch_min_is_to']
                self.v_ch_max_is_from_when_enter = features['v_ch_max_is_from']
                self.v_ch_min_is_from_when_enter = features['v_ch_min_is_from']
                self.ch_from_enter = 0
                self.ch_from_lowest_since_enter = 0
            else:
                self.reset()



def status_as_dict(status):
    return {
        'in_position': status.in_position,
        'value_at_enter': status.value_at_enter,
        'lowest_since_enter': status.lowest_since_enter,
        'timedelta_since_position_enter': status.timedelta_since_position_enter,
        'v_ch_max_is_to_when_enter': status.v_ch_max_is_to_when_enter,
        'v_ch_min_is_to_when_enter': status.v_ch_min_is_to_when_enter,
        'v_ch_max_is_from_when_enter': status.v_ch_max_is_from_when_enter,
        'v_ch_min_is_from_when_enter': status.v_ch_min_is_from_when_enter,
        'ch_from_enter': status.ch_from_enter,
        'ch_from_lowest_since_enter': status.ch_from_lowest_since_enter,
    }


def status_as_df(status):
    return pd.DataFrame({k: [v] for k, v in status_as_dict(status).items()})
