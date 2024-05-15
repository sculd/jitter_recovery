import pandas as pd, numpy as np
from collections import defaultdict
import numba
from numba import njit
from numba.experimental import jitclass

import algo.jitter_common.calculate
from algo.jitter_common.calculate import JitterFeatureParam

default_jump_threshold, default_exit_drop_threshold = 0.10, -0.03


class JitterFollowingTradingParam:
    def __init__(self, feature_param, jump_threshold, exit_drop_threshold):
        self.feature_param = feature_param
        self.jump_threshold= jump_threshold
        self.exit_drop_threshold = exit_drop_threshold

    @staticmethod
    def get_default_param():
        return JitterFollowingTradingParam(
            JitterFeatureParam.get_default_param(), default_jump_threshold, default_exit_drop_threshold)

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])


class Status:
    def __init__(self):
        self.reset()

    def reset(self):
        self.in_position = 0
        self.value_at_enter = 0
        self.lowest_since_enter = 0
        self.highest_since_enter = 0
        self.timedelta_since_position_enter = 0
        self.v_ch_max_is_to_when_enter, self.v_ch_min_is_to_when_enter = 0, 0
        self.v_ch_max_is_from_when_enter, self.v_ch_min_is_from_when_enter = 0, 0
        self.ch_from_enter = 0
        self.ch_from_lowest_since_enter = 0

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])

    def update(self, features, trading_param: JitterFollowingTradingParam) -> None:
        value = features['value']
        if self.in_position == 1:
            if value < self.lowest_since_enter:
                self.lowest_since_enter = value

            if value > self.highest_since_enter:
                self.highest_since_enter = value

            self.timedelta_since_position_enter += 1
            self.ch_from_enter = algo.jitter_common.calculate._get_ch(self.value_at_enter, value)
            self.ch_from_lowest_since_enter = algo.jitter_common.calculate._get_ch(self.lowest_since_enter, value)
            self.ch_from_highest_since_enter = algo.jitter_common.calculate._get_ch(self.highest_since_enter, value)

            if self.ch_from_highest_since_enter < trading_param.exit_drop_threshold \
                and self.timedelta_since_position_enter >= 5:
                self.in_position = 0

            if self.ch_from_enter < trading_param.exit_drop_threshold:
                self.in_position = 0

            if value < (self.v_ch_max_is_to_when_enter - self.v_ch_max_is_from_when_enter) / 3.0 + self.v_ch_max_is_from_when_enter:
                self.in_position = 0
        else:
            should_enter_position = features['ch_max'] > trading_param.jump_threshold \
            and features['distance_max_ch'] < 2

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
