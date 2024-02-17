import pandas as pd, numpy as np
from collections import defaultdict
from algo.jitter_recovery.calculate import Status as BasicStatus


default_window = 40

default_drop_threshold, default_jump_from_drop_threshold, default_exit_drop_threshold = -0.15, +0.04, -0.02


class CollectiveRecoveryFeatureParam:
    def __init__(self, window):
        self.window = window

    @staticmethod
    def get_default_param():
        return CollectiveRecoveryFeatureParam(
            default_window)

    def __str__(self):
        return ', '.join([f'{k}: {str(v)}' for k, v in vars(self).items()])


class CollectiveRecoveryTradingParam:
    def __init__(self, feature_param, drop_threshold, jump_from_drop_threshold, exit_drop_threshold):
        self.feature_param = feature_param
        self.drop_threshold = drop_threshold
        self.jump_from_drop_threshold = jump_from_drop_threshold
        self.exit_drop_threshold = exit_drop_threshold

    @staticmethod
    def get_default_param():
        return CollectiveRecoveryTradingParam(
            CollectiveRecoveryFeatureParam.get_default_param(), default_drop_threshold, default_jump_from_drop_threshold, default_exit_drop_threshold)

    def __str__(self):
        return ', '.join([f'{k}: {str(v)}' for k, v in vars(self).items()])


def _get_ch(v1: float, v2: float) -> float:
    if v1 == 0:
        return 0
    return (v2 - v1) / v1


class Status(BasicStatus):
    def __init__(self):
        self.reset()

    def reset(self):
        super().reset()
        self.highest_since_enter = 0

    def update(self, collective_features, features, trading_param) -> None:
        value = features['value']
        if self.in_position == 1:
            if value < self.lowest_since_enter:
                self.lowest_since_enter = value

            if value > self.highest_since_enter:
                self.highest_since_enter = value

            self.timedelta_since_position_enter += 1
            self.ch_from_enter = _get_ch(self.value_at_enter, value)
            self.ch_from_highest_since_enter = _get_ch(self.highest_since_enter, value)

            if self.ch_from_highest_since_enter > trading_param.drop_threshold / 4.0 \
                and self.timedelta_since_position_enter >= 5:
                self.in_position = 0

            if self.ch_from_enter < trading_param.exit_drop_threshold:
                self.in_position = 0

            if value > (self.v_ch_min_is_to_when_enter - self.v_ch_min_is_from_when_enter) / 3.0 + self.v_ch_min_is_from_when_enter:
                self.in_position = 0
        else:
            should_enter_position = False

            should_enter_position = collective_features['ch_window30_min'] < -0.10 \
                and features['ch_min'] < trading_param.drop_threshold \
                and features['ch_since_min'] > trading_param.jump_from_drop_threshold \
                and features['distance_min_ch'] < 20 \
                and features['distance_min_ch'] > 2

            if should_enter_position:
                self.in_position = 1
                self.value_at_enter = value
                self.lowest_since_enter = value
                self.highest_since_enter = value #
                self.timedelta_since_position_enter = 0
                self.v_ch_max_is_to_when_enter = features['v_ch_max_is_to']
                self.v_ch_min_is_to_when_enter = features['v_ch_min_is_to']
                self.v_ch_max_is_from_when_enter = features['v_ch_max_is_from']
                self.v_ch_min_is_from_when_enter = features['v_ch_min_is_from']
                self.ch_from_enter = 0
                self.ch_from_lowest_since_enter = 0
                self.ch_from_highest_since_enter = 0
            else:
                self.reset()



def status_as_dict(status):
    return {
        'in_position': status.in_position,
        'value_at_enter': status.value_at_enter,
        # 'lowest_since_enter': status.lowest_since_enter,
        'highest_since_enter': status.highest_since_enter,
        'timedelta_since_position_enter': status.timedelta_since_position_enter,
        'v_ch_max_is_to_when_enter': status.v_ch_max_is_to_when_enter,
        'v_ch_min_is_to_when_enter': status.v_ch_min_is_to_when_enter,
        'v_ch_max_is_from_when_enter': status.v_ch_max_is_from_when_enter,
        'v_ch_min_is_from_when_enter': status.v_ch_min_is_from_when_enter,
        'ch_from_enter': status.ch_from_enter,
        #'ch_from_lowest_since_enter': status.ch_from_lowest_since_enter,
        'ch_from_highest_since_enter': status.ch_from_highest_since_enter,
    }


def status_as_df(status):
    return pd.DataFrame({k: [v] for k, v in status_as_dict(status).items()})
