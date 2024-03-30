import pandas as pd, numpy as np
from collections import defaultdict
from algo.jitter_recovery.calculate import Status as BasicStatus


default_window = 40
default_collective_window = 40

default_collective_drop_threshold = -0.10
default_collective_drop_lower_threshold = -0.40
default_drop_threshold, default_jump_from_drop_threshold, default_exit_drop_threshold = -0.15, +0.04, -0.02

default_collective_jump_threshold = 0.10
default_collective_jump_lower_threshold = 0.40
default_jump_threshold, default_drop_from_jump_threshold, default_exit_jump_threshold = +0.15, -0.04, +0.02

default_collective_small_drop_threshold = -0.03
default_collective_small_drop_lower_threshold = -0.15
default_small_drop_threshold, default_jump_from_small_drop_threshold, default_exit_small_drop_threshold = -0.03, +0.005, -0.01

default_collective_small_jump_threshold = +0.03
default_collective_small_jump_lower_threshold = +0.15
default_small_jump_threshold, default_drop_from_small_jump_threshold, default_exit_small_jump_threshold = +0.03, -0.005, +0.01


class CollectiveRecoveryFeatureParam:
    def __init__(self, window: int, collective_window: int):
        self.window = window
        self.collective_window = collective_window

    @staticmethod
    def get_default_param():
        return CollectiveRecoveryFeatureParam(
            default_window, default_collective_window)

    def as_label(self):
        return '_'.join([f'{k}{v}' for k, v in vars(self).items()])

    def __str__(self):
        return ', '.join([f'{k}: {str(v)}' for k, v in vars(self).items()])


class CollectiveDropRecoveryTradingParam:
    def __init__(self, collective_drop_threshold, collective_drop_lower_threshold, drop_threshold, jump_from_drop_threshold, exit_drop_threshold):
        self.collective_drop_threshold = collective_drop_threshold
        self.collective_drop_lower_threshold = collective_drop_lower_threshold
        self.drop_threshold = drop_threshold
        self.jump_from_drop_threshold = jump_from_drop_threshold
        self.exit_drop_threshold = exit_drop_threshold

    @staticmethod
    def get_default_param():
        return CollectiveDropRecoveryTradingParam(
            collective_drop_threshold = default_collective_drop_threshold,
            collective_drop_lower_threshold = default_collective_drop_lower_threshold,
            drop_threshold = default_drop_threshold,
            jump_from_drop_threshold = default_jump_from_drop_threshold,
            exit_drop_threshold  = default_exit_drop_threshold,
            )

    @staticmethod
    def get_default_param_small_drop():
        return CollectiveDropRecoveryTradingParam(
            collective_drop_threshold = default_collective_small_drop_threshold,
            collective_drop_lower_threshold = default_collective_small_drop_lower_threshold,
            drop_threshold = default_small_drop_threshold,
            jump_from_drop_threshold = default_jump_from_small_drop_threshold,
            exit_drop_threshold  = default_exit_small_drop_threshold,
            )

    def __str__(self):
        return ', '.join([f'{k}: {str(v)}' for k, v in vars(self).items()])


class CollectiveJumpRecoveryTradingParam:
    def __init__(self, collective_jump_threshold, collective_jump_lower_threshold, jump_threshold, drop_from_jump_threshold, exit_jump_threshold):
        self.collective_jump_threshold = collective_jump_threshold
        self.collective_jump_lower_threshold = collective_jump_lower_threshold
        self.jump_threshold = jump_threshold
        self.drop_from_jump_threshold = drop_from_jump_threshold
        self.exit_jump_threshold = exit_jump_threshold

    @staticmethod
    def get_default_param():
        return CollectiveJumpRecoveryTradingParam(
            collective_jump_threshold = default_collective_jump_threshold,
            collective_jump_lower_threshold = default_collective_jump_lower_threshold,
            jump_threshold = default_jump_threshold,
            drop_from_jump_threshold = default_drop_from_jump_threshold,
            exit_jump_threshold  = default_exit_jump_threshold,
            )

    @staticmethod
    def get_default_param_small_jump():
        return CollectiveJumpRecoveryTradingParam(
            collective_jump_threshold = default_collective_small_jump_threshold,
            collective_jump_lower_threshold = default_collective_small_jump_lower_threshold,
            jump_threshold = default_small_jump_threshold,
            drop_from_jump_threshold = default_drop_from_small_jump_threshold,
            exit_jump_threshold  = default_exit_small_jump_threshold,
            )

    def __str__(self):
        return ', '.join([f'{k}: {str(v)}' for k, v in vars(self).items()])


class CollectiveRecoveryTradingParam:
    def __init__(self, feature_param, collective_drop_recovery_trading_param, collective_jump_recovery_trading_param):
        self.feature_param = feature_param
        self.collective_drop_recovery_trading_param = collective_drop_recovery_trading_param
        self.collective_jump_recovery_trading_param = collective_jump_recovery_trading_param

    @staticmethod
    def get_default_param():
        return CollectiveRecoveryTradingParam(
            CollectiveRecoveryFeatureParam.get_default_param(), 
            CollectiveDropRecoveryTradingParam.get_default_param(), 
            CollectiveJumpRecoveryTradingParam.get_default_param(), 
            )

    @staticmethod
    def get_default_param_small_move():
        return CollectiveRecoveryTradingParam(
            CollectiveRecoveryFeatureParam.get_default_param(), 
            CollectiveDropRecoveryTradingParam.get_default_param_small_drop(), 
            CollectiveJumpRecoveryTradingParam.get_default_param_small_jump(), 
            )

    @staticmethod
    def get_default_param_small_drop():
        return CollectiveRecoveryTradingParam(
            CollectiveRecoveryFeatureParam.get_default_param(),
            CollectiveDropRecoveryTradingParam.get_default_param_small_drop(),
            collective_jump_recovery_trading_param=None,
            )

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
        self.ch_from_highest_since_enter = 0

    def update(self, features, trading_param: CollectiveRecoveryTradingParam) -> None:
        value = features['value']
        if self.in_position != 0:
            if value < self.lowest_since_enter:
                self.lowest_since_enter = value

            if value > self.highest_since_enter:
                self.highest_since_enter = value

            self.timedelta_since_position_enter += 1
            self.ch_from_enter = _get_ch(self.value_at_enter, value)
            self.ch_from_lowest_since_enter = _get_ch(self.lowest_since_enter, value)
            self.ch_from_highest_since_enter = _get_ch(self.highest_since_enter, value)

            if self.in_position == 1:
                if self.ch_from_highest_since_enter < trading_param.collective_drop_recovery_trading_param.exit_drop_threshold \
                    and self.timedelta_since_position_enter >= 5:
                    self.in_position = 0

                if self.ch_from_enter < trading_param.collective_drop_recovery_trading_param.exit_drop_threshold:
                    self.in_position = 0

                if value > self.v_ch_min_is_from_when_enter - (self.v_ch_min_is_from_when_enter - self.v_ch_min_is_to_when_enter) / 3.0:
                    self.in_position = 0

            elif self.in_position == -1:
                if self.ch_from_lowest_since_enter > trading_param.collective_jump_recovery_trading_param.exit_jump_threshold \
                    and self.timedelta_since_position_enter >= 5:
                    self.in_position = 0

                if self.ch_from_enter > trading_param.collective_jump_recovery_trading_param.exit_jump_threshold:
                    self.in_position = 0

                if value < self.v_ch_max_is_from_when_enter + (self.v_ch_max_is_to_when_enter - self.v_ch_max_is_from_when_enter) / 3.0:
                    self.in_position = 0
        else:
            should_enter_long_position = False
            if trading_param.collective_drop_recovery_trading_param is not None:
                should_enter_long_position = features['ch_window30_min_collective'] < trading_param.collective_drop_recovery_trading_param.collective_drop_threshold \
                    and features['ch_window30_min_collective'] > trading_param.collective_drop_recovery_trading_param.collective_drop_lower_threshold \
                    and features['ch_min'] < trading_param.collective_drop_recovery_trading_param.drop_threshold \
                    and features['ch_since_min'] > trading_param.collective_drop_recovery_trading_param.jump_from_drop_threshold \
                    and features['distance_min_ch'] < 20 \
                    and features['distance_min_ch'] > 2

            should_enter_short_position = False
            if trading_param.collective_jump_recovery_trading_param is not None:
                should_enter_short_position = features['ch_window30_min_collective'] > trading_param.collective_jump_recovery_trading_param.collective_jump_threshold \
                    and features['ch_window30_min_collective'] < trading_param.collective_jump_recovery_trading_param.collective_jump_lower_threshold \
                    and features['ch_max'] > trading_param.collective_jump_recovery_trading_param.jump_threshold \
                    and features['ch_since_max'] < trading_param.collective_jump_recovery_trading_param.drop_from_jump_threshold \
                    and features['distance_max_ch'] < 20 \
                    and features['distance_max_ch'] > 2
                
            if should_enter_long_position or should_enter_short_position:
                self.in_position =  1 if should_enter_long_position else -1
                self.value_at_enter = value
                self.lowest_since_enter = value
                self.highest_since_enter = value
                self.ch_from_lowest_since_enter = 0
                self.ch_from_highest_since_enter = 0
                self.timedelta_since_position_enter = 0
                self.v_ch_max_is_to_when_enter = features['v_ch_max_is_to']
                self.v_ch_min_is_to_when_enter = features['v_ch_min_is_to']
                self.v_ch_max_is_from_when_enter = features['v_ch_max_is_from']
                self.v_ch_min_is_from_when_enter = features['v_ch_min_is_from']
                self.ch_from_enter = 0
            else:
                self.reset()

    def __str__(self):
        return ', '.join([f'{k}: {str(v)}' for k, v in vars(self).items()])



def status_as_dict(status):
    return {
        'in_position': status.in_position,
        'value_at_enter': status.value_at_enter,
        'lowest_since_enter': status.lowest_since_enter,
        'highest_since_enter': status.highest_since_enter,
        'ch_from_lowest_since_enter': status.ch_from_lowest_since_enter,
        'ch_from_highest_since_enter': status.ch_from_highest_since_enter,
        'timedelta_since_position_enter': status.timedelta_since_position_enter,
        'v_ch_max_is_to_when_enter': status.v_ch_max_is_to_when_enter,
        'v_ch_min_is_to_when_enter': status.v_ch_min_is_to_when_enter,
        'v_ch_max_is_from_when_enter': status.v_ch_max_is_from_when_enter,
        'v_ch_min_is_from_when_enter': status.v_ch_min_is_from_when_enter,
        'ch_from_enter': status.ch_from_enter,
    }


def status_as_df(status):
    return pd.DataFrame({k: [v] for k, v in status_as_dict(status).items()})
