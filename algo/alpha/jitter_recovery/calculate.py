import numpy as np
import pandas as pd

import algo.feature.jitter.calculate
import algo.feature.util.jitter_common
from algo.feature.jitter.calculate import JitterFeatureParam

default_jump_threshold, default_drop_from_jump_threshold, default_exit_jumpt_threshold = 0.20, -0.04, 0.02
default_jump_threshold_longterm, default_drop_from_jump_threshold_longterm, default_exit_jumpt_threshold_longterm = 0.40, -0.10, 0.05


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
            JitterFeatureParam.get_default_param(), default_jump_threshold, default_drop_from_jump_threshold, default_exit_jumpt_threshold, is_long_term = False)

    @staticmethod
    def get_default_param_longterm():
        return JitterRecoveryTradingParam(
            JitterFeatureParam.get_default_param_longterm(), default_jump_threshold_longterm, default_drop_from_jump_threshold_longterm, default_exit_jumpt_threshold_longterm, is_long_term = True)

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])

class Status:
    def __init__(self, cols):
        self.cols = cols
        self.reset(self.cols)

    def reset(self, cols):
        self.in_position = np.zeros(cols)
        self.value_at_enter = np.zeros(cols)
        self.lowest_since_enter = np.zeros(cols)
        self.highest_since_enter = np.zeros(cols)
        self.timedelta_since_position_enter = np.zeros(cols)
        self.v_ch_max_is_to_when_enter, self.v_ch_min_is_to_when_enter = np.zeros(cols), np.zeros(cols)
        self.v_ch_max_is_from_when_enter, self.v_ch_min_is_from_when_enter = np.zeros(cols), np.zeros(cols)
        self.ch_from_enter = np.zeros(cols)
        self.ch_from_lowest_since_enter = np.zeros(cols)
        self.ch_from_highest_since_enter = np.zeros(cols)

    def __str__(self):    
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])

    def update(self, features, trading_param: JitterRecoveryTradingParam) -> None:
        value = features['value']
        is_in_position = self.in_position != 0
        self.lowest_since_enter = np.where((is_in_position) & (value < self.lowest_since_enter), value, self.lowest_since_enter)
        self.highest_since_enter = np.where((is_in_position) & (value > self.highest_since_enter), value, self.highest_since_enter)
        self.timedelta_since_position_enter = np.where(is_in_position, self.timedelta_since_position_enter + 1, self.timedelta_since_position_enter)
        self.ch_from_enter = np.where(is_in_position, algo.feature.util.jitter_common.get_ch(self.value_at_enter, value), self.ch_from_enter)
        self.ch_from_lowest_since_enter = np.where(is_in_position, algo.feature.util.jitter_common.get_ch(self.lowest_since_enter, value), self.ch_from_lowest_since_enter)
        self.ch_from_highest_since_enter = np.where(is_in_position, algo.feature.util.jitter_common.get_ch(self.highest_since_enter, value), self.ch_from_highest_since_enter)

        is_pos_1_position = self.in_position == 1
        self.in_position = np.where((is_pos_1_position) & ~trading_param.is_long_term & \
                                    (self.ch_from_highest_since_enter < -abs(trading_param.exit_jumpt_threshold)), 0, self.in_position)
        self.in_position = np.where((is_pos_1_position) & trading_param.is_long_term & \
                                    (self.ch_from_highest_since_enter < -abs(trading_param.exit_jumpt_threshold)) & \
                                    (self.timedelta_since_position_enter >= 5), 0, self.in_position)

        is_neg_1_position = self.in_position == -1
        self.in_position = np.where((is_neg_1_position) & ~trading_param.is_long_term & \
                                    (self.ch_from_lowest_since_enter > abs(trading_param.exit_jumpt_threshold)), 0, self.in_position)
        self.in_position = np.where((is_neg_1_position) & trading_param.is_long_term & \
                                    (self.ch_from_lowest_since_enter > abs(trading_param.exit_jumpt_threshold)) & \
                                    (self.timedelta_since_position_enter >= 5), 0, self.in_position)


        if self.in_position != 0:
            if value < self.lowest_since_enter:
                self.lowest_since_enter = value
            if value > self.highest_since_enter:
                self.highest_since_enter = value

            self.timedelta_since_position_enter += 1
            self.ch_from_enter = algo.feature.util.jitter_common.get_ch(self.value_at_enter, value)
            self.ch_from_lowest_since_enter = algo.feature.util.jitter_common.get_ch(self.lowest_since_enter, value)
            self.ch_from_highest_since_enter = algo.feature.util.jitter_common.get_ch(self.highest_since_enter, value)

            if self.in_position == 1:
                if not trading_param.is_long_term:
                    if self.ch_from_highest_since_enter > trading_param.exit_jumpt_threshold:
                        self.in_position = 0
                else:
                    if self.ch_from_highest_since_enter < -abs(trading_param.exit_jumpt_threshold) \
                        and self.timedelta_since_position_enter >= 5:
                        self.in_position = 0

                    #if value < (self.v_ch_max_is_to_when_enter - self.v_ch_max_is_from_when_enter) / 3.0 + self.v_ch_max_is_from_when_enter:
                    #    self.in_position = 0

            elif self.in_position == -1:
                if not trading_param.is_long_term:
                    if self.ch_from_lowest_since_enter < trading_param.exit_jumpt_threshold:
                        self.in_position = 0
                else:
                    if self.ch_from_lowest_since_enter > abs(trading_param.exit_jumpt_threshold) \
                        and self.timedelta_since_position_enter >= 5:
                        self.in_position = 0

        else:
            new_position = 0

            if trading_param.is_long_term:
                if features['ch_max'] > trading_param.jump_threshold \
                and features['ch_since_max'] < trading_param.drop_from_jump_threshold \
                and features['distance_max_ch'] < 10 \
                and features['distance_max_ch'] > 2:
                    new_position = 1

                #should_enter_position = should_enter_position and features['ch_max_collective'] > 0.05

            else:
                if features['ch_max'] > abs(trading_param.jump_threshold) \
                and features['ch_since_max'] < -abs(trading_param.drop_from_jump_threshold) \
                and features['distance_max_ch'] < 60 \
                and features['distance_max_ch'] > 2:
                    new_position = -1

                if features['ch_min'] < -abs(trading_param.jump_threshold) \
                and features['ch_since_min'] > abs(trading_param.drop_from_jump_threshold) \
                and features['distance_min_ch'] < 60 \
                and features['distance_min_ch'] > 2:
                    new_position = 1

            if new_position != 0:
                self.in_position = new_position
                self.value_at_enter = value
                self.lowest_since_enter = value
                self.highest_since_enter = value
                self.timedelta_since_position_enter = 0
                self.v_ch_max_is_to_when_enter = features['v_ch_max_is_to']
                self.v_ch_min_is_to_when_enter = features['v_ch_min_is_to']
                self.v_ch_max_is_from_when_enter = features['v_ch_max_is_from']
                self.v_ch_min_is_from_when_enter = features['v_ch_min_is_from']
                self.ch_from_enter = 0
                self.ch_from_lowest_since_enter = 0
            else:
                self.reset(self.cols)



def status_as_dict(status):
    return {
        'in_position': status.in_position,
        'value_at_enter': status.value_at_enter,
        'lowest_since_enter': status.lowest_since_enter,
        'highest_since_enter': status.highest_since_enter,
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
