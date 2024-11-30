import pandas as pd

import algo.feature.jitter.calculate
from algo.feature.jitter.calculate import JitterFeatureParam

default_jump_threshold, default_drop_from_jump_threshold = 0.18, -0.02


class JitterSimpleReversalTradingParam:
    def __init__(self, feature_param, jump_threshold, drop_from_jump_threshold):
        self.feature_param = feature_param
        self.jump_threshold= jump_threshold
        self.drop_from_jump_threshold = drop_from_jump_threshold

    @staticmethod
    def get_default_param():
        return JitterSimpleReversalTradingParam(
            JitterFeatureParam.get_default_param(), default_jump_threshold, default_drop_from_jump_threshold)

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])


class Status:
    def __init__(self):
        self.reset()

    def reset(self):
        self.in_position = 0
        self.ch_max_threshold_crossed = False
        self.ch_min_threshold_crossed = False
        self.lowest_ch_min_since_ch_min_threshold_crossed = 0
        self.highest_ch_max_since_ch_max_threshold_crossed = 0
        self.value_at_enter = 0
        self.lowest_since_enter = 0
        self.highest_since_enter = 0
        self.timedelta_since_position_enter = None
        self.timestamp_at_enter = None
        self.v_ch_max_is_to_when_enter, self.v_ch_min_is_to_when_enter = 0, 0
        self.v_ch_max_is_from_when_enter, self.v_ch_min_is_from_when_enter = 0, 0
        self.ch_from_enter = 0
        self.ch_from_lowest_since_enter = 0

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])

    def update(self, timestamp: pd.Timestamp, features, trading_param: JitterSimpleReversalTradingParam) -> None:
        value = features['value']

        if self.ch_max_threshold_crossed:
            if features['ch_max'] > self.highest_ch_max_since_ch_max_threshold_crossed:
                self.highest_ch_max_since_ch_max_threshold_crossed = features['ch_max']

        if self.ch_min_threshold_crossed:
            if features['ch_min'] < self.lowest_ch_min_since_ch_min_threshold_crossed:
                self.lowest_ch_min_since_ch_min_threshold_crossed = features['ch_min']

        if self.in_position != 0:
            if value < self.lowest_since_enter:
                self.lowest_since_enter = value

            if value > self.highest_since_enter:
                self.highest_since_enter = value

            self.timedelta_since_position_enter = (timestamp - self.timestamp_at_enter).to_pytimedelta()
            self.ch_from_enter = algo.feature.jitter.calculate._get_ch(self.value_at_enter, value)
            self.ch_from_lowest_since_enter = algo.feature.jitter.calculate._get_ch(self.lowest_since_enter, value)
            self.ch_from_highest_since_enter = algo.feature.jitter.calculate._get_ch(self.highest_since_enter, value)

            if self.in_position == -1:
                if self.timedelta_since_position_enter.seconds > 30 * 60 or features['ch_max'] < 0.025:
                    self.in_position = 0
                    self.ch_max_threshold_crossed = False
                    self.ch_min_threshold_crossed = False
                    self.highest_ch_max_since_ch_max_threshold_crossed = 0
                    self.lowest_ch_min_since_ch_min_threshold_crossed = 0

                # take profit
                if self.ch_from_enter < -abs(trading_param.jump_threshold):
                    self.in_position = 0

            elif self.in_position == +1:
                if self.timedelta_since_position_enter.seconds > 30 * 60 or features['ch_min'] > -0.025:
                    self.in_position = 0
                    self.ch_max_threshold_crossed = False
                    self.ch_min_threshold_crossed = False
                    self.highest_ch_max_since_ch_max_threshold_crossed = 0
                    self.lowest_ch_min_since_ch_min_threshold_crossed = 0

                # take profit
                if self.ch_from_enter > abs(trading_param.jump_threshold):
                    self.in_position = 0

        else:
            new_position = 0

            if features['ch_max'] > abs(trading_param.jump_threshold):
                self.ch_max_threshold_crossed = True
                self.highest_ch_max_since_ch_max_threshold_crossed = features['ch_max']

            if features['ch_min'] < -abs(trading_param.jump_threshold):
                self.ch_min_threshold_crossed = True
                self.lowest_ch_min_since_ch_min_threshold_crossed = features['ch_min']

            #if self.ch_max_threshold_crossed and features['ch_max'] < self.highest_ch_max_since_ch_max_threshold_crossed:
            if self.ch_max_threshold_crossed and features['ch_max'] < abs(trading_param.jump_threshold) and features['ch_since_max'] < -abs(trading_param.drop_from_jump_threshold):
                new_position = -1

            #if self.ch_min_threshold_crossed and features['ch_min'] > self.lowest_ch_min_since_ch_min_threshold_crossed:
            if self.ch_min_threshold_crossed and features['ch_min'] > -abs(trading_param.jump_threshold) and features['ch_since_min'] > abs(trading_param.drop_from_jump_threshold):
                new_position = 1

            if new_position != 0:
                self.in_position = new_position
                self.value_at_enter = value
                self.lowest_since_enter = value
                self.timestamp_at_enter = timestamp
                self.timedelta_since_position_enter = (timestamp - timestamp).to_pytimedelta()
                self.v_ch_max_is_to_when_enter = features['v_ch_max_is_to']
                self.v_ch_min_is_to_when_enter = features['v_ch_min_is_to']
                self.v_ch_max_is_from_when_enter = features['v_ch_max_is_from']
                self.v_ch_min_is_from_when_enter = features['v_ch_min_is_from']
                self.ch_from_enter = 0
                self.ch_from_lowest_since_enter = 0
            elif not self.ch_max_threshold_crossed and self.ch_min_threshold_crossed:
                self.reset()



def status_as_dict(status):
    return {
        'in_position': status.in_position,
        'value_at_enter': status.value_at_enter,
        'lowest_since_enter': status.lowest_since_enter,
        'highest_since_enter': status.highest_since_enter,
        'ch_max_threshold_crossed': status.ch_max_threshold_crossed,
        'ch_min_threshold_crossed': status.ch_min_threshold_crossed,
        'lowest_ch_min_since_ch_min_threshold_crossed': status.lowest_ch_min_since_ch_min_threshold_crossed,
        'highest_ch_max_since_ch_max_threshold_crossed': status.highest_ch_max_since_ch_max_threshold_crossed,
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
