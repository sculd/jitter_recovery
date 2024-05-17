import datetime

import pandas as pd

import algo.feature.momentum.calculate
from algo.feature.momentum.calculate import MomentumFeatureParam

default_selection_size = 10
default_rebalance_interval_minutes = 6 * 60

class MomentumTradingParam:
    def __init__(self, feature_param: MomentumFeatureParam, selection_size: int, rebalance_interval_minutes: int):
        self.feature_param = feature_param
        self.selection_size = selection_size
        self.rebalance_interval_minutes = rebalance_interval_minutes

    @staticmethod
    def get_default_param():
        return MomentumTradingParam(
            MomentumFeatureParam.get_default_param(), default_selection_size, default_rebalance_interval_minutes)

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])


def _is_long(features, trading_param: MomentumTradingParam):
    return features['rank_descending'] <= trading_param.selection_size and features['momentum'] > 0

def _is_short(features, trading_param: MomentumTradingParam):
    return features['rank'] <= trading_param.selection_size and features['momentum'] < 0


class Status:
    def __init__(self):
        self.reset()

    def reset(self):
        self.in_position = 0
        self.value_at_enter = 0
        self.ch_from_enter = 0
        self.ema_at_enter = 0
        self.ch_ema_from_enter = 0

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in vars(self).items()])

    def update(self, t: datetime.datetime, features: dict, trading_param: MomentumTradingParam) -> None:
        value = features['value']
        ema = features['ema']

        if self.in_position != 0:
            self.ch_from_enter = algo.feature.momentum.calculate._get_ch(self.value_at_enter, value)
            self.ch_ema_from_enter = algo.feature.momentum.calculate._get_ch(self.ema_at_enter, ema)

        '''
        # stop-loss
        if self.ch_ema_from_enter * self.in_position < -0.05:
            self.in_position = 0
            self.reset()
        '''

        if int(t.strftime('%s')) % (trading_param.rebalance_interval_minutes * 60) != 0:
            return

        if self.in_position == 1:
            self.in_position = 1 if _is_long(features, trading_param) else 0
        elif self.in_position == -1:
            self.in_position = -1 if _is_short(features, trading_param) else 0
        else:
            if _is_long(features, trading_param):
                in_position = 1
            elif _is_short(features, trading_param):
                in_position = -1
            else:
                in_position = 0

            if in_position != 0:
                self.in_position = in_position
                self.value_at_enter = value
                self.ch_from_enter = 0
                self.ema_at_enter = ema
                self.ch_ema_from_enter = 0

        if self.in_position == 0:
            self.reset()


def status_as_dict(status):
    return {
        'in_position': status.in_position,
        'value_at_enter': status.value_at_enter,
        'ch_from_enter': status.ch_from_enter,
    }


def status_as_df(status):
    return pd.DataFrame({k: [v] for k, v in status_as_dict(status).items()})
