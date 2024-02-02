import pandas as pd, numpy as np, datetime
import pytz
import logging

import algo.jitter_recovery.calculate
import trading.execution
from collections import defaultdict


def epoch_seconds_to_datetime(timestamp_seconds):
    t = datetime.datetime.utcfromtimestamp(timestamp_seconds)
    t_tz = pytz.utc.localize(t)
    return t_tz

class TradeManager:
    def __init__(self, is_long_term, trading_param=None, trade_execution=None):
        if not is_long_term:
            default_trading_param = algo.jitter_recovery.calculate.JitterRecoveryTradingParam.get_default_param_longterm()
        else:
            default_trading_param = algo.jitter_recovery.calculate.JitterRecoveryTradingParam.get_default_param()

        self.trading_param = trading_param if trading_param is not None else default_trading_param
        self.trade_execution = trade_execution if trade_execution else trading.execution.TradeExecution()
        self.status_per_symbol = defaultdict(algo.jitter_recovery.calculate.Status)

    def on_new_minutes(self, symbol, timestamp_epoch_seconds, timestamp_epochs_values):
        '''
        timestamp_epochs_values is an arrya of (timestamp, value) tuples.
        '''
        w = self.trading_param.jitter_recover_feature_param.jump_window
        changes = algo.jitter_recovery.calculate.get_changes_1dim(np.array([tv[1] for tv in list(timestamp_epochs_values)[-w:]]))
        in_position_before = self.status_per_symbol[symbol].in_position
        self.status_per_symbol[symbol].update(changes, self.trading_param)
        if self.status_per_symbol[symbol].in_position != in_position_before:
            direction = 1 if self.status_per_symbol[symbol].in_position == 1 else -1
            logging.info(f'in_position changes at {epoch_seconds_to_datetime(timestamp_epoch_seconds)} for {symbol} from {in_position_before} to {self.status_per_symbol[symbol].in_position} with changes: {changes}')
            self.trade_execution.execute(symbol, timestamp_epoch_seconds, changes['value'], -1, direction)

