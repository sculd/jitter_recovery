import numpy as np, datetime
import pytz
import logging

import algo.collective_jitter_recovery.calculate
import trading.execution
from collections import defaultdict, deque


def epoch_seconds_to_datetime(timestamp_seconds):
    t = datetime.datetime.utcfromtimestamp(timestamp_seconds)
    t_tz = pytz.utc.localize(t)
    return t_tz

class TradeManager:
    def __init__(self, trading_param=None, trade_execution=None):
        default_trading_param = algo.collective_jitter_recovery.calculate_collective.CollectiveRecoveryTradingParam.get_default_param()

        self.trading_param = trading_param if trading_param is not None else default_trading_param
        self.trade_execution = trade_execution if trade_execution else trading.execution.TradeExecution()
        self.status_per_symbol = defaultdict(algo.collective_jitter_recovery.calculate_collective.Status)
        self.ch_per_symbol = defaultdict(float)
        self.ch_min_per_symbol = defaultdict(float)
        self.collective_chs = deque()
        self.collective_ch_mins = deque()

    def _get_collective_features(self, timestamp_epoch_seconds):
        if len(self.ch_per_symbol) == 0: return

        ch = np.mean(list(self.ch_per_symbol.values()))
        ch_min = np.mean(list(self.ch_min_per_symbol.values()))
        timestamp_epoch_seconds = int(timestamp_epoch_seconds / 60) * 60
        recent_timestamp_epoch_seconds = self.collective_chs[-1][0] if len(self.collective_chs) > 0 else 0
        if timestamp_epoch_seconds == recent_timestamp_epoch_seconds:
            self.collective_chs[-1] = (timestamp_epoch_seconds, ch)
            self.collective_ch_mins[-1] = (timestamp_epoch_seconds, ch_min)
        elif timestamp_epoch_seconds > recent_timestamp_epoch_seconds:
            self.collective_chs.append((timestamp_epoch_seconds, ch))
            self.collective_ch_mins.append((timestamp_epoch_seconds, ch_min))

        while len(self.collective_chs) > 30:
            self.collective_chs.popleft()
        while len(self.collective_ch_mins) > 30:
            self.collective_ch_mins.popleft()

        ch_window30_min = min([tch[1] for tch in self.collective_chs])
        ch_min_window30_min = min([tch[1] for tch in self.collective_ch_mins])
        return {'ch_window30_min': ch_window30_min}
    
    def on_new_minutes(self, symbol, timestamp_epoch_seconds, timestamp_epochs_values):
        '''
        timestamp_epochs_values is an arrya of (timestamp, value) tuples.
        '''
        w = self.trading_param.feature_param.window
        changes = algo.jitter_recovery.calculate.get_changes_1dim(np.array([tv[1] for tv in list(timestamp_epochs_values)[-w:]]))
        self.ch_per_symbol[symbol] = changes['ch']
        collective_features = self._get_collective_features(timestamp_epoch_seconds)
        in_position_before = self.status_per_symbol[symbol].in_position
        self.status_per_symbol[symbol].update(collective_features, changes, self.trading_param)
        if self.status_per_symbol[symbol].in_position != in_position_before:
            direction = 1 if self.status_per_symbol[symbol].in_position == 1 else -1
            logging.info(f'in_position changes at {epoch_seconds_to_datetime(timestamp_epoch_seconds)} for {symbol} from {in_position_before} to {self.status_per_symbol[symbol].in_position} with changes: {changes}')
            self.trade_execution.execute(symbol, timestamp_epoch_seconds, changes['value'], +1, direction)

