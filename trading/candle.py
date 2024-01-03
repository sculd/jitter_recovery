import pandas as pd, numpy as np
import datetime, time
from collections import defaultdict, deque
import logging


class CandleCache:
    def __init__(self, trading_manager, windows_minutes):
        self.trading_manager = trading_manager
        self.symbol_serieses = defaultdict(deque)
        self.windows_minutes = windows_minutes

        self.history_read_i = 0
        self.latest_timestamp_epoch_seconds_by_symbol = defaultdict(int)
        self.latest_timestamp_epoch_seconds = 0
        self.latest_timestamp_epoch_seconds_truncated_daily = 0


    def on_candle(self, timestamp_epoch_seconds, symbol, open_, high_, low_, close_, volume):
        #print(f'on_candle {timestamp}, {symbol}, {open_}, {high_}, {low_}, {close_}, {volume}')
        insert_new_minute = False
        if len(self.symbol_serieses[symbol]) == 0:
            self.symbol_serieses[symbol].append((timestamp_epoch_seconds, close_))
        else:
            last_timestamp_epoch_seconds = self.symbol_serieses[symbol][-1][0]
            if int(last_timestamp_epoch_seconds / 60) == int(timestamp_epoch_seconds / 60):
                self.symbol_serieses[symbol][-1] = (timestamp_epoch_seconds, close_,)
            else:
                copy_timestamp_epoch_seconds = (int(last_timestamp_epoch_seconds / 60) + 1) * 60
                # fill the gap if any
                while copy_timestamp_epoch_seconds < timestamp_epoch_seconds:
                    self.symbol_serieses[symbol].append((copy_timestamp_epoch_seconds, self.symbol_serieses[symbol][-1][1]))
                    copy_timestamp_epoch_seconds += 60
                insert_new_minute = True

        self.latest_timestamp_epoch_seconds_by_symbol[symbol] = timestamp_epoch_seconds
        self.latest_timestamp_epoch_seconds = timestamp_epoch_seconds

        previous_latest_timestamp_epoch_seconds_truncated_daily = self.latest_timestamp_epoch_seconds_truncated_daily
        self.latest_timestamp_epoch_seconds_truncated_daily = int(timestamp_epoch_seconds / (24 * 60 * 60)) * (24 * 60 * 60)

        if self.latest_timestamp_epoch_seconds_truncated_daily > previous_latest_timestamp_epoch_seconds_truncated_daily:
            logging.info(f'start reading a new day: {datetime.datetime.utcfromtimestamp(self.latest_timestamp_epoch_seconds_truncated_daily)}')

        while len(self.symbol_serieses[symbol]) > self.windows_minutes:
            self.symbol_serieses[symbol].popleft()

        if insert_new_minute:
            self.trading_manager.on_new_minutes(symbol, timestamp_epoch_seconds, self.symbol_serieses[symbol])
            self.symbol_serieses[symbol].append((timestamp_epoch_seconds, close_))
