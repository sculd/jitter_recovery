import pandas as pd, numpy as np
import datetime, time
from collections import defaultdict, deque
import logging


class BacktestCsvPriceCache:
    def __init__(self, csv_filename, windows_minutes):
        df_prices_history = pd.read_csv(csv_filename)
        df_prices_history['time'] = pd.to_datetime(df_prices_history['timestamp'], unit='s')

        self.__init_with_df(df_prices_history, windows_minutes)
        logging.info(f'csv price cache loaded {csv_filename}')

    def __init_with_df(self, df_prices_history, windows_minutes):
        self.symbol_serieses = defaultdict(deque)
        self.windows_minutes = windows_minutes
        self.df_prices_history = df_prices_history
        self.iterrows = df_prices_history.iterrows()

        self.history_read_i = 0
        self.latest_timestamp_epoch_seconds_by_symbol = defaultdict(int)
        self.latest_timestamp_epoch_seconds = 0

        self.trading_manager = None

        logging.info(f'csv price cache loaded {len(df_prices_history)}')

    def set_trading_manager(self, trading_manager):
        self.trading_manager = trading_manager

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
                self.symbol_serieses[symbol].append((timestamp_epoch_seconds, close_))

        self.latest_timestamp_epoch_seconds_by_symbol[symbol] = timestamp_epoch_seconds
        self.latest_timestamp_epoch_seconds = timestamp_epoch_seconds

        while len(self.symbol_serieses[symbol]) > self.windows_minutes:
            self.symbol_serieses[symbol].popleft()

        if insert_new_minute:
            self.trading_manager.on_new_minutes(symbol, timestamp_epoch_seconds, self.symbol_serieses[symbol])
            self.symbol_serieses[symbol].append((timestamp_epoch_seconds, close_))

    def _get_next_candle(self):
        return next(self.iterrows, None)

    def process_next_candle(self):
        i_candle = self._get_next_candle()
        if i_candle is None:
            return False

        candle = i_candle[1]
        self.on_candle(candle['timestamp'], candle['symbol'], candle['open'], candle['high'], candle['low'], candle['close'], candle['volume'])

        if self.history_read_i % 10000 == 0:
            pass # print(f'self.history_read_i: {self.history_read_i}')

        self.history_read_i += 1

        return True

