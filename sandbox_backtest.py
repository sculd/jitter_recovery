import logging, sys, datetime
import trading.prices_csv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("logs/{}.log".format("log_backtest")),
        logging.StreamHandler(sys.stdout)
    ]
)

import trading.trade

is_long_term=True

trading_manager = trading.trade.TradeManager(is_long_term=is_long_term)


filename = "data/okx/csv_okx_20231216_1231.csv"
price_cache = trading.prices_csv.BacktestCsvPriceCache(trading_manager, filename, trading_manager.trading_param.jitter_recover_feature_param.jump_window)


logging.info(f"### starting a new backtest at {datetime.datetime.now()}, filename: {filename}")

while True:
    if price_cache.process_next_candle() is False:
        break

trading_manager.trade_execution.print()

trading_manager.trade_execution.closed_execution_records.to_csv_file(open(f'{filename}_backtest.csv', 'w'))

