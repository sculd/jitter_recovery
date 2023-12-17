import logging, sys
import trading.prices_csv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("logs/{}.log".format("log_backtest.txt")),
        logging.StreamHandler(sys.stdout)
    ]
)


price_cache = trading.prices_csv.BacktestCsvPriceCache("data/okx/csv_okx_20231127_1128.csv", 60)
#price_cache = trading.prices_csv.BacktestCsvPriceCache("data/okx/csv_okx_20231125_1212.csv", 60)


logging.info("### starting a new backtest ###")

import trading.trade
trading_manager = trading.trade.TradeManager(price_cache=price_cache)

while True:
    if price_cache.process_next_candle() is False:
        break


