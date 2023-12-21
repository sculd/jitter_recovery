import logging, sys, datetime, time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("logs/{}.log".format("log_live")),
        logging.StreamHandler(sys.stdout)
    ]
)

import trading.trade
trading_manager = trading.trade.TradeManager()

import trading.price

logging.info(f"### starting a new live at {datetime.datetime.now()}")

price_cache = trading.price.PriceCache(trading_manager, 60)

while True:
    trading_manager.trade_execution.print()
    time.sleep(60)

trading_manager.trade_execution.closed_execution_records.to_csv_file(open(f'live.csv', 'w'))

