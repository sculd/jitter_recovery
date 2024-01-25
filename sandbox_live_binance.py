import logging, sys, datetime, time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("logs/{}.log".format("log_live_binance")),
        logging.StreamHandler(sys.stdout)
    ]
)

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

import trading.trade, trading.execution_binance

trade_execution = trading.execution_binance.TradeExecution(target_betsize=50, leverage=5)
trading_manager = trading.trade.TradeManager(trade_execution=trade_execution)

import trading.price_binance

logging.info(f"### starting a new binance live at {datetime.datetime.now()}")

import publish.telegram
publish.telegram.post_message(f"starting a new binance live at {datetime.datetime.now()}")

price_cache = trading.price_binance.PriceCache(trading_manager, 60)

while True:
    trading_manager.trade_execution.print()
    time.sleep(60 * 60)

trading_manager.trade_execution.closed_execution_records.to_csv_file(open(f'live.csv', 'w'))

