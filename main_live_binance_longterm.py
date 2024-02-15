import logging, sys, datetime, time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("logs/{}.log".format("log_live_binance_longterm")),
        logging.StreamHandler(sys.stdout)
    ]
)

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

import trading.trade, trading.execution_binance
import trading.price_binance

logging.info(f"### starting a new binance longterm live at {datetime.datetime.now()}")

import publish.telegram
publish.telegram.post_message(f"starting a new binance longterm live at {datetime.datetime.now()}")

trade_execution = trading.execution_binance.TradeExecution(target_betsize=50, leverage=5)
trading_manager = trading.trade.TradeManager(is_long_term=True, trade_execution=trade_execution)
logging.info("starting a binance longterm")
price_cache_longterm = trading.price_binance.PriceCache(trading_manager, trading_manager.trading_param.jitter_recovery_feature_param.window)

while True:
    trading_manager.trade_execution.print()
    time.sleep(60 * 60)

