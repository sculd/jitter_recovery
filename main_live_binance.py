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

import algo.alpha.jitter_recovery.trade, trading.execution_binance
import trading.price_binance

logging.info(f"### starting a new binance live at {datetime.datetime.now()}")

import publish.telegram
publish.telegram.post_message(f"starting a new binance live at {datetime.datetime.now()}")

trade_execution = trading.execution_binance.TradeExecution(target_betsize=50, leverage=5)
trading_manager = algo.alpha.jitter_recovery.trade.TradeManager(is_long_term=False, trade_execution=trade_execution)
trading_manager_long_term = algo.alpha.jitter_recovery.trade.TradeManager(is_long_term=True, trade_execution=trade_execution)

logging.info("starting a binance short/long term")
price_cache = trading.price_binance.PriceCache([trading_manager, trading_manager_long_term])

while True:
    trading_manager.trade_execution.print()
    trading_manager_long_term.trade_execution.print()
    time.sleep(60 * 60)

