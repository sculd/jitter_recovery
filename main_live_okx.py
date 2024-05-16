import logging, sys, datetime, time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("logs/{}.log".format("log_live_okx")),
        logging.StreamHandler(sys.stdout)
    ]
)

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

import algo.alpha.jitter_recovery.trade, trading.execution_okx
import trading.price_okx

logging.info(f"### starting a new okx live at {datetime.datetime.now()}")

import publish.telegram
publish.telegram.post_message(f"starting a okx new live at {datetime.datetime.now()}")

trade_execution = trading.execution_okx.TradeExecution(target_betsize=200, leverage=5)
trading_manager = algo.alpha.jitter_recovery.trade.TradeManager(is_long_term=False, trade_execution=trade_execution)
trading_manager_longterm = algo.alpha.jitter_recovery.trade.TradeManager(is_long_term=True, trade_execution=trade_execution)
logging.info("starting a okx short/long term")
price_cache = trading.price_okx.PriceCache([trading_manager, trading_manager_longterm])

while True:
    trading_manager.trade_execution.print()
    trading_manager_longterm.trade_execution.print()
    time.sleep(60 * 60)

