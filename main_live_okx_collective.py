import logging, sys, datetime, time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("logs/{}.log".format("log_live_okx_collective")),
        logging.StreamHandler(sys.stdout)
    ]
)

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

import algo.alpha.collective_jitter_recovery.trade, trading.execution_okx
import algo.alpha.collective_jitter_recovery.calculate
import trading.price_okx

logging.info(f"### starting a new okx live at {datetime.datetime.now()}")

import publish.telegram
publish.telegram.post_message(f"starting a new okx collective live at {datetime.datetime.now()}")

trade_execution = trading.execution_okx.TradeExecution(target_betsize=30, leverage=10)
trade_execution.enable_dry_run(enable=False)
trading_param = algo.alpha.collective_jitter_recovery.calculate.CollectiveRecoveryTradingParam.get_default_param()
trading_manager = algo.alpha.collective_jitter_recovery.trade.TradeManager(trade_execution=trade_execution, trading_param=trading_param)

trade_execution_small_drop = trading.execution_okx.TradeExecution(target_betsize=30, leverage=10)
trade_execution_small_drop.enable_dry_run(enable=False)
trading_param_small_drop = algo.alpha.collective_jitter_recovery.calculate.CollectiveRecoveryTradingParam.get_default_param_small_drop()
trading_manager_small_drop = algo.alpha.collective_jitter_recovery.trade.TradeManager(trade_execution=trade_execution_small_drop, trading_param=trading_param_small_drop)
logging.info("starting a okx collective")
price_cache = trading.price_okx.PriceCache([trading_manager, trading_manager_small_drop])
#price_cache = trading.price_okx.PriceCache([trading_manager_small_drop])

while True:
    trading_manager.trade_execution.print()
    trading_manager_small_drop.trade_execution.print()
    time.sleep(60 * 60)

