import logging, sys, datetime, time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

import trading.execution_okx

trade_execution = trading.execution_okx.TradeExecution(30, 5)


trade_execution.enter('GMX-USDT-SWAP', 60, -1)

time.sleep(3)

trade_execution.exit('GMX-USDT-SWAP', 120)

