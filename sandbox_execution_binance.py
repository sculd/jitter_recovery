import time
import logging
import os, sys
from binance.client import Client
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.


_API_KEY = os.getenv('API_KEY_BINANCE')
_API_SECRET = os.getenv('API_SECRET_BINANCE')


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

_client = None

def get_client():
    global _client
    if _client is not None:
        return _client
    _client = Client(_API_KEY, _API_SECRET)
    return _client



client = get_client()

account_info = client.get_account()
margin_account_info = client.get_margin_account()
future_account_info = client.futures_account()
future_account_balance = client.futures_account_balance()

print(client.futures_change_leverage(symbol='BTCUSDT', leverage=5))


exchange_info = client.get_exchange_info()

import trading.execution_binance

execution = trading.execution_binance.TradeExecution(30, 5)
execution.close_open_positions()

execution.enter('XVSUSDT', 60, 11, -1)


print(account_info)




