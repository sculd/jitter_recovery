import os
from binance.client import Client

_API_KEY = os.getenv('API_KEY_BINANCE')
_API_SECRET = os.getenv('API_SECRET_BINANCE')

_USDT = 'USDT'
_BUSD = 'BUSD'

_client = None

def get_client():
    global _client
    if _client is not None:
        return _client
    _client = Client(_API_KEY, _API_SECRET)
    return _client

def get_symbols_usd():
    client = get_client()
    exinfo = client.get_exchange_info()
    symbols = [s for s in exinfo['symbols']]
    symbols = [s['symbol'] for s in symbols if s['status'] == 'TRADING']
    symbols_usd = [s for s in symbols if s.endswith(_USDT) or s.endswith(_BUSD)]

    return symbols_usd

def get_future_symbobls_usd():
    client = get_client()

    exinfo_future = client.futures_exchange_info()
    symbols_info = [s for s in exinfo_future['symbols'] if s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING']
    symbols_future_usdt = [s['symbol'] for s in symbols_info if s['quoteAsset'] == _USDT]
    symbols_future_busd = [s['symbol'] for s in symbols_info if s['quoteAsset'] == _BUSD]

    return symbols_future_usdt

