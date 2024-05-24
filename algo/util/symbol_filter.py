from pathlib import Path

_filename_reportable_coins = 'reportable_coins.txt'
_filename_gemini_symbols = 'gemini_symbols.txt'

_p_reportable_coins = Path(__file__).with_name(_filename_reportable_coins)
_p_gemini_symbols = Path(__file__).with_name(_filename_gemini_symbols)

_reportable_coins = set()
with _p_reportable_coins.open('r') as f:
    for coin in f:
        _reportable_coins.add(coin.split()[0])

def _coin_from_symbol(symbol):
    coin = symbol.replace('-', '')
    coin = coin.split('USD')[0]
    coin = coin.split('KRW')[0]
    return coin

_gemini_coins = set()
with _p_gemini_symbols.open('r') as f:
    for symbol in f:
        _gemini_coins.add(_coin_from_symbol(symbol.upper()))

def if_reportable_symbol(symbol):
    return _coin_from_symbol(symbol) in _reportable_coins

def if_gemini_symbol(symbol):
    return _coin_from_symbol(symbol) in _gemini_coins

def _filter_out_symbols(df, filter_out_func = None, symbol_in_index=False):
    symbols = list(df.symbol.unique()) if not symbol_in_index else list(df.index.get_level_values('symbol').unique())
    symbols_good = set([s for s in symbols if not filter_out_func(s)])
    if symbol_in_index:
        return df[
            df.index.get_level_values('symbol').isin(symbols_good)
        ]
    else:
        return df[
            df.symbol.isin(symbols_good)
        ]

def filter_out_reportable_symbols(df, symbol_in_index=False):
    return _filter_out_symbols(df, filter_out_func=if_reportable_symbol, symbol_in_index=symbol_in_index)

def filter_out_non_gemini_symbol(df, symbol_in_index=False):
    return _filter_out_symbols(df, filter_out_func=lambda s: not if_gemini_symbol(s), symbol_in_index=symbol_in_index)

