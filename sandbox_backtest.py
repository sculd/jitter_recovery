import logging, sys, datetime
import trading.prices_csv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("logs/{}.log".format("log_backtest")),
        logging.StreamHandler(sys.stdout)
    ]
)

is_long_term=True

#trading_manager = trading.trade.TradeManager(is_long_term=is_long_term)

import algo.feature.collective_jitter.calculate
import algo.alpha.collective_jitter_recovery.calculate
feature_param = algo.feature.collective_jitter.calculate.CollectiveJitterFeatureParam(40, 40)
trading_param = algo.alpha.collective_jitter_recovery.calculate.CollectiveRecoveryTradingParam(
    feature_param, 
    collective_drop_threshold = -0.03,
    collective_drop_lower_threshold = -0.05,
    drop_threshold = -0.03,
    jump_from_drop_threshold = +0.005,
    exit_drop_threshold  = -0.01,
    )
trading_manager = algo.alpha.collective_jitter_recovery.trade.TradeManager(trading_param=trading_param)


filename = "data/okx/csv_okx_20240101_0115.csv"
price_cache = trading.prices_csv.BacktestCsvPriceCache(trading_manager, filename, trading_manager.trading_param.feature_param.window)


logging.info(f"### starting a new backtest at {datetime.datetime.now()}, filename: {filename}")

while True:
    if price_cache.process_next_candle() is False:
        break

trading_manager.trade_execution.print()

trading_manager.trade_execution.closed_execution_records.to_csv_file(open(f'{filename}_backtest.csv', 'w'))

