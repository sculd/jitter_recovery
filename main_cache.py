import datetime, logging, sys, os


if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')
    os.environ["GOOGLE_CLOUD_PROJECT"] = "trading-290017"
else:
    print('the credential.json file does not exist')


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

import market_data.ingest.bq.common
import market_data.ingest.bq.cache
import market_data.ingest.bq.validate
import algo.jitter_common.calculate
import algo.jitter_recovery.calculate
import algo.jitter_common.research
import algo.jitter_recovery.research
import algo.collective_jitter_recovery.calculate
import algo.collective_jitter_recovery.research
import algo.cache


def _get_feature_param_labels():
    params = [
        algo.jitter_common.calculate.JitterFeatureParam(30),
        algo.jitter_common.calculate.JitterFeatureParam(40),
        algo.jitter_common.calculate.JitterFeatureParam(240),
    ]
    labels = [
        algo.jitter_common.research.get_feature_label_for_caching(param) for param in params
    ]
    return params, labels

def _get_collective_feature_param_labels():
    collective_params = [
        algo.collective_jitter_recovery.calculate.CollectiveRecoveryFeatureParam(window=40, collective_window=30),
    ]
    collective_labels = [
        algo.collective_jitter_recovery.research.get_feature_label_for_caching(param) for param in collective_params
    ]
    return collective_params, collective_labels


def verify_features_cache(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
) -> None:
    _, labels = _get_feature_param_labels()
    _, collective_labels = _get_collective_feature_param_labels()
    for label in labels + collective_labels:
        logging.info(f"verify feature cache for feature {label}")
        algo.cache.validate_df(
            label=label,
            date_str_from=date_str_from,
            date_str_to=date_str_to,
            dataset_mode=dataset_mode,
            export_mode=export_mode,
        )


def cache_features(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
    symbol_filter=None, value_column='close',
) -> None:
    df = market_data.ingest.bq.cache.read_from_cache(
        dataset_mode,
        export_mode,
        market_data.ingest.bq.common.AGGREGATION_MODE.TAKE_LASTEST,
        date_str_from=date_str_from, date_str_to=date_str_to)

    if df is None:
        return

    df = df.reset_index()

    def do_cache(feature_params, labels, get_dfst_feature_func):
        for feature_param, label in zip(feature_params, labels):
            logging.info(f"for {label}")
            dfst_feature = get_dfst_feature_func(df, feature_param, symbol_filter=symbol_filter, value_column=value_column)
            algo.cache.cache_df(
                dfst_feature,
                label=label,
                dataset_mode=dataset_mode,
                export_mode=export_mode,
                overwrite=True)
            del dfst_feature

    feature_params, labels = _get_feature_param_labels()
    do_cache(feature_params, labels, algo.jitter_common.research.get_dfst_feature)

    feature_params, labels = _get_collective_feature_param_labels()
    do_cache(feature_params, labels, algo.collective_jitter_recovery.research.get_dfst_feature)

def _get_trading_param_labels():
    params = [
        algo.jitter_recovery.calculate.JitterRecoveryTradingParam(
            algo.jitter_common.calculate.JitterFeatureParam(30),
            0.20, -0.04, 0.02, is_long_term=False),
    ]
    feature_labels = [
        algo.jitter_common.research.get_feature_label_for_caching(param.feature_param) for param in params
    ]
    trading_labels = [
        algo.jitter_recovery.research.get_trading_label_for_caching(param) for param in params
    ]
    return params, feature_labels, trading_labels


def _get_collective_trading_param_labels():
    collective_params = [
        algo.collective_jitter_recovery.calculate.CollectiveRecoveryTradingParam(
            algo.collective_jitter_recovery.calculate.CollectiveRecoveryFeatureParam(window=40, collective_window=30),
            collective_drop_recovery_trading_param=algo.collective_jitter_recovery.calculate.CollectiveDropRecoveryTradingParam(
                -0.03, -0.30, -0.03, +0.005, -0.01),
            collective_jump_recovery_trading_param=None,
        ),
    ]
    collective_feature_labels = [
        algo.collective_jitter_recovery.research.get_feature_label_for_caching(param.feature_param) for param in collective_params
    ]
    collective_trading_labels = [
        algo.collective_jitter_recovery.research.get_trading_label_for_caching(param) for param in collective_params
    ]
    return collective_params, collective_feature_labels, collective_trading_labels


def verify_trading_cache(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
) -> None:
    _, _, labels = _get_trading_param_labels()
    _, _, collective_labels = _get_collective_trading_param_labels()
    for label in labels + collective_labels:
        logging.info(f"verify trading cache for trading {label}")
        algo.cache.validate_df(
            label=label,
            date_str_from=date_str_from,
            date_str_to=date_str_to,
            dataset_mode=dataset_mode,
            export_mode=export_mode,
        )


def cache_trading(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
) -> None:
    def do_cache(trading_params, feature_labels, trading_labels, get_dfst_trading_func):
        for trading_param, feature_label, trading_label in zip(trading_params, feature_labels, trading_labels):
            logging.info(f"for {trading_label}")
            dfst_feature = algo.cache.read_df(
                label=feature_label,
                dataset_mode=dataset_mode,
                export_mode=export_mode,
                date_str_from=date_str_from,
                date_str_to=date_str_to)
            if dfst_feature is None:
                logging.error(f"feature for {feature_label} can not be found in the cache.")
                continue
            dfst_trading = get_dfst_trading_func(dfst_feature, trading_param)
            del dfst_feature
            algo.cache.cache_df(
                dfst_trading,
                label=trading_label,
                dataset_mode=dataset_mode,
                export_mode=export_mode,
                overwrite=True)
            del dfst_trading

    trading_params, feature_labels, trading_labels = _get_trading_param_labels()
    do_cache(trading_params, feature_labels, trading_labels, algo.jitter_recovery.research.get_dfst_trading)

    trading_params, feature_labels, trading_labels = _get_collective_trading_param_labels()
    do_cache(trading_params, feature_labels, trading_labels, algo.collective_jitter_recovery.research.get_dfst_trading)


def cache_all(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
    if_cache_features=False,
    if_verify_features=False,
    if_cache_trading=False,
    if_verify_trading=False,
    symbol_filter=lambda s: s.endswith('USD'),
    value_column='close',
):
    aggregation_mode = market_data.ingest.bq.common.AGGREGATION_MODE.TAKE_LASTEST
    #'''
    market_data.ingest.bq.cache.fetch_and_cache(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=dataset_mode, export_mode=export_mode,
        aggregation_mode=aggregation_mode,
    )
    #'''
    #'''
    market_data.ingest.bq.validate.verify_data_cache(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=dataset_mode, export_mode=export_mode,
        aggregation_mode=aggregation_mode,
    )
    #'''

    if if_cache_features:
        cache_features(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
            symbol_filter=symbol_filter, value_column=value_column,
        )

    if if_verify_features:
        verify_features_cache(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
        )

    if if_cache_trading:
        cache_trading(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
        )

    if if_verify_trading:
        verify_trading_cache(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
        )


def run_okx(date_str_from: str, date_str_to: str, if_cache_features=False, if_cache_trading=False, if_verify_features=False, if_verify_trading=False):
    cache_all(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.OKX,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
        if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
        symbol_filter=lambda s: s.endswith('-USDT-SWAP')
    )


def run_binance(date_str_from: str, date_str_to: str, if_cache_features=False, if_cache_trading=False, if_verify_features=False, if_verify_trading=False):
    cache_all(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.BINANCE,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
        if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
    )


def run_cex(date_str_from: str, date_str_to: str, if_cache_features=False, if_cache_trading=False, if_verify_features=False, if_verify_trading=False):
    cache_all(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.CEX,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
        if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
    )


def run_gemini(date_str_from: str, date_str_to: str, if_cache_features=False, if_cache_trading=False, if_verify_features=False, if_verify_trading=False):
    cache_all(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.GEMINI,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
        if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
    )


def run_bithumb(date_str_from: str, date_str_to: str, if_cache_features=False, if_cache_trading=False, if_verify_features=False, if_verify_trading=False):
    cache_all(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.BITHUMB,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.ORDERBOOK_LEVEL1,
        if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
        symbol_filter=lambda s: s.endswith('-KRW'), value_column='price_ask',
    )


if __name__ == '__main__':
    date_str_from='2024-04-10'
    date_str_to='2024-04-30'
    if_cache_features = False
    if_verify_features = False
    if_cache_trading = True
    if_verify_trading = False
    #run_okx(date_str_from=date_str_from, date_str_to=date_str_to, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading)
    run_binance(date_str_from=date_str_from, date_str_to=date_str_to, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading)
    run_cex(date_str_from=date_str_from, date_str_to=date_str_to, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading)
    run_gemini(date_str_from=date_str_from, date_str_to=date_str_to, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading)
    run_bithumb(date_str_from=date_str_from, date_str_to=date_str_to, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading)
