import logging, sys, os


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
import algo.feature.jitter.calculate
import algo.feature.collective_jitter.calculate
import algo.feature.momentum.calculate
import algo.feature.jitter.research
import algo.feature.collective_jitter.research
import algo.feature.momentum.research
import algo.alpha.jitter_recovery.calculate
import algo.alpha.jitter_following.calculate
import algo.alpha.collective_jitter_recovery.calculate
import algo.alpha.momentum.calculate
import algo.alpha.jitter_recovery.research
import algo.alpha.jitter_following.research
import algo.alpha.collective_jitter_recovery.research
import algo.alpha.momentum.research
import algo.cache


def _get_jitter_feature_param_labels_get_dfst_feature_func():
    params = [
        algo.feature.jitter.calculate.JitterFeatureParam(30),
        algo.feature.jitter.calculate.JitterFeatureParam(40),
        algo.feature.jitter.calculate.JitterFeatureParam(240),
    ]
    labels = [
        algo.feature.jitter.research.get_feature_label_for_caching(param) for param in params
    ]
    return params, labels, algo.feature.jitter.research.get_dfst_feature

def _get_collective_feature_param_labels_get_dfst_feature_func():
    collective_params = [
        algo.feature.collective_jitter.calculate.CollectiveJitterFeatureParam(window=40, collective_window=30),
    ]
    collective_labels = [
        algo.feature.collective_jitter.research.get_feature_label_for_caching(param) for param in collective_params
    ]
    return collective_params, collective_labels, algo.feature.collective_jitter.research.get_dfst_feature

def _get_momentum_feature_param_labels_get_dfst_feature_func():
    params = [
        algo.feature.momentum.calculate.MomentumFeatureParam(120, 30),
        algo.feature.momentum.calculate.MomentumFeatureParam(180, 30),
        algo.feature.momentum.calculate.MomentumFeatureParam(360, 60),
    ]
    labels = [
        algo.feature.momentum.research.get_feature_label_for_caching(param) for param in params
    ]
    return params, labels, algo.feature.momentum.research.get_dfst_feature


def _get_feature_param_labels_get_dfst_feature_func(feature_name: str):
    if feature_name == 'jitter':
        return _get_jitter_feature_param_labels_get_dfst_feature_func()
    elif feature_name == 'collective_jitter':
        return _get_collective_feature_param_labels_get_dfst_feature_func()
    elif feature_name == 'momentum':
        return _get_momentum_feature_param_labels_get_dfst_feature_func()
    else:
        return [], []


def _get_jitter_trading_param_labels_trading_func():
    params = [
        algo.alpha.jitter_recovery.calculate.JitterRecoveryTradingParam(
            algo.feature.jitter.calculate.JitterFeatureParam(30),
            0.20, -0.04, 0.02, is_long_term=False),
    ]
    feature_labels = [
        algo.feature.jitter.research.get_feature_label_for_caching(param.feature_param) for param in params
    ]
    trading_labels = [
        algo.alpha.jitter_recovery.research.get_trading_label_for_caching(param) for param in params
    ]
    return params, feature_labels, trading_labels, algo.alpha.jitter_recovery.research.get_dfst_trading


def _get_jitter_following_trading_param_labels_trading_func():
    params = [
        algo.alpha.jitter_following.calculate.JitterFollowingTradingParam(
            algo.feature.jitter.calculate.JitterFeatureParam(30),
            0.15, -0.02),
        algo.alpha.jitter_following.calculate.JitterFollowingTradingParam(
            algo.feature.jitter.calculate.JitterFeatureParam(30),
            0.20, -0.02),
        algo.alpha.jitter_following.calculate.JitterFollowingTradingParam(
            algo.feature.jitter.calculate.JitterFeatureParam(30),
            0.15, -0.01),
        algo.alpha.jitter_following.calculate.JitterFollowingTradingParam(
            algo.feature.jitter.calculate.JitterFeatureParam(30),
            0.20, -0.01),
    ]
    feature_labels = [
        algo.feature.jitter.research.get_feature_label_for_caching(param.feature_param) for param in params
    ]
    trading_labels = [
        algo.alpha.jitter_following.research.get_trading_label_for_caching(param) for param in params
    ]
    return params, feature_labels, trading_labels, algo.alpha.jitter_following.research.get_dfst_trading


def _get_collective_trading_param_labels_trading_func():
    collective_params = [
        algo.alpha.collective_jitter_recovery.calculate.CollectiveRecoveryTradingParam(
            algo.feature.collective_jitter.calculate.CollectiveJitterFeatureParam(window=40, collective_window=30),
            collective_drop_recovery_trading_param=algo.alpha.collective_jitter_recovery.calculate.CollectiveDropRecoveryTradingParam(
                -0.03, -0.30, -0.03, +0.005, -0.01),
            collective_jump_recovery_trading_param=None,
        ),
    ]
    collective_feature_labels = [
        algo.feature.collective_jitter.research.get_feature_label_for_caching(param.feature_param) for param in collective_params
    ]
    collective_trading_labels = [
        algo.alpha.collective_jitter_recovery.research.get_trading_label_for_caching(param) for param in collective_params
    ]
    return collective_params, collective_feature_labels, collective_trading_labels, algo.alpha.collective_jitter_recovery.research.get_dfst_trading


def _get_momentum_trading_param_labels_trading_func():
    trading_params = [
        algo.alpha.momentum.calculate.MomentumTradingParam(
            algo.feature.momentum.calculate.MomentumFeatureParam(window=180, ema_window=30), selection_size=2, rebalance_interval_minutes=3*60,
        ),
        algo.alpha.momentum.calculate.MomentumTradingParam(
            algo.feature.momentum.calculate.MomentumFeatureParam(window=360, ema_window=60), selection_size=2, rebalance_interval_minutes=6*60,
        ),
    ]
    collective_feature_labels = [
        algo.feature.momentum.research.get_feature_label_for_caching(param.feature_param) for param in trading_params
    ]
    collective_trading_labels = [
        algo.alpha.momentum.research.get_trading_label_for_caching(param) for param in trading_params
    ]
    return trading_params, collective_feature_labels, collective_trading_labels, algo.alpha.momentum.research.get_dfst_trading


def _get_trading_param_labels_get_dfst_trading_func(alpha_name: str):
    if alpha_name == 'jitter_reversal':
        return _get_jitter_trading_param_labels_trading_func()
    elif alpha_name == 'jitter_following':
        return _get_jitter_following_trading_param_labels_trading_func()
    elif alpha_name == 'collective_jitter':
        return _get_collective_trading_param_labels_trading_func()
    elif alpha_name == 'momentum':
        return _get_momentum_trading_param_labels_trading_func()
    else:
        return [], []


def verify_features_cache(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
    feature_name: str,
) -> None:
    _, labels, _ = _get_feature_param_labels_get_dfst_feature_func(feature_name)
    for label in labels:
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
    feature_name: str,
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

    feature_params, labels, get_dfst_feature_func = _get_feature_param_labels_get_dfst_feature_func(feature_name)
    do_cache(feature_params, labels, get_dfst_feature_func)


def verify_trading_cache(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
    alpha_name: str,
) -> None:
    _, _, labels, _ = _get_trading_param_labels_get_dfst_trading_func(alpha_name)
    for label in labels:
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
    alpha_name: str,
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

    trading_params, feature_labels, trading_labels, get_dfst_trading_func = _get_trading_param_labels_get_dfst_trading_func(alpha_name)
    do_cache(trading_params, feature_labels, trading_labels, get_dfst_trading_func)


def cache_all(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
    feature_name: str,
    alpha_name: str,
    if_cache_features=False,
    if_verify_features=False,
    if_cache_trading=False,
    if_verify_trading=False,
    symbol_filter=lambda s: s.endswith('USD'),
    value_column='close',
):
    aggregation_mode = market_data.ingest.bq.common.AGGREGATION_MODE.TAKE_LASTEST
    market_data.ingest.bq.cache.fetch_and_cache(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=dataset_mode, export_mode=export_mode,
        aggregation_mode=aggregation_mode,
    )
    market_data.ingest.bq.validate.verify_data_cache(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=dataset_mode, export_mode=export_mode,
        aggregation_mode=aggregation_mode,
    )

    if if_cache_features:
        cache_features(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
            feature_name=feature_name,
            symbol_filter=symbol_filter, value_column=value_column,
        )

    if if_verify_features:
        verify_features_cache(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
            feature_name=feature_name,
        )

    if if_cache_trading:
        cache_trading(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
            alpha_name=alpha_name,
        )

    if if_verify_trading:
        verify_trading_cache(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
            alpha_name=alpha_name,
        )


def run_okx(date_str_from: str, date_str_to: str, feature_name: str, alpha_name: str, if_cache_features=False, if_cache_trading=False, if_verify_features=False, if_verify_trading=False):
    cache_all(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.OKX,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
        feature_name=feature_name, alpha_name=alpha_name,
        if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
        symbol_filter=lambda s: s.endswith('-USDT-SWAP')
    )


def run_binance(date_str_from: str, date_str_to: str, feature_name: str, alpha_name: str, if_cache_features=False, if_cache_trading=False, if_verify_features=False, if_verify_trading=False):
    cache_all(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.BINANCE,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
        feature_name=feature_name, alpha_name=alpha_name,
        if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
        symbol_filter=lambda s: s.endswith('-USDT')
    )


def run_cex(date_str_from: str, date_str_to: str, feature_name: str, alpha_name: str, if_cache_features=False, if_cache_trading=False, if_verify_features=False, if_verify_trading=False):
    cache_all(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.CEX,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
        feature_name=feature_name, alpha_name=alpha_name,
        if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
    )


def run_gemini(date_str_from: str, date_str_to: str, feature_name: str, alpha_name: str, if_cache_features=False, if_cache_trading=False, if_verify_features=False, if_verify_trading=False):
    cache_all(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.GEMINI,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
        feature_name=feature_name, alpha_name=alpha_name,
        if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
    )


def run_bithumb(date_str_from: str, date_str_to: str, feature_name: str, alpha_name: str, if_cache_features=False, if_cache_trading=False, if_verify_features=False, if_verify_trading=False):
    cache_all(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.BITHUMB,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.ORDERBOOK_LEVEL1,
        feature_name=feature_name, alpha_name=alpha_name,
        if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
        symbol_filter=lambda s: s.endswith('-KRW'), value_column='price_ask',
    )


if __name__ == '__main__':
    date_str_from='2024-03-01'
    date_str_to='2024-03-31'
    date_str_from='2024-03-30'
    date_str_to='2024-05-10'
    feature_name='momentum'
    alpha_name='momentum'
    if_cache_features = True
    if_verify_features = False
    if_cache_trading = False
    if_verify_trading = False
    run_okx(date_str_from=date_str_from, date_str_to=date_str_to, feature_name=feature_name, alpha_name=alpha_name, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading)
    #run_cex(date_str_from=date_str_from, date_str_to=date_str_to, feature_name=feature_name, alpha_name=alpha_name, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading)
    #run_gemini(date_str_from=date_str_from, date_str_to=date_str_to, feature_name=feature_name, alpha_name=alpha_name, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading)
    #run_binance(date_str_from=date_str_from, date_str_to=date_str_to, feature_name=feature_name, alpha_name=alpha_name, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading)
    #run_bithumb(date_str_from=date_str_from, date_str_to=date_str_to, feature_name=feature_name, alpha_name=alpha_name, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading)
