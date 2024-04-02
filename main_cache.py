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
import algo.jitter_recovery.calculate
import algo.jitter_recovery.research
import algo.collective_jitter_recovery.calculate
import algo.collective_jitter_recovery.research
import algo.cache


def _get_feature_param_labels():
    params = [
        algo.jitter_recovery.calculate.JitterRecoveryFeatureParam(30),
        algo.jitter_recovery.calculate.JitterRecoveryFeatureParam(40),
        algo.jitter_recovery.calculate.JitterRecoveryFeatureParam(240),
    ]
    labels = [
        algo.jitter_recovery.research.get_feature_label_for_caching(param) for param in params
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
) -> None:
    df = market_data.ingest.bq.cache.fetch_and_cache(
        dataset_mode,
        export_mode,
        market_data.ingest.bq.common.AGGREGATION_MODE.TAKE_LASTEST,
        date_str_from=date_str_from, date_str_to=date_str_to).reset_index()

    def do_cache(feature_params, labels, get_dfst_feature_func):
        for feature_param, label in zip(feature_params, labels):
            logging.info(f"for {label}")
            dfst_feature = get_dfst_feature_func(df, feature_param)
            algo.cache.cache_df(
                dfst_feature,
                label=label,
                dataset_mode=dataset_mode,
                export_mode=export_mode,
                overwrite=True)
            del dfst_feature

    feature_params, labels = _get_feature_param_labels()
    do_cache(feature_params, labels, algo.jitter_recovery.research.get_dfst_feature)

    feature_params, labels = _get_collective_feature_param_labels()
    do_cache(feature_params, labels, algo.collective_jitter_recovery.research.get_dfst_feature)

def _get_trading_param_labels():
    params = [
        algo.jitter_recovery.calculate.JitterRecoveryTradingParam(
            algo.jitter_recovery.calculate.JitterRecoveryFeatureParam(30),
            0.20, -0.04, 0.02, is_long_term=False),
    ]
    feature_labels = [
        algo.jitter_recovery.research.get_feature_label_for_caching(param.feature_param) for param in params
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


if __name__ == '__main__':
    verify_features_cache(
        date_str_from='2024-01-02',
        date_str_to='2024-03-21',
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.OKX,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
    )
    verify_trading_cache(
        date_str_from='2024-01-02',
        date_str_to='2024-03-21',
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.OKX,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
    )
    '''
    cache_features(
        date_str_from='2024-03-09',
        date_str_to='2024-03-12',
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.OKX,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
    )
    #'''
    '''
    cache_trading(
        date_str_from='2024-03-09',
        date_str_to='2024-03-11',
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.OKX,
        export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
    )
    #'''
