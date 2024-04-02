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

    collective_params = [
        algo.collective_jitter_recovery.calculate.CollectiveRecoveryFeatureParam(window=40, collective_window=30),
    ]
    collective_labels = [
        algo.collective_jitter_recovery.research.get_feature_label_for_caching(param) for param in collective_params
    ]
    return labels + collective_labels


def verify_features_cache(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
) -> None:
    labels = _get_feature_param_labels()
    for label in labels:
        logging.info(f"for feature {label}")
        algo.cache.validate_df(
            label=label,
            date_str_from=date_str_from,
            date_str_to=date_str_to,
            dataset_mode=dataset_mode,
            export_mode=export_mode,
        )


def _get_trading_param_labels():
    params = [
        algo.jitter_recovery.calculate.JitterRecoveryTradingParam(
            algo.jitter_recovery.calculate.JitterRecoveryFeatureParam(30),
            0.20, -0.04, 0.02, is_long_term=False),
    ]
    labels = [
        algo.jitter_recovery.research.get_trading_label_for_caching(param) for param in params
    ]

    collective_params = [
        algo.collective_jitter_recovery.calculate.CollectiveRecoveryTradingParam(
            algo.collective_jitter_recovery.calculate.CollectiveRecoveryFeatureParam(window=40, collective_window=30),
            collective_drop_recovery_trading_param=algo.collective_jitter_recovery.calculate.CollectiveDropRecoveryTradingParam(
                -0.03, -0.30, -0.03, +0.005, -0.01),
            collective_jump_recovery_trading_param=None,
        ),
    ]
    collective_labels = [
        algo.collective_jitter_recovery.research.get_trading_label_for_caching(trading_param) for trading_param in collective_params
    ]
    return labels + collective_labels


def verify_trading_cache(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
) -> None:
    labels = _get_trading_param_labels()
    for label in labels:
        logging.info(f"for trading {label}")
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

    feature_params, labels = _get_feature_param_labels()
    for feature_param, label in zip(feature_params, labels):
        logging.info(f"for {label}")
        dfst_feature = algo.jitter_recovery.research.get_dfst_feature(df, feature_param)
        algo.cache.cache_df(
            dfst_feature,
            label = label,
            dataset_mode=dataset_mode,
            export_mode=export_mode,
            overwrite = True)

        del dfst_feature


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
    '''
