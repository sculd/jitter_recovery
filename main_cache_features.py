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
    feature_params = [
        algo.jitter_recovery.calculate.JitterRecoveryFeatureParam(30),
        algo.jitter_recovery.calculate.JitterRecoveryFeatureParam(40),
        algo.jitter_recovery.calculate.JitterRecoveryFeatureParam(240),
    ]
    labels = [
        algo.jitter_recovery.research.get_feature_label_for_caching(feature_param) for feature_param in feature_params
    ]

    collective_feature_params = [
        algo.collective_jitter_recovery.calculate.CollectiveRecoveryFeatureParam(window=40, collective_window=30),
    ]
    collective_labels = [
        algo.collective_jitter_recovery.research.get_feature_label_for_caching(feature_param) for feature_param in collective_feature_params
    ]
    return feature_params + collective_feature_params, labels + collective_labels



def verify_features(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
):
    feature_params, labels = _get_feature_param_labels()
    for feature_param, label in zip(feature_params, labels):
        logging.info(f"for {label}")
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
):
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
    verify_features(
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
