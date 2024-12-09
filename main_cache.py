import datetime
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
import market_data.ingest.util.time
import algo.cache
import algo.feature.jitter.calculate
import algo.feature.simple_jitter.calculate
import algo.feature.collective_jitter.calculate
import algo.feature.momentum.calculate
import algo.feature.jitter.research
import algo.feature.simple_jitter.research
import algo.feature.collective_jitter.research
import algo.feature.momentum.research
import algo.alpha.jitter_recovery.calculate
import algo.alpha.jitter_simple_reversal.calculate
import algo.alpha.jitter_following.calculate
import algo.alpha.collective_jitter_recovery.calculate
import algo.alpha.momentum.calculate
import algo.alpha.momentum_reversal.calculate
import algo.alpha.jitter_recovery.research
import algo.alpha.jitter_simple_reversal.research
import algo.alpha.jitter_following.research
import algo.alpha.collective_jitter_recovery.research
import algo.alpha.momentum.research
import algo.alpha.momentum_reversal.research


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

def _get_simple_jitter_feature_param_labels_get_dfst_feature_func():
    params = [
        algo.feature.simple_jitter.calculate.SimpleJitterFeatureParam(30),
    ]
    labels = [
        algo.feature.simple_jitter.research.get_feature_label_for_caching(param) for param in params
    ]
    return params, labels, algo.feature.simple_jitter.research.get_dfst_feature

def _get_collective_feature_param_labels_get_dfst_feature_func():
    collective_params = [
        algo.feature.collective_jitter.calculate.CollectiveJitterFeatureParam(window=40, collective_window=30),
    ]
    collective_labels = [
        algo.feature.collective_jitter.research.get_feature_label_for_caching(param) for param in collective_params
    ]
    return collective_params, collective_labels, algo.feature.collective_jitter.research.get_dfst_feature


def _get_feature_param_labels_get_dfst_feature_func(feature_name: str):
    if feature_name == 'jitter':
        return _get_jitter_feature_param_labels_get_dfst_feature_func()
    elif feature_name == 'simple_jitter':
        return _get_simple_jitter_feature_param_labels_get_dfst_feature_func()
    elif feature_name == 'collective_jitter':
        return _get_collective_feature_param_labels_get_dfst_feature_func()
    else:
        return [], [], []


def _get_jitter_reversal_trading_param_labels_trading_func():
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


def _get_jitter_simple_reversal_trading_param_labels_trading_func():
    params = [
        algo.alpha.jitter_simple_reversal.calculate.JitterSimpleReversalTradingParam(
            algo.feature.jitter.calculate.JitterFeatureParam(30),
            jump_threshold=0.18, drop_from_jump_threshold=-0.02),
    ]
    feature_labels = [
        algo.feature.jitter.research.get_feature_label_for_caching(param.feature_param) for param in params
    ]
    trading_labels = [
        algo.alpha.jitter_simple_reversal.research.get_trading_label_for_caching(param) for param in params
    ]
    return params, feature_labels, trading_labels, algo.alpha.jitter_simple_reversal.research.get_dfst_trading


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


def _get_trading_param_labels_get_dfst_trading_func(alpha_name: str):
    if alpha_name == 'jitter_reversal':
        return _get_jitter_reversal_trading_param_labels_trading_func()
    elif alpha_name == 'jitter_simple_reversal':
        return _get_jitter_simple_reversal_trading_param_labels_trading_func()
    elif alpha_name == 'collective_jitter_reversal':
        return _get_collective_trading_param_labels_trading_func()
    else:
        return [], [], [], []


def cache_all(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
    feature_name: str,
    alpha_name: str,
    if_cache_market_data=False,
    if_verify_market_data=False,
    if_cache_features=False,
    if_verify_features=False,
    if_cache_trading=False,
    if_verify_trading=False,
    symbol_filter=lambda s: s.endswith('USD'),
    value_column='close',
):
    print(f"{date_str_from=} {date_str_to=}")
    aggregation_mode = market_data.ingest.bq.common.AGGREGATION_MODE.TAKE_LASTEST

    if if_cache_market_data:
        market_data.ingest.bq.cache.read_from_cache_or_query_and_cache(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
            aggregation_mode=aggregation_mode,
            label=market_data.ingest.bq.cache._label_market_data,
        )

    if if_verify_market_data:
        algo.cache.verify_cache(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
            aggregation_mode=aggregation_mode,
            labels=[market_data.ingest.bq.cache._label_market_data],
        )

    if if_cache_features:
        feature_params, labels, get_dfst_feature_func = _get_feature_param_labels_get_dfst_feature_func(feature_name)
        algo.cache.cache_features(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
            aggregation_mode=market_data.ingest.bq.common.AGGREGATION_MODE.TAKE_LASTEST,
            feature_params=feature_params,
            labels=labels,
            get_dfst_feature_func=get_dfst_feature_func,
            symbol_filter=symbol_filter,
            value_column=value_column,
        )

    if if_verify_features:
        _, labels, _ = _get_feature_param_labels_get_dfst_feature_func(feature_name)
        algo.cache.verify_cache(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
            aggregation_mode=aggregation_mode,
            labels=labels,
        )

    if if_cache_trading:
        trading_params, feature_labels, trading_labels, get_dfst_trading_func = _get_trading_param_labels_get_dfst_trading_func(alpha_name)
        algo.cache.cache_trading(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
            aggregation_mode=market_data.ingest.bq.common.AGGREGATION_MODE.TAKE_LASTEST,
            trading_params = trading_params,
            feature_labels = feature_labels,
            trading_labels = trading_labels,
            get_dfst_trading_func = get_dfst_trading_func,
        )

    if if_verify_trading:
        _, _, trading_labels, _ = _get_trading_param_labels_get_dfst_trading_func(alpha_name)
        algo.cache.verify_cache(
            date_str_from=date_str_from, date_str_to=date_str_to,
            dataset_mode=dataset_mode, export_mode=export_mode,
            aggregation_mode=aggregation_mode,
            labels=trading_labels,
        )


def run_for(
        date_str_from: str, date_str_to: str,
        dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
        export_mode: market_data.ingest.bq.common.EXPORT_MODE,
        feature_name: str, alpha_name: str,
        if_cache_market_data = False, if_verify_market_data = False,
        if_cache_features=False, if_cache_trading=False,
        if_verify_features=False, if_verify_trading=False,
        symbol_filter=lambda s: s.endswith('USDT-SWAP'),
):
    cache_all(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=dataset_mode,
        export_mode=export_mode,
        feature_name=feature_name, alpha_name=alpha_name,
        if_cache_market_data=if_cache_market_data, if_verify_market_data=if_verify_market_data,
        if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
        symbol_filter=symbol_filter
    )


def run_for_multiple_batches(
        date_str_from: str, date_str_to: str,
        dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
        batch_size_days: int,
        export_mode: market_data.ingest.bq.common.EXPORT_MODE,
        feature_name: str, alpha_name: str,
        if_cache_market_data = False, if_verify_market_data = False,
        if_cache_features=False, if_cache_trading=False,
        if_verify_features=False, if_verify_trading=False,
        symbol_filter=lambda s: s.endswith('USDT-SWAP'),
):
    t_from, t_to = market_data.ingest.util.time.to_t(date_str_from=date_str_from, date_str_to=date_str_to)
    t_head = t_from
    t_tail = t_to
    while True:
        t_tail = min(t_to, t_tail)

        cache_all(
            date_str_from=t_head.strftime("%Y-%m-%d"), date_str_to=t_tail.strftime("%Y-%m-%d"),
            dataset_mode=dataset_mode,
            export_mode=export_mode,
            feature_name=feature_name, alpha_name=alpha_name,
            if_cache_market_data=if_cache_market_data, if_verify_market_data=if_verify_market_data,
            if_cache_features=if_cache_features, if_cache_trading=if_cache_trading,
            if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
            symbol_filter=symbol_filter
        )

        if t_tail >= t_to:
            break

        t_head = t_tail
        t_tail = t_head + datetime.timedelta(days=batch_size_days)


if __name__ == '__main__':
    feature_name='collective_jitter'
    alpha_name='collective_jitter_reversal'
    print(f"{feature_name=} {alpha_name=}")
    if_cache_market_data = True
    if_verify_market_data = True
    if_cache_features = True
    if_verify_features = True
    if_cache_trading = True
    if_verify_trading = True

    date_str_from='2024-11-21'
    date_str_to='2024-11-30'
    run_for(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.OKX, export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
        feature_name=feature_name, alpha_name=alpha_name, if_cache_market_data=if_cache_market_data, if_verify_market_data=if_verify_market_data, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
        symbol_filter=lambda s: s.endswith('USDT-SWAP'),
    )

    date_str_from='2024-11-29'
    date_str_to='2024-12-07'
    run_for(
        date_str_from=date_str_from, date_str_to=date_str_to,
        dataset_mode=market_data.ingest.bq.common.DATASET_MODE.OKX, export_mode=market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE,
        feature_name=feature_name, alpha_name=alpha_name, if_cache_market_data=if_cache_market_data, if_verify_market_data=if_verify_market_data, if_cache_features=if_cache_features, if_cache_trading=if_cache_trading, if_verify_features=if_verify_features, if_verify_trading=if_verify_trading,
        symbol_filter=lambda s: s.endswith('USDT-SWAP'),
    )

    exit(0)
