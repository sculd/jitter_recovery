import logging
import typing
import types

import market_data.ingest.bq.cache
import market_data.ingest.bq.common
import market_data.ingest.util.time


def verify_cache(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
    aggregation_mode: market_data.ingest.bq.common.AGGREGATION_MODE,
    labels: typing.List[str],
) -> None:
    for label in labels:
        logging.info(f"verify cache for {label}")
        market_data.ingest.bq.cache.validate_df(
            label=label,
            date_str_from=date_str_from,
            date_str_to=date_str_to,
            dataset_mode=dataset_mode,
            export_mode=export_mode,
            aggregation_mode=aggregation_mode,
        )


def cache_features(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
    aggregation_mode: market_data.ingest.bq.common.AGGREGATION_MODE,
    feature_params: typing.List,
    labels: typing.List,
    get_dfst_feature_func: types.FunctionType,
    symbol_filter=None,
    value_column='close',
) -> None:
    df = market_data.ingest.bq.cache.read_from_cache(
        dataset_mode=dataset_mode,
        export_mode=export_mode,
        aggregation_mode=aggregation_mode,
        label=market_data.ingest.bq.cache._label_market_data,
        date_str_from=date_str_from,
        date_str_to=date_str_to)

    if df is None:
        logging.error(f"the market data for cache_features for {dataset_mode} {export_mode} {aggregation_mode} is not available")
        return

    df = df.reset_index()

    for feature_param, label in zip(feature_params, labels):
        logging.info(f"for {label}")
        dfst_feature = get_dfst_feature_func(df, feature_param, symbol_filter=symbol_filter, value_column=value_column)
        market_data.ingest.bq.cache.cache_df(
            dfst_feature,
            label=label,
            dataset_mode=dataset_mode,
            export_mode=export_mode,
            aggregation_mode=aggregation_mode,
            overwrite=True)
        del dfst_feature


def cache_trading(
    date_str_from: str,
    date_str_to: str,
    dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
    export_mode: market_data.ingest.bq.common.EXPORT_MODE,
    aggregation_mode: market_data.ingest.bq.common.AGGREGATION_MODE,
    trading_params: typing.List,
    feature_labels: typing.List,
    trading_labels: typing.List,
    get_dfst_trading_func: types.FunctionType,
) -> None:
    for trading_param, feature_label, trading_label in zip(trading_params, feature_labels, trading_labels):
        logging.info(f"for {trading_label}")
        dfst_feature = market_data.ingest.bq.cache.read_from_cache(
            dataset_mode=dataset_mode,
            export_mode=export_mode,
            aggregation_mode=aggregation_mode,
            label=feature_label,
            date_str_from=date_str_from,
            date_str_to=date_str_to)
        if dfst_feature is None:
            logging.error(f"feature for {feature_label} can not be found in the cache.")
            continue
        dfst_trading = get_dfst_trading_func(dfst_feature, trading_param)
        del dfst_feature
        market_data.ingest.bq.cache.cache_df(
            dfst_trading,
            label=trading_label,
            dataset_mode=dataset_mode,
            export_mode=export_mode,
            aggregation_mode=aggregation_mode,
            overwrite=True)
        del dfst_trading

