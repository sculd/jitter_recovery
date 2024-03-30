import pandas as pd, numpy as np
import datetime
import logging
import typing
import os

import market_data.ingest.bq.cache
import market_data.ingest.bq.common
import market_data.ingest.util.time

# the cache will be stored per day.
_cache_interval = datetime.timedelta(days=1)
_full_day_check_grace_period = datetime.timedelta(minutes=10)

_cache_base_path = os.path.expanduser('~/feature_data')
try:
    os.mkdir(_cache_base_path)
except FileExistsError:
    pass

_timestamp_index_name = 'timestamp'


def _get_filename(label: str, t_id: str, t_from: datetime.datetime, t_to: datetime.datetime) -> str:
    feature_dir = os.path.join(_cache_base_path, label)
    try:
        os.makedirs(feature_dir, exist_ok=True)
    except FileExistsError:
        pass

    t_str_from = t_from.strftime("%Y-%m-%dT%H:%M:%S%z")
    t_str_to = t_to.strftime("%Y-%m-%dT%H:%M:%S%z")
    return os.path.join(feature_dir, f"{t_id}_{t_str_from}_{t_str_to}.parquet")

def _timestamp_covers_full_day(first_t: datetime.datetime, last_t: datetime.datetime, grace_period: datetime.timedelta=_full_day_check_grace_period) -> bool:
    def _is_t_begin_of_day(t) -> bool:
        return t - datetime.datetime(year=t.year, month=t.month, day=t.day, hour=0, minute=0, second=0, tzinfo=t.tzinfo) <= grace_period

    def _is_t_end_of_day(t) -> bool:
        return datetime.datetime(year=t.year, month=t.month, day=t.day, hour=23, minute=59, second=0, tzinfo=t.tzinfo) - t <= grace_period

    return _is_t_begin_of_day(first_t) and _is_t_end_of_day(last_t)


def _timerange_str_for_timerange(first_t: datetime.datetime, last_t: datetime.datetime) -> str:
    def _date_str_from_t(t) -> str:
        return  f"{t.year}-{t.month}-{t.day}"
    return f"{_date_str_from_t(first_t)}_{_date_str_from_t(last_t)}"


def _split_df_by_day(df: pd.DataFrame) -> typing.List[pd.DataFrame]:
    dfs = [group[1] for group in df.groupby(df.index.get_level_values(_timestamp_index_name).date)]
    return dfs


def _cache_df_daily(df_daily: pd.DataFrame, label: str, t_id: str, overwrite=True):
    timestamps = df_daily.index.get_level_values(_timestamp_index_name).unique()
    t_begin = market_data.ingest.bq.cache._anchor_to_begin_of_day(timestamps[0])
    t_end = market_data.ingest.bq.cache._anchor_to_begin_of_day(t_begin + _cache_interval)

    filename = _get_filename(label, t_id, t_begin, t_end)
    if os.path.exists(filename):
        logging.info(f"{filename} already exists.")
        if overwrite:
            logging.info(f"and would overwrite it.")
            df_daily.to_parquet(filename)
        else:
            logging.info(f"and would not write it.")
    else:
        df_daily.to_parquet(filename)


def _read_df_daily(label: str, t_id: str, t_from: datetime.datetime, t_to: datetime.datetime) -> typing.Optional[pd.DataFrame]:
    if not market_data.ingest.bq.cache._is_exact_cache_interval(t_from, t_to):
        logging.info(f"{t_from} to {t_to} do not match a full day thus will not read from the cache.")
        return None
    filename = _get_filename(label, t_id, t_from, t_to)
    if not os.path.exists(filename):
        logging.info(f"{filename=} does not exist.")
        return None
    return pd.read_parquet(filename)


def cache_df(
        df: pd.DataFrame,
        label: str,
        dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
        export_mode: market_data.ingest.bq.common.EXPORT_MODE,
        overwrite = False,
        skip_first_day = True,
) -> None:
    t_id = market_data.ingest.bq.common.get_full_table_id(dataset_mode, export_mode)
    df_dailys = _split_df_by_day(df)
    for i, df_daily in enumerate(df_dailys):
        if skip_first_day and i == 0:
            continue
        _cache_df_daily(df_daily, label, t_id, overwrite=overwrite)
        del df_daily


def read_df(
        label: str,
        dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
        export_mode: market_data.ingest.bq.common.EXPORT_MODE,
        t_from: datetime.datetime = None,
        t_to: datetime.datetime = None,
        epoch_seconds_from: int = None,
        epoch_seconds_to: int = None,
        date_str_from: str = None,
        date_str_to: str = None,
        ) -> pd.DataFrame:
    t_id = market_data.ingest.bq.common.get_full_table_id(dataset_mode, export_mode)
    t_from, t_to = market_data.ingest.util.time.to_t(
        t_from=t_from,
        t_to=t_to,
        epoch_seconds_from=epoch_seconds_from,
        epoch_seconds_to=epoch_seconds_to,
        date_str_from=date_str_from,
        date_str_to=date_str_to,
    )
    t_ranges = market_data.ingest.bq.cache._split_t_range(t_from, t_to)
    df_concat = None
    for t_range in t_ranges:
        df_cache = _read_df_daily(label, t_id, t_range[0], t_range[-1])
        if df_cache is None:
            logging.info(f"df_cache is None for {t_range}")
            continue
        if df_concat is None:
            df_concat = df_cache.copy()
        else:
            df_concat = pd.concat([df_concat, df_cache])
        del df_cache

    return df_concat
