import pandas as pd, numpy as np
import datetime
import logging
import typing
import os

import market_data.ingest.bq.cache
import market_data.ingest.bq.common
import market_data.ingest.util.time
from google.cloud import storage

# the cache will be stored per day.
_cache_interval = datetime.timedelta(days=1)
_full_day_check_grace_period = datetime.timedelta(minutes=10)

_cache_base_path = os.path.expanduser(f'~/algo_cache')
try:
    os.mkdir(_cache_base_path)
except FileExistsError:
    pass


_storage_client = storage.Client()
_gcs_bucket_name = "algo_cache"
_gcs_bucket = _storage_client.bucket(_gcs_bucket_name)

_timestamp_index_name = 'timestamp'


def _get_filename(label: str, t_id: str, t_from: datetime.datetime, t_to: datetime.datetime) -> str:
    feature_dir = os.path.join(_cache_base_path, label)
    t_str_from = t_from.strftime("%Y-%m-%dT%H:%M:%S%z")
    t_str_to = t_to.strftime("%Y-%m-%dT%H:%M:%S%z")
    r = os.path.join(feature_dir, f"{t_id}/{t_str_from}_{t_str_to}.parquet")
    dir = os.path.dirname(r)
    try:
        os.makedirs(dir, exist_ok=True)
    except FileExistsError:
        pass

    return r

def _get_gcsblobname(label: str, t_id: str, t_from: datetime.datetime, t_to: datetime.datetime) -> str:
    t_str_from = t_from.strftime("%Y-%m-%dT%H:%M:%S%z")
    t_str_to = t_to.strftime("%Y-%m-%dT%H:%M:%S%z")
    return os.path.join(label, f"{t_id}/{t_str_from}_{t_str_to}.parquet")

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


def _upload_file_to_public_gcs_bucket(local_filename, gcs_filename, rewrite=False) -> None:
    # uoload the file to gcs
    if storage.Blob(bucket=_gcs_bucket, name=gcs_filename).exists(_storage_client):
        if rewrite:
            blob = _gcs_bucket.blob(gcs_filename)
            blob.delete(if_generation_match=None)
        else:
            print(f'{gcs_filename} already present in the bucket {_gcs_bucket_name} thus not proceeding further for {property}')
            return

    blob = _gcs_bucket.blob(gcs_filename)
    generation_match_precondition = 0
    blob.upload_from_filename(local_filename, if_generation_match=generation_match_precondition)

    print(
        f"File {local_filename} uploaded to {_gcs_bucket_name}/{gcs_filename}."
    )


def _cache_df_daily(df_daily: pd.DataFrame, label: str, t_id: str, overwrite=True):
    if len(df_daily) == 0:
        logging.info(f"df_daily is empty thus will be skipped.")
        return
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

    blob_name = _get_gcsblobname(label, t_id, t_begin, t_end)
    _upload_file_to_public_gcs_bucket(filename, blob_name, rewrite=overwrite)

def _download_gcs_blob(source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # source_blob_name = "storage-object-name"

    # The path to which the file should be downloaded
    # destination_file_name = "local/path/to/file"

    bucket = _storage_client.bucket(_gcs_bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        f"Downloaded storage object {source_blob_name} from bucket {_gcs_bucket_name} to local file {destination_file_name}."
    )

def _read_df_daily(
        label: str,
        t_id: str,
        t_from: datetime.datetime,
        t_to: datetime.datetime,
        columns: typing.List[str] = None,
) -> typing.Optional[pd.DataFrame]:
    if not market_data.ingest.bq.cache._is_exact_cache_interval(t_from, t_to):
        logging.info(f"{t_from} to {t_to} do not match a full day thus will not read from the cache.")
        return None
    filename = _get_filename(label, t_id, t_from, t_to)
    if not os.path.exists(filename):
        blob_name = _get_gcsblobname(label, t_id, t_from, t_to)
        blob_exist = storage.Blob(bucket=_gcs_bucket, name=blob_name).exists(_storage_client)
        logging.info(f"{filename=} does not exist in local cache. For gcs, {blob_exist=}.")
        if blob_exist:
            _download_gcs_blob(blob_name, filename)
        return None
    df = pd.read_parquet(filename)
    if len(df) == 0:
        return None

    if columns is None:
        return df
    else:
        columns = [c for c in columns if c in df.columns]
        return df[columns]


def cache_df(
        df: pd.DataFrame,
        label: str,
        dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
        export_mode: market_data.ingest.bq.common.EXPORT_MODE,
        overwrite = False,
        skip_first_day = True,
) -> None:
    if len(df) == 0:
        logging.info(f"df is empty for {label} thus will be skipped.")
        return
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
        columns: typing.List[str] = None,
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
    t_ranges = market_data.ingest.bq.cache.split_t_range(t_from, t_to)
    df_concat = None
    for t_range in t_ranges:
        df_cache = _read_df_daily(label, t_id, t_range[0], t_range[-1], columns=columns)
        if df_cache is None:
            logging.info(f"df_cache is None for {t_range}")
            continue
        if df_concat is None:
            df_concat = df_cache.copy()
        else:
            df_concat = pd.concat([df_concat, df_cache])
        del df_cache

    return df_concat


def validate_df(
        label: str,
        dataset_mode: market_data.ingest.bq.common.DATASET_MODE,
        export_mode: market_data.ingest.bq.common.EXPORT_MODE,
        t_from: datetime.datetime = None,
        t_to: datetime.datetime = None,
        epoch_seconds_from: int = None,
        epoch_seconds_to: int = None,
        date_str_from: str = None,
        date_str_to: str = None,
        ) -> None:
    t_id = market_data.ingest.bq.common.get_full_table_id(dataset_mode, export_mode)
    t_from, t_to = market_data.ingest.util.time.to_t(
        t_from=t_from,
        t_to=t_to,
        epoch_seconds_from=epoch_seconds_from,
        epoch_seconds_to=epoch_seconds_to,
        date_str_from=date_str_from,
        date_str_to=date_str_to,
    )
    t_ranges = market_data.ingest.bq.cache.split_t_range(t_from, t_to)
    for t_range in t_ranges:
        t_from, t_to = t_range[0], t_range[-1]
        filename = _get_filename(label, t_id, t_from, t_to)
        if not os.path.exists(filename):
            blob_name = _get_gcsblobname(label, t_id, t_from, t_to)
            blob_exist = storage.Blob(bucket=_gcs_bucket, name=blob_name).exists(_storage_client)
            logging.info(f"{filename=} does not exist in local cache. For gcs, {blob_exist=}.")
            if blob_exist:
                _download_gcs_blob(blob_name, filename)
                print(f'after download exist: {os.path.exists(filename)} for {filename}')
