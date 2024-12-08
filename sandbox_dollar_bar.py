import pandas as pd
import datetime
import logging
import typing
from collections import defaultdict

from market_data.ingest.util import time as util_time

from market_data.ingest.bq.common import AGGREGATION_MODE
import market_data.ingest.bq.common
import market_data.ingest.bq.candle


def _fetch_dollar_candle_datetime(t_id: str, t_from: datetime.datetime, t_to: datetime.datetime) -> pd.DataFrame:
    logging.debug(
        f'fetching dollar candles from {t_from} to {t_to}')

    query_template = market_data.ingest.bq.candle._get_query_template(AGGREGATION_MODE.COLLECT_ALL_UPDATES)
    query_str = query_template.format(
        t_id=t_id,
        t_str_from=util_time.t_to_bq_t_str(t_from),
        t_str_to=util_time.t_to_bq_t_str(t_to),
    )
    df = market_data.ingest.bq.common.run_query(query_str, timestamp_columnname="ingestion_timestamp")
    df = df.reset_index().set_index("timestamp")
    return df


def fetch_dollar_candle(
        t_id: str,
        t_from: datetime.datetime = None,
        t_to: datetime.datetime = None,
        epoch_seconds_from: int = None,
        epoch_seconds_to: int = None,
        date_str_from: str = None,
        date_str_to: str = None,
        ) -> pd.DataFrame:
    t_from, t_to = util_time.to_t(
        t_from=t_from,
        t_to=t_to,
        epoch_seconds_from=epoch_seconds_from,
        epoch_seconds_to=epoch_seconds_to,
        date_str_from=date_str_from,
        date_str_to=date_str_to,
    )

    return _fetch_dollar_candle_datetime(t_id, t_from, t_to)


dataset_mode = market_data.ingest.bq.common.DATASET_MODE.OKX
export_mode = market_data.ingest.bq.common.EXPORT_MODE.BY_MINUTE
t_id = market_data.ingest.bq.common.get_full_table_id(dataset_mode, export_mode)
date_str_from='2024-11-26'
date_str_to='2024-11-27'
fetch_dollar_candle(t_id, date_str_from=date_str_from, date_str_to=date_str_to)

df = pd.read_parquet("okx_raw_1minutes.parquet")

