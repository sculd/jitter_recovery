import datetime
import pandas as pd, numpy as np


date_str_20220901 = "2022-09-01"
date_str_20220919 = "2022-09-19"
date_str_20220920 = "2022-09-20"
date_str_20220921 = "2022-09-21"
date_str_20220922 = "2022-09-22"
date_str_20220923 = "2022-09-23"
date_str_20220924 = "2022-09-24"
date_str_20220925 = "2022-09-25"
date_str_20220930 = "2022-09-30"

date_str_20230801 = "2023-08-01"
date_str_20230803 = "2023-08-03"
date_str_20230806 = "2023-08-06"
date_str_20230809 = "2023-08-09"
date_str_20230810 = "2023-08-10"
date_str_20230811 = "2023-08-11"
date_str_20230812 = "2023-08-12"
date_str_20230813 = "2023-08-13"
date_str_20230814 = "2023-08-14"
date_str_20230815 = "2023-08-15"
date_str_20230816 = "2023-08-16"
date_str_20230831 = "2023-08-31"

date_str_20230901 = "2023-09-01"
date_str_20230930 = "2023-09-30"


base_okx = 'data/okx'
df_okx_20231127_1128 = pd.read_parquet(f'{base_okx}/df_okx_20231127_1128.parquet')
df_okx_20231126_1129 = pd.read_parquet(f'{base_okx}/df_okx_20231126_1129.parquet')
df_okx_20231126_1201 = pd.read_parquet(f'{base_okx}/df_okx_20231126_1201.parquet')
df_okx_20231125_1204 = pd.read_parquet(f'{base_okx}/df_okx_20231125_1204.parquet')
df_okx_20231205_1212 = pd.read_parquet(f'{base_okx}/df_okx_20231205_1212.parquet')
df_okx_20231125_1212 = pd.read_parquet(f'{base_okx}/df_okx_20231125_1212.parquet')


def get_close_between_datetime(df, sample_period_minutes, symbols, start_datetime_str, end_datetime_str, if_2023=True):
    df_between = df[(df.index >= start_datetime_str) & (df.index < end_datetime_str)][symbols].resample(f'{sample_period_minutes}min').last().dropna()
    return df_between


def get_close_between_date(df, sample_period_minutes, symbols, start_date_str, end_date_str, if_2023=True):   
    return get_close_between_datetime(df, sample_period_minutes, symbols, start_date_str + " 00:00:000", end_date_str + " 00:00:000", if_2023=if_2023)

