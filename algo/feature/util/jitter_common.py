import pandas as pd, numpy as np
import numba
from numba import njit
import typing

default_window_minutes = 5

@njit
def get_ch(v1: np.array, v2: np.array) -> np.array:
    return np.where(v1 != 0, (v2 - v1) / v1, 0)


def rows_to_dataframe(rows: typing.List, index):
    null_row_vals = {}
    for r in rows:
        if r is None: continue
        null_row_vals = {k: None for k in r.keys()}
        break
    rows = [null_row_vals if r is None else r for r in rows]
    return pd.DataFrame(rows, index=index)
