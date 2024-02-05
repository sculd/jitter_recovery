import pandas as pd, numpy as np
from collections import defaultdict
from numba import njit


defaultjump_window = 60
defaultjump_window_longterm = 240

default_jump_threshold, default_drop_from_jump_threshold, default_exit_jumpt_threshold = 0.20, -0.04, 0.02
default_jump_threshold_longterm, default_drop_from_jump_threshold_longterm, default_exit_jumpt_threshold_longterm = 0.40, -0.10, 0.05


class JitterRecoveryFeatureParam:
    def __init__(self, jump_window):
        self.jump_window = jump_window

    def get_default_param():
        return JitterRecoveryFeatureParam(
            defaultjump_window)

    def get_default_param_longterm():
        return JitterRecoveryFeatureParam(
            defaultjump_window_longterm)


class JitterRecoveryTradingParam:
    def __init__(self, jitter_recovery_feature_param, jump_threshold, drop_from_jump_threshold, exit_jumpt_threshold, is_long_term):
        self.jitter_recovery_feature_param = jitter_recovery_feature_param
        self.jump_threshold= jump_threshold
        self.drop_from_jump_threshold = drop_from_jump_threshold
        self.exit_jumpt_threshold = exit_jumpt_threshold
        self.is_long_term = is_long_term

    def get_default_param():
        return JitterRecoveryTradingParam(
            JitterRecoveryFeatureParam.get_default_param(), default_jump_threshold, default_drop_from_jump_threshold, default_exit_jumpt_threshold, is_long_term = False)

    def get_default_param_longterm():
        return JitterRecoveryTradingParam(
            JitterRecoveryFeatureParam.get_default_param_longterm(), default_jump_threshold_longterm, default_drop_from_jump_threshold_longterm, default_exit_jumpt_threshold_longterm, is_long_term = True)


@njit
def _get_ch(v1: float, v2: float) -> float:
    if v1 == 0:
        return 0
    return (v2 - v1) / v1

@njit
def get_changes_1dim(values):
    '''
    values is a 1 dimensional array.
    '''
    l = values.shape[0]
    if l < 1: return None

    '''
    if len(values_argg.shape) == 2:
        values = np.array([v[0] for v in values_argg])
    else:
        values = values_argg
    '''

    ch_max = 0
    ch_min = 0
    distance_max_ch = 1
    distance_min_ch = 1
    ch_since_max, ch_since_min = 0, 0

    first_v, last_v = values[0], values[-1]
    max_v, min_v = values[0], values[0]
    sum_v = 0
    avg_v_before_max_ch, avg_v_before_min_ch = 0, 0
    v_ch_max_is_to, v_ch_min_is_to = max_v, min_v
    v_ch_max_is_from, v_ch_min_is_from = max_v, min_v

    for i, v in enumerate(values):
        min_v, max_v = min(min_v, v), max(max_v, v)
        sum_v += v
        avg_v = sum_v * 1.0 / (i+1)

        ch_jump = _get_ch(min_v, v)
        ch_drop = _get_ch(max_v, v)
        
        ch = _get_ch(first_v, v)
        ch_since = _get_ch(v, last_v)

        d =  l-1-i

        if ch_max <= ch_jump:
            distance_max_ch, ch_since_max, ch_max = d, ch_since, ch_jump
            v_ch_max_is_from = min_v
            v_ch_max_is_to = v
            avg_v_before_max_ch = avg_v

        if ch_min >= ch_drop:
            distance_min_ch, ch_since_min, ch_min = d, ch_since, ch_drop
            v_ch_min_is_from = max_v
            v_ch_min_is_to = v
            avg_v_before_min_ch = avg_v


    return {
        'value': values[-1],
        'ch_max': ch_max, 'ch_min': ch_min,
        'avg_v_before_max_ch': avg_v_before_max_ch,
        'avg_v_before_min_ch': avg_v_before_min_ch,
        'v_ch_max_is_from': v_ch_max_is_from, 'v_ch_min_is_from': v_ch_min_is_from,
        'v_ch_max_is_to': v_ch_max_is_to, 'v_ch_min_is_to': v_ch_min_is_to,
        'ch_since_max': ch_since_max, 'ch_since_min': ch_since_min,
        'distance_max_ch': distance_max_ch, 'distance_min_ch': distance_min_ch,
        }


def get_feature_df(dfs, jitter_recovery_feature_param):
    window = jitter_recovery_feature_param.jump_window
    return pd.DataFrame([get_changes_1dim(np.array([v[0] for v in df_.to_numpy(dtype=np.float64)], dtype=np.float64)) for df_ in dfs[['close']].rolling(window, min_periods=window)], index=dfs.index)


class Status:
    def __init__(self):
        self.reset()

    def reset(self):
        self.in_position = 0
        self.value_at_enter = 0
        self.lowest_since_enter = 0
        self.timedelta_since_position_enter = 0
        self.v_ch_max_is_to_when_enter, self.v_ch_min_is_to_when_enter = 0, 0
        self.v_ch_max_is_from_when_enter, self.v_ch_min_is_from_when_enter = 0, 0
        self.ch_from_enter = 0
        self.ch_from_lowest_since_enter = 0

    def as_dict(self):
        return {
            'in_position': self.in_position,
            'value_at_enter': self.value_at_enter,
            'lowest_since_enter': self.lowest_since_enter,
            'timedelta_since_position_enter': self.timedelta_since_position_enter,
            'v_ch_max_is_to_when_enter': self.v_ch_max_is_to_when_enter,
            'v_ch_min_is_to_when_enter': self.v_ch_min_is_to_when_enter,
            'v_ch_max_is_from_when_enter': self.v_ch_max_is_from_when_enter,
            'v_ch_min_is_from_when_enter': self.v_ch_min_is_from_when_enter,
            'ch_from_enter': self.ch_from_enter,
            'ch_from_lowest_since_enter': self.ch_from_lowest_since_enter,
        }

    def as_df(self):
        return pd.DataFrame({k: [v] for k, v in self.as_dict().items()})

    def update(self, changes, trading_param):
        value = changes['value']
        if self.in_position == 1:
            if value < self.lowest_since_enter:
                self.lowest_since_enter = value

            self.timedelta_since_position_enter += 1
            self.ch_from_enter =  (value - self.value_at_enter) / self.value_at_enter
            self.ch_from_lowest_since_enter = (value - self.lowest_since_enter) / self.lowest_since_enter

            if not trading_param.is_long_term:
                if self.ch_from_lowest_since_enter > trading_param.exit_jumpt_threshold:
                    self.in_position = 0
            else:
                if self.ch_from_lowest_since_enter > trading_param.jump_threshold / 4.0 \
                    and self.timedelta_since_position_enter >= 5:
                    self.in_position = 0

                if self.ch_from_enter > trading_param.exit_jumpt_threshold:
                    self.in_position = 0

                if value < (self.v_ch_max_is_to_when_enter - self.v_ch_max_is_from_when_enter) / 3.0 + self.v_ch_max_is_from_when_enter:
                    self.in_position = 0
        else:
            should_enter_position = False

            if not trading_param.is_long_term:
                should_enter_position = changes['ch_max'] > trading_param.jump_threshold \
                and changes['ch_since_max'] < trading_param.drop_from_jump_threshold \
                and changes['distance_max_ch'] < 10 \
                and changes['distance_max_ch'] > 2
            else:
                should_enter_position = changes['ch_max'] > trading_param.jump_threshold \
                and changes['ch_since_max'] < trading_param.drop_from_jump_threshold \
                and changes['distance_max_ch'] < 60 \
                and changes['distance_max_ch'] > 2

            if should_enter_position:
                self.in_position = 1
                self.value_at_enter = value
                self.lowest_since_enter = value
                self.timedelta_since_position_enter = 0
                self.v_ch_max_is_to_when_enter = changes['v_ch_max_is_to']
                self.v_ch_min_is_to_when_enter = changes['v_ch_min_is_to']
                self.v_ch_max_is_from_when_enter = changes['v_ch_max_is_from']
                self.v_ch_min_is_from_when_enter = changes['v_ch_min_is_from']
                self.ch_from_enter = 0
                self.ch_from_lowest_since_enter = 0
            else:
                self.reset()


def add_trading_columns(df_feature, trading_param):
    status = Status()
    trading_dict = defaultdict(list)

    for i in df_feature.index:
        changes = df_feature.loc[i].to_dict()
        status.update(changes, trading_param)
        for k, v in {**changes, **status.as_dict()}.items():
            trading_dict[k].append(v)

    df_feature_trading = pd.DataFrame(trading_dict)
    df_feature_trading['position_changed'] = df_feature_trading.in_position.diff()
    df_feature_trading['profit_raw'] = -df_feature_trading.value.diff() * df_feature_trading.in_position.shift()
    df_feature_trading['profit'] = -df_feature_trading.value.pct_change() * df_feature_trading.in_position.shift()

    return df_feature_trading
