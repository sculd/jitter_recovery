import pandas as pd, numpy as np
from collections import defaultdict

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
    def __init__(self, jitter_recover_feature_param, jump_threshold, drop_from_jump_threshold, exit_jumpt_threshold, is_long_term):
        self.jitter_recover_feature_param = jitter_recover_feature_param
        self.jump_thresholdv= jump_threshold
        self.drop_from_jump_threshold = drop_from_jump_threshold
        self.exit_jumpt_threshold = exit_jumpt_threshold
        self.is_long_term = is_long_term

    def get_default_param():
        return JitterRecoveryTradingParam(
            JitterRecoveryFeatureParam.get_default_param(), default_jump_threshold, default_drop_from_jump_threshold, default_exit_jumpt_threshold, is_long_term = False)

    def get_default_param_longterm():
        return JitterRecoveryTradingParam(
            JitterRecoveryFeatureParam.get_default_param_longterm(), default_jump_threshold_longterm, default_drop_from_jump_threshold_longterm, default_exit_jumpt_threshold_longterm, is_long_term = True)


def _get_ch(v1, v2):
    if v1 == 0:
        return 0
    return (v2 - v1) / v1


def get_changes_1dim(values):
    '''
    values is a 1 dimensional array.
    '''
    l = values.shape[0]
    if l < 1: return None

    if len(values.shape) == 2:
        values = [v[0] for v in values]

    ch_max, ch_min = 0, 0
    distance_max_ch, distance_min_ch = 1, 1
    ch_since_max, ch_since_min = 0, 0

    first_v, last_v = values[0], values[-1]
    max_v, min_v = values[0], values[0]
    v_ch_max_is_to, v_ch_min_is_to = max_v, min_v
    v_ch_max_is_from, v_ch_min_is_from = max_v, min_v

    for i, v in enumerate(values):
        min_v, max_v = min(min_v, v), max(max_v, v)

        ch_jump = _get_ch(min_v, v)
        ch_drop = _get_ch(max_v, v)
        
        ch = _get_ch(first_v, v)
        ch_since = _get_ch(v, last_v)

        d =  l-1-i

        if ch_max <= ch_jump:
            distance_max_ch, ch_since_max, ch_max = d, ch_since, ch_jump
            v_ch_max_is_from = min_v
            v_ch_max_is_to = v

        if ch_min >= ch_drop:
            distance_min_ch, ch_since_min, ch_min = d, ch_since, ch_drop
            v_ch_min_is_from = max_v
            v_ch_min_is_to = v


    return {
        'value': values[-1],
        'ch_max': ch_max, 'ch_min': ch_min,
        'v_ch_max_is_from': v_ch_max_is_from, 'v_ch_min_is_from': v_ch_min_is_from,
        'v_ch_max_is_to': v_ch_max_is_to, 'v_ch_min_is_to': v_ch_min_is_to,
        'ch_since_max': ch_since_max, 'ch_since_min': ch_since_min,
        'distance_max_ch': distance_max_ch, 'distance_min_ch': distance_min_ch,
        }


def get_feature_df(dfs, jitter_recover_feature_param):
    window = jitter_recover_feature_param.jump_window
    return pd.DataFrame([get_changes_1dim(df_.values) for df_ in dfs[['close']].rolling(window, min_periods=window)], index=dfs.index)


class Status:
    def __init__(self):
        self.reset()

    def reset(self):
        self.in_position = 0
        self.value_at_enter = 0
        self.lowest_since_enter = 0
        self.timedelta_since_position_enter = 0
        self.v_ch_max_is_to, self.v_ch_min_is_to = 0, 0
        self.v_ch_max_is_from, self.v_ch_min_is_from = 0, 0
        self.ch_from_enter = 0
        self.ch_from_lowest_since_enter = 0

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
                if self.ch_from_lowest_since_enter > trading_param.exit_jumpt_threshold \
                    and self.timedelta_since_position_enter >= 5:
                    self.in_position = 0

                if self.ch_from_enter > trading_param.exit_jumpt_threshold:
                    self.in_position = 0

                if value < (self.v_ch_max_is_to - self.v_ch_max_is_from) / 3.0 + self.v_ch_max_is_from:
                    self.in_position = 0
        else:
            should_enter_position = False

            if not trading_param.is_long_term:
                should_enter_position = changes['ch_max'] > trading_param.jump_thresholdv \
                and changes['ch_since_max'] < trading_param.drop_from_jump_threshold \
                and changes['distance_max_ch'] < 10 \
                and changes['distance_max_ch'] > 2
            else:
                should_enter_position = changes['ch_max'] > trading_param.jump_thresholdv \
                and changes['ch_since_max'] < trading_param.drop_from_jump_threshold \
                and changes['distance_max_ch'] < 60 \
                and changes['distance_max_ch'] > 2

            if should_enter_position:
                self.in_position = 1
                self.value_at_enter = value
                self.lowest_since_enter = value
                self.timedelta_since_position_enter = 0
                self.v_ch_max_is_to = changes['v_ch_max_is_to']
                self.v_ch_min_is_to = changes['v_ch_min_is_to']
                self.v_ch_max_is_from = changes['v_ch_max_is_from']
                self.v_ch_min_is_from = changes['v_ch_min_is_from']
                self.ch_from_enter = 0
                self.ch_from_lowest_since_enter = 0
            else:
                self.reset()


def add_trading_columns(df_feature, jitter_recover_trading_param):
    in_positions = [0]
    lowest_since_enters = [0]
    timedelta_since_position_enters = [0]
    value_at_enters = [0]
    v_ch_max_is_tos, v_ch_min_is_tos = [0], [0]
    v_ch_max_is_froms, v_ch_min_is_froms = [0], [0]
    ch_from_enters = [0]
    ch_from_lowest_since_enters = [0]
    for i in range(1, len(df_feature.index)):
        in_position = in_positions[-1]
        value_at_enter = value_at_enters[-1]
        lowest_since_enter = lowest_since_enters[-1]
        timedelta_since_position_enter = timedelta_since_position_enters[-1]
        v_ch_max_is_to = v_ch_max_is_tos[-1]
        v_ch_min_is_to = v_ch_min_is_tos[-1]
        v_ch_max_is_from = v_ch_max_is_froms[-1]
        v_ch_min_is_from = v_ch_min_is_froms[-1]
        ch_from_enter = ch_from_enters[-1]
        ch_from_lowest_since_enter = ch_from_lowest_since_enters[-1]
        # if i_decision = i-1, the position of current is one step delayed to make it more realistic.
        # on the other hand, it might be too unrealistically strigent condition
        decision_delay = 0
        i_decision = i - decision_delay
        v = df_feature.value.values[i]
        if in_position == 1:
            if v < lowest_since_enter:
                lowest_since_enter = v
            timedelta_since_position_enter = timedelta_since_position_enters[-1] + 1
            ch_from_enter =  (v - value_at_enter) / value_at_enter
            ch_from_lowest_since_enter = (v - lowest_since_enter) / lowest_since_enter
    
            if not jitter_recover_trading_param.is_long_term:
                if ch_from_lowest_since_enter > jitter_recover_trading_param.exit_jumpt_threshold:
                    in_position = 0
            else:
                if ch_from_lowest_since_enter > jitter_recover_trading_param.exit_jumpt_threshold \
                    and timedelta_since_position_enter >= 5:
                    in_position = 0

                if ch_from_enter > jitter_recover_trading_param.exit_jumpt_threshold:
                    in_position = 0

                if v < (v_ch_max_is_to - v_ch_max_is_from) / 3.0 + v_ch_max_is_from:
                    in_position = 0
        else:
            should_enter_position = False
            
            if not jitter_recover_trading_param.is_long_term:
                should_enter_position = df_feature.ch_max.values[i_decision] > jitter_recover_trading_param.jump_thresholdv \
                and df_feature.ch_since_max.values[i_decision] < jitter_recover_trading_param.drop_from_jump_threshold \
                and df_feature.distance_max_ch.values[i_decision] < 10 \
                and df_feature.distance_max_ch.values[i_decision] > 2
            else:
                should_enter_position = df_feature.ch_max.values[i_decision] > jitter_recover_trading_param.jump_thresholdv \
                and df_feature.ch_since_max.values[i_decision] < jitter_recover_trading_param.drop_from_jump_threshold \
                and df_feature.distance_max_ch.values[i_decision] < 60 \
                and df_feature.distance_max_ch.values[i_decision] > 2

            if should_enter_position: 
                in_position = 1
                value_at_enter = v
                lowest_since_enter = df_feature.value.values[i]
                timedelta_since_position_enter = 0
                v_ch_max_is_to = df_feature.v_ch_max_is_to.values[i]
                v_ch_min_is_to = df_feature.v_ch_min_is_to.values[i]
                v_ch_max_is_from = df_feature.v_ch_max_is_from.values[i]
                v_ch_min_is_from = df_feature.v_ch_min_is_from.values[i]
                ch_from_enter = 0
                ch_from_lowest_since_enter = 0
            else:
                in_position = 0
                value_at_enter = 0
                lowest_since_enter = 0
                timedelta_since_position_enter = 0
                v_ch_max_is_to = 0
                v_ch_min_is_to = 0
                v_ch_max_is_from = 0
                v_ch_min_is_from = 0
                ch_from_enter = 0
                ch_from_lowest_since_enter = 0
    
        in_positions.append(in_position)
        value_at_enters.append(value_at_enter)
        lowest_since_enters.append(lowest_since_enter)
        timedelta_since_position_enters.append(timedelta_since_position_enter)
        v_ch_max_is_tos.append(v_ch_max_is_to)
        v_ch_min_is_tos.append(v_ch_min_is_to)
        v_ch_max_is_froms.append(v_ch_max_is_from)
        v_ch_min_is_froms.append(v_ch_min_is_from)
        ch_from_enters.append(ch_from_enter)
        ch_from_lowest_since_enters.append(ch_from_lowest_since_enter)
    
    df_feature['in_position'] = in_positions
    df_feature['value_at_enter'] = value_at_enters
    df_feature['position_changed'] = df_feature.in_position.diff()
    df_feature['lowest_since_enter'] = lowest_since_enters
    df_feature['timedelta_since_position_enter'] = timedelta_since_position_enters
    df_feature['v_ch_max_is_to'] = v_ch_max_is_tos
    df_feature['v_ch_min_is_to'] = v_ch_min_is_tos
    df_feature['v_ch_max_is_from'] = v_ch_max_is_froms
    df_feature['v_ch_min_is_from'] = v_ch_min_is_froms
    df_feature['ch_from_enter'] = ch_from_enters
    df_feature['ch_from_lowest_since_enter'] = ch_from_lowest_since_enters
    df_feature['profit_raw'] = -df_feature.value.diff() * df_feature.in_position.shift()
    df_feature['profit'] = -df_feature.value.pct_change() * df_feature.in_position.shift()

    return df_feature
