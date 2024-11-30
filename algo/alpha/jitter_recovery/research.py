import pandas as pd
from collections import defaultdict
import algo.feature.jitter.calculate
import algo.alpha.jitter_recovery.calculate
import algo.feature.util.research
import algo.feature.jitter.research
from algo.alpha.jitter_recovery.calculate import JitterRecoveryTradingParam


_trading_label_prefix = '(changes_recovery_trading)'

def get_trading_label_for_caching(trading_param: JitterRecoveryTradingParam, label_suffix=None) -> str:
    r = algo.feature.util.research.get_param_label_for_caching(trading_param, _trading_label_prefix, label_suffix=label_suffix)
    return f'trading/{r}'


def get_dfst_trading(dfst_feature, trading_param: JitterRecoveryTradingParam):
    if 'ch_max' not in dfst_feature.columns:
        symbol_with_jumps = []
    else:
        symbol_with_jumps = dfst_feature[
            (
                (dfst_feature.ch_max > abs(trading_param.jump_threshold)) | (dfst_feature.ch_min < -abs(trading_param.jump_threshold))
            )
            ].index.get_level_values('symbol').unique().values

    print(f'symbol_with_jumps: {len((symbol_with_jumps))}')

    dfst_trading = dfst_feature.copy()
    for i, symbol in enumerate(symbol_with_jumps):
        df_feature = dfst_feature.xs(symbol, level=0)

        print(f'{i} symbol: {symbol}: {len(df_feature[df_feature.ch_max > trading_param.jump_threshold])} (trading)')
        df_trading = add_trading_columns(df_feature, trading_param)

        for column in df_trading.columns:
            if column in df_feature.columns:
                continue
            dfst_trading.loc[symbol, column] = df_trading[column].values

        del df_feature
        del df_trading

    return dfst_trading


def add_trading_columns(df_feature, trading_param):
    status = algo.alpha.jitter_recovery.calculate.Status()
    trading_dict = defaultdict(list)

    for i in df_feature.index:
        features = df_feature.loc[i].to_dict()
        all_features = features
        status.update(all_features, trading_param)
        for k, v in {**all_features, **algo.alpha.jitter_recovery.calculate.status_as_dict(status)}.items():
            trading_dict[k].append(v)

    df_feature_trading = pd.DataFrame(trading_dict, index=df_feature.index)
    df_feature_trading['position_changed'] = df_feature_trading.in_position.diff()
    df_feature_trading['profit_raw'] = -df_feature_trading.value.diff() * df_feature_trading.in_position.shift()
    df_feature_trading['profit'] = -df_feature_trading.value.pct_change() * df_feature_trading.in_position.shift()

    del trading_dict
    return df_feature_trading
