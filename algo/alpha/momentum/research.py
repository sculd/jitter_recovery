import pandas as pd
from collections import defaultdict
import algo.feature.momentum.calculate
import algo.alpha.momentum.calculate
import algo.feature.util.research
import algo.feature.momentum.research
from algo.alpha.momentum.calculate import MomentumTradingParam
import matplotlib.pyplot as plt


_trading_label_prefix = '(momentum_trading)'

def get_trading_label_for_caching(trading_param: MomentumTradingParam, label_suffix=None) -> str:
    r = algo.feature.util.research.get_param_label_for_caching(trading_param, _trading_label_prefix, label_suffix=label_suffix)
    return f'trading/{r}'


def get_dfst_trading(dfst_feature, trading_param: MomentumTradingParam):
    if 'rank' not in dfst_feature.columns:
        symbol_with_momentums = []
    else:
        symbol_with_momentums = dfst_feature[
            ((dfst_feature['rank'] <= trading_param.selection_size) & (dfst_feature['ch_ema'] < 0))
            | ((dfst_feature['rank_descending'] <= trading_param.selection_size) & (dfst_feature['ch_ema'] > 0))
            ].index.get_level_values('symbol').unique().values

    print(f'symbol_with_momentums: {len((symbol_with_momentums))}')

    dfst_trading = dfst_feature.copy()
    for i, symbol in enumerate(symbol_with_momentums):
        df_feature = dfst_feature.xs(symbol, level=0)

        print(f'{i} symbol: {symbol}: (trading)')
        df_trading = add_trading_columns(df_feature, trading_param)

        for column in df_trading.columns:
            if column in df_feature.columns:
                continue
            dfst_trading.loc[symbol, column] = df_trading[column].values

        del df_feature
        del df_trading

    return dfst_trading


def add_trading_columns(df_feature, trading_param):
    status = algo.alpha.momentum.calculate.Status()
    trading_dict = defaultdict(list)

    for i in df_feature.index:
        features = df_feature.loc[i].to_dict()
        status.update(i, features, trading_param)
        for k, v in {**features, **algo.alpha.momentum.calculate.status_as_dict(status)}.items():
            trading_dict[k].append(v)

    df_feature_trading = pd.DataFrame(trading_dict, index=df_feature.index)
    df_feature_trading['position_changed'] = df_feature_trading.in_position.diff()
    df_feature_trading['profit_raw'] = df_feature_trading.value.diff() * df_feature_trading.in_position.shift()
    df_feature_trading['profit'] = df_feature_trading.value.pct_change() * df_feature_trading.in_position.shift()

    del trading_dict
    return df_feature_trading
