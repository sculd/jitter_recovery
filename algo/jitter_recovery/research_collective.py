import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt
import algo.jitter_recovery.calculate
import algo.jitter_recovery.calculate_collective


collective_feature_columns_no_rolling = ['ch', 'ch_max', 'ch_min', 'ch_since_max', 'ch_since_min']
collective_feature_columns = collective_feature_columns_no_rolling + ['ch_window30_min']


def get_dfsts(df, trading_param):
    dfi = df.set_index(['timestamp', 'symbol'])
    all_symbols = df.symbol.unique()
    all_symbols = [s for s in all_symbols if 'USDT' in s]
      
    dfst_feature = df.set_index(['symbol', 'timestamp'])
    for i, symbol in enumerate(all_symbols):
        if 'USDT' not in symbol: continue
        dfs = dfi.xs(symbol, level=1)
        
        df_feature = algo.jitter_recovery.calculate.get_feature_df(dfs, trading_param.feature_param)
        del dfs
        
        print(f'{i} symbol: {symbol}: {len(df_feature[df_feature.ch_min <= trading_param.drop_threshold * 0.9])} (feature)')
        
        for column in df_feature.columns:
            dfst_feature.loc[symbol, column] = df_feature[column].values
        del df_feature

    df_collective_feature = dfst_feature.dropna().groupby('timestamp')[collective_feature_columns_no_rolling].median().resample('1min').asfreq().ffill()
    df_collective_feature['ch_window30_min'] = df_collective_feature.ch.rolling(window=60).min() 

    dfst_trading = df.set_index(['symbol', 'timestamp'])
    for i, symbol in enumerate(all_symbols):
        if 'USDT' not in symbol: continue
        
        df_feature = dfst_feature.xs(symbol, level=0)

        print(f'{i} symbol: {symbol}: {len(df_feature[df_feature.ch_min <= trading_param.drop_threshold * 0.9])} (trading)')
        df_trading = add_trading_columns(df_feature, df_collective_feature, trading_param)
        
        for column in df_trading.columns:
            dfst_trading.loc[symbol, column] = df_trading[column].values
        del df_trading

    return dfst_feature, dfst_trading


def add_trading_columns(df_feature, df_collective_feature_approxiamate, trading_param):
    status = algo.jitter_recovery.calculate_collective.Status()
    trading_dict = defaultdict(list)

    for i in df_feature.index:
        features = df_feature.loc[i].to_dict()
        if i in df_collective_feature_approxiamate.index:
            collective_featuers = df_collective_feature_approxiamate.loc[i].to_dict()
        else:
            collective_featuers = {c: 0.0 for c in collective_feature_columns}
        all_features = {**features, **collective_featuers}
        status.update(collective_featuers, features, trading_param)
        for k, v in {**all_features, **algo.jitter_recovery.calculate.status_as_dict(status)}.items():
            trading_dict[k].append(v)

    df_feature_trading = pd.DataFrame(trading_dict, index=df_feature.index)
    df_feature_trading['position_changed'] = df_feature_trading.in_position.diff()
    df_feature_trading['profit_raw'] = df_feature_trading.value.diff() * df_feature_trading.in_position.shift()
    df_feature_trading['profit'] = df_feature_trading.value.pct_change() * df_feature_trading.in_position.shift()

    return df_feature_trading


