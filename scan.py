from src.scanner.wheel import WheelScanner
from tabulate import tabulate
import pandas as pd
import numpy as np
import os


# set constants
balance = 100100
position_pp = 0.10
target_equities = [
    'AAPL', 'TWTR', 'SQ', 'SNAP', 'SHOP', 'ROKU', 'INTC', 'AMD', 'FB', 
    'CAT', 'AMZN', 'TSLA', 'T', 'CSCO', 'CVS', 'VZ', 'BAC', 'C', 'KO', 
    'TGT', 'PG', 'CLX', 'KMB', 'JNJ', 'TROW', 'F', 'WM', 'SYY', 'AFL', 
    'WFC', 'GE', 'DB', 'DIS', 'NVDA'
]


if __name__ == '__main__':
    scanner = WheelScanner(
        uni_list=target_equities,
        num_processes=6,
        save_scan=False,
        log_changes=True
    )

    # get results
    data = np.array(sum(scanner.run()['results'].values(), []))
    df = pd.DataFrame(data[:, 1:]).astype(np.float64)

    # convert to dataframe
    df = pd.DataFrame(data[:, 1:]).astype(np.float64)
    df.index = data[:, 0]
    df.columns = [
        'underlying', 'premium', 'dte', 'roc', 'be', 'be_moneyness', 
        'prob_be_delta', 'prob_be_iv', 'iv', 'iv_skew',
        'udl_year_ret', 'udl_year_ret_r2', 'udl_year_market_corr', 'udl_hist_vol', 
        'udl_iv_percentile', 'udl_hv_percentile', 'above_be_percentile'
    ]

    # filter all contracts
    df['a_roc'] = (1.0 + df['roc']) ** (365.2425 / df['dte']) - 1.0
    df = df[df['be'] <= 200]
    df = df[df['prob_be_delta'] >= 0.80]
    df = df[df['a_roc'] >= 0.15]

    def normalize(row):
        return (row - row.min()) / (row.max() - row.min())

    # refine columns
    df_filt = pd.DataFrame().astype(np.float64)
    df_filt['und'] = df['underlying']
    df_filt['roc'] = df['roc']
    df_filt['a_roc'] = df['a_roc']
    df_filt['be'] = df['be']
    df_filt['score'] = normalize(df['a_roc']) * normalize(df['prob_be_delta'])
    df_filt['be_moneyness'] = df['be_moneyness']
    df_filt['prob_be_delta'] = df['prob_be_delta']
    df_filt['prob_be_iv'] = df['prob_be_iv']
    df_filt['iv_percentile'] = df['udl_iv_percentile']
    df_filt['iv_skew'] = df['iv_skew']
    df_filt['target_ask'] = df['premium'] / 100.0
    df_filt['target_qty'] = np.floor((balance * position_pp) / (df['be'] * 100.0))
    df_filt.index = df.index

    # filter top contracts
    top_indices, top_results = [], []
    for equity in target_equities:
        df_result = df_filt[df_filt.index.str.startswith(equity + ' ')]
        df_result = df_result.nlargest(1, 'score')
        if df_result.shape[0] == 1: 
            result = np.squeeze(df_result)
            top_indices.append(result.name)
            top_results.append(result)
    top_results = pd.DataFrame(top_results)
    top_results.columns = df_filt.columns
    top_results.index = top_indices

    # output results
    df_top = top_results.sort_values('score', ascending=False)
    formatted_df_top = tabulate(df_top, headers='keys', tablefmt='psql')
    print(formatted_df_top)