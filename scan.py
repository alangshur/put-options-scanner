from src.util.sheets import SheetsPortfolioExtractor
from src.scanner.wheel import WheelScanner
from tabulate import tabulate
import pandas as pd
import numpy as np
import os


if __name__ == '__main__':

    # fetch target equities
    sheets_extractor = SheetsPortfolioExtractor()
    target_equities = sheets_extractor.fetch('B9:B100')['Ticker'].values

    # load scanner 
    scanner = WheelScanner(
        uni_list=target_equities,
        num_processes=6,
        save_scan=False,
        log_changes=True
    )

    # update target equities
    portfolio_contracts = sheets_extractor.fetch('Main!G4:G100')['Ticker (F)'].values
    target_equities = list(set(target_equities) - set(portfolio_contracts))

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
    df = df[df['be_moneyness'] < 0.90]
    df = df[df['prob_be_delta'] >= 0.80]
    df = df[df['a_roc'] >= 0.25]

    # refine columns
    df_filt = pd.DataFrame().astype(np.float64)
    df_filt['und'] = df['underlying']
    df_filt['roc'] = df['roc']
    df_filt['a_roc'] = df['a_roc']
    df_filt['be'] = df['be']
    df_filt['be_moneyness'] = df['be_moneyness']
    df_filt['prob_be_delta'] = df['prob_be_delta']
    df_filt['prob_be_iv'] = df['prob_be_iv']
    df_filt['iv_percentile'] = df['udl_iv_percentile']
    df_filt['iv_skew'] = df['iv_skew']
    df_filt['target_ask'] = df['premium'] / 100.0
    df_filt.index = df.index

    # filter top contracts
    top_indices, top_results = [], []
    for equity in target_equities:
        df_result = df_filt[df_filt.index.str.startswith(equity + ' ')]
        df_result = df_result.nlargest(1, 'prob_be_delta')
        if df_result.shape[0] == 1: 
            result = np.squeeze(df_result)
            top_indices.append(result.name)
            top_results.append(result)
    top_results = pd.DataFrame(top_results)
    top_results.columns = df_filt.columns
    top_results.index = top_indices

    # output results
    df_top = top_results.sort_values('prob_be_delta', ascending=False)
    formatted_df_top = tabulate(df_top, headers='keys', tablefmt='psql')
    print(formatted_df_top)