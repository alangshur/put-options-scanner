from src.util.sheets import SheetsPortfolioExtractor
from src.scanner.wheelput import WheelPutScanner
from src.scanner.wheelcall import WheelCallScanner
from tabulate import tabulate
import pandas as pd
import numpy as np
import argparse
import os


def run_put_scanner(ignore_active_tickers=False):

    # fetch target equities
    sheets_extractor = SheetsPortfolioExtractor()
    target_equities = sheets_extractor.fetch('B9:B100')['Ticker'].values

    # load scanner 
    scanner = WheelPutScanner(
        uni_list=target_equities,
        num_processes=6,
        price_cap=250.0,
        save_scan=False,
        log_changes=True
    )

    # update target equities
    if ignore_active_tickers:
        portfolio_df = sheets_extractor.fetch('Main!G5:V100')
        portfolio_df = portfolio_df[portfolio_df['Stage (F)'] != 'Done']
        portfolio_contracts = portfolio_df['Ticker (F)'].values
        target_equities = list(set(target_equities) - set(portfolio_contracts))

    # get results
    data = np.array(sum(scanner.run()['results'].values(), []))
    if data.shape[0] == 0:
        print('No matching puts.')
        return
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
    df = df[df['be'] <= 250]
    df = df[df['be_moneyness'] < 0.95]
    df = df[df['prob_be_delta'] >= 0.80]
    df = df[df['a_roc'] >= 0.15]

    # refine columns
    df_filt = pd.DataFrame().astype(np.float64)
    df_filt['und'] = df['underlying']
    df_filt['dte'] = df['dte']
    df_filt['premium'] = df['premium']
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


def run_call_scanner(put_contract):

    # fetch contract stats
    sheets_extractor = SheetsPortfolioExtractor()
    portfolio_df = sheets_extractor.fetch('Main!G5:V100')
    contract = portfolio_df[portfolio_df['Active Contract (F)'] == put_contract]
    put_ticker = put_contract.split(' ')[0]
    put_strike = float(put_contract.split(' ')[4][1:])
    cost_basis = float(contract.iloc[0]['Cost Basis (F)'][1:])

    # load scanner
    scanner = WheelCallScanner(
        put_ticker=put_ticker,
        put_strike=put_strike,
        cost_basis=cost_basis,
        save_scan=False
    )

    # get results
    data = np.array(scanner.run()['results'])
    df = pd.DataFrame(data[:, 1:]).astype(np.float64)
    df.index = data[:, 0]
    df.columns = [
        'underlying', 'premium', 'dte', 'roc',
        'updated_basis', 'underlying_gain', 
        'moneyness', 'prob_itm_delta',
        'prob_itm_iv', 'iv' 
    ]

    # filter results
    df['adj_payout'] = df['premium'] + df['underlying_gain'] * df['prob_itm_delta']
    df = df[df['adj_payout'] > 0.0]

    # refine columns
    df_filt = pd.DataFrame().astype(np.float64)
    df_filt['underlying'] = df['underlying']
    df_filt['dte'] = df['dte']
    df_filt['premium'] = df['premium']
    df_filt['updated_basis'] = df['updated_basis']
    df_filt['roc'] = df['roc']
    df_filt['a_roc'] = (1.0 + df['roc']) ** (365.2425 / df['dte']) - 1.0
    df_filt['adj_a_roc'] = (1.0 + (df['adj_payout'] / (put_strike * 100.0))) ** (365.2425 / df['dte']) - 1.0
    df_filt['underlying_gain'] = df['underlying_gain']
    df_filt['prob_itm_delta'] = df['prob_itm_delta']
    df_filt['prob_itm_iv'] = df['prob_itm_iv']
    df_filt['target_ask'] = df['premium'] / 100.0
    df_filt.index = df.index

    # output results
    df_top = df_filt.sort_values('adj_a_roc', ascending=False)
    formatted_df_top = tabulate(df_top, headers='keys', tablefmt='psql')
    print(formatted_df_top)


if __name__ == '__main__':

    # build arg parser
    parser = argparse.ArgumentParser(description='Run scanner for puts and calls.')
    parser.add_argument('direction', 
        type=str,
        help='The option type direction in which to aim the scan.'
    )
    parser.add_argument('--pcontract', 
        type=str,
        help='The name of the original put contract to scan calls for.'
    )

    # run scanner
    args = parser.parse_args()
    if (args.direction in ['call', 'c', 'calls']) and args.pcontract is None:
        parser.error('direction of type \"call\" requires --pcontract.')
    if args.direction.lower() in ['put', 'p', 'puts']: run_put_scanner()
    elif args.direction.lower() in ['call', 'c', 'calls']: run_call_scanner(args.pcontract)
    else: parser.error('invalid choice of direction, choose from [put/call].')
