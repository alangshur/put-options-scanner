from src.util.sheets import SheetsPortfolioExtractor
from src.scanner.wheelput import WheelPutScanner
from src.scanner.wheelcall import WheelCallScanner
from tabulate import tabulate
import pandas as pd
import numpy as np
import os


class ScanExecutor:

    def run_put_scanner(self,
                    ignore_active_tickers=False, 
                    aroc_limit=0.3,
                    prob_itm_limit=0.25,
                    print_results=True,
                    refresh_results=False,
                    return_results=False,
                    **scan_kwargs):

        # fetch target equities
        sheets_extractor = SheetsPortfolioExtractor()
        target_matrix = sheets_extractor.fetch('\'Target Tickers\'!B4:P1000')
        target_equities = target_matrix['Ticker'].values

        # get analyst forecasts
        forecast_df = target_matrix[[
            '12M High Forecast',
            '12M Median Forecast',
            '12M Low Forecast'
        ]]
        forecast_df.index = target_equities
        forecast_df = forecast_df.replace('N/A', np.nan, regex=True)
        forecast_df = forecast_df.replace('[\$,]', '', regex=True).astype(float)

        # update target equities
        if ignore_active_tickers:
            portfolio_df = sheets_extractor.fetch('\'Active Positions\'!B5:R1000')
            portfolio_df = portfolio_df[portfolio_df['Stage (F)'] != 'Done']
            portfolio_contracts = portfolio_df['Ticker'].values
            target_equities = list(set(target_equities) - set(portfolio_contracts))

        # load scanner
        scanner = WheelPutScanner(
            uni_list=target_equities,
            num_processes=6,
            price_cap=1000.0,
            save_scan=False,
            log_changes=False,
            **scan_kwargs
        )

        # get results
        data = np.array(sum(scanner.run()['results'].values(), []))
        if data.shape[0] == 0:
            print('No matching puts.')
            return
        df = pd.DataFrame(data[:, 1:]).astype(np.float64)
        df.index = data[:, 0]
        df.columns = [
            'underlying', 'premium', 'dte', 'roc', 'be', 'moneyness',
            'prob_itm_delta', 'prob_be_delta', 'prob_be_iv', 'iv', 'iv_skew',
            'udl_year_ret', 'udl_year_ret_r2', 'udl_year_market_corr', 'udl_hist_vol', 
            'udl_iv_percentile', 'udl_hv_percentile', 'above_be_percentile', 'exp_move_conf'
        ]

        # filter all contracts
        df['prob_itm_delta'] = np.abs(df['prob_itm_delta'])
        df['a_roc'] = (1.0 + df['roc']) ** (365.2425 / df['dte']) - 1.0
        df = df[df['a_roc'] >= aroc_limit]
        df = df[df['prob_itm_delta'] < prob_itm_limit]

        thresh = prob_itm_limit + aroc_limit
        min_score = aroc_limit * (thresh - prob_itm_limit)
        max_score = 0.5 * (thresh - 0.1)
        norm = lambda x: (x - x.min()) / (x.max() - x.min())

        # refine columns
        df_filt = pd.DataFrame().astype(np.float64)
        df_filt['underlying ($)'] = round(df['underlying'], 2)
        df_filt['target_ask ($)'] = round(df['premium'] / 100.0, 2)
        df_filt['score (%)'] = round(100 * (df['a_roc'] * (thresh - df['prob_itm_delta']) - min_score) / (max_score - min_score), 3)
        df_filt['dte (D)'] = df['dte']
        df_filt['moneyness (%)'] = round(df['moneyness'], 3)
        df_filt['a_roc (%)'] = round(df['a_roc'], 3)
        df_filt['prob_itm (%)'] = round(df['prob_itm_delta'] * 100, 3)
        df_filt.index = df.index

        # get expected relative move
        contract_tgt = df['moneyness'] * df['underlying']
        exp_move_tgt = df['underlying'] - df['exp_move_conf'] * df['underlying']
        exp_rel_move = (exp_move_tgt - contract_tgt) / contract_tgt
        df_filt['exp_move (%)'] = round(exp_rel_move * 100, 3)

        # filter top contracts
        top_indices, top_results = [], []
        for equity in target_equities:
            df_result = df_filt[df_filt.index.str.startswith(equity + ' ')]
            df_result = df_result.nsmallest(1, 'prob_itm (%)')
            if df_result.shape[0] == 1: 
                result = np.squeeze(df_result)
                top_indices.append(result.name)
                top_results.append(result)
        top_results = pd.DataFrame(top_results)
        top_results.columns = df_filt.columns
        top_results.index = top_indices

        # incorporate analyst metrics
        strikes = np.array([float(idx.split(' ')[4][1:]) for idx in top_indices]).reshape((-1, 1))
        tickers = [str(idx.split(' ')[0]) for idx in top_indices]
        forecast_df = forecast_df.loc[tickers]
        forecast_df -= np.broadcast_to(strikes, (strikes.shape[0], 3))
        forecast_df /= np.broadcast_to(strikes, (strikes.shape[0], 3))
        top_results['hi_fc (%)'] = np.round(100 * forecast_df.iloc[:, 0].values, 3)
        top_results['mdn_fc (%)'] = np.round(100 * forecast_df.iloc[:, 1].values, 3)
        top_results['lo_fc (%)'] = np.round(100 * forecast_df.iloc[:, 2].values, 3)
        top_results = top_results.replace(np.nan, "N/A", regex=True)
        df_top = top_results.sort_values('prob_itm (%)', ascending=True)

        # output results
        if print_results:
            formatted_df_top = tabulate(df_top, headers='keys', tablefmt='psql')
            if refresh_results: os.system('clear')
            print()
            print('Contract Scan')
            print(formatted_df_top, flush=True)

        # return results
        if return_results:
            return df_top

    def run_focus_put_scanner(self, symbol):

        # load scanner 
        scanner = WheelPutScanner(
            uni_list=[symbol],
            num_processes=6,
            price_cap=1000.0,
            save_scan=False,
            log_changes=True
        )

        # get results
        data = np.array(sum(scanner.run()['results'].values(), []))
        if data.shape[0] == 0:
            print('No matching puts.')
            return
        df = pd.DataFrame(data[:, 1:]).astype(np.float64)
        df.index = data[:, 0]
        df.columns = [
            'underlying', 'premium', 'dte', 'roc', 'be', 'moneyness', 
            'prob_itm_delta', 'prob_be_delta', 'prob_be_iv', 'iv', 'iv_skew',
            'udl_year_ret', 'udl_year_ret_r2', 'udl_year_market_corr', 'udl_hist_vol', 
            'udl_iv_percentile', 'udl_hv_percentile', 'above_be_percentile'
        ]

        # filter all contracts
        df['a_roc'] = (1.0 + df['roc']) ** (365.2425 / df['dte']) - 1.0
        df = df[df['a_roc'] >= 0.30]

        # refine columns
        norm = lambda x: (x - x.min()) / (x.max() - x.min())
        df_filt = pd.DataFrame().astype(np.float64)
        df_filt['und'] = df['underlying']
        df_filt['dte'] = df['dte']
        df_filt['premium'] = df['premium']
        df_filt['roc'] = df['roc']
        df_filt['a_roc'] = df['a_roc']
        df_filt['be'] = df['be']
        df_filt['moneyness'] = df['moneyness']
        df_filt['prob_itm_delta'] = np.abs(df['prob_itm_delta'])
        df_filt['prob_be_delta'] = df['prob_be_delta']
        df_filt['prob_be_iv'] = df['prob_be_iv']
        df_filt['iv_percentile'] = df['udl_iv_percentile']
        df_filt['iv_skew'] = df['iv_skew']
        df_filt['target_ask'] = df['premium'] / 100.0
        df_filt.index = df.index

        # output results
        df_top = df_filt.sort_values('prob_itm_delta', ascending=True)
        formatted_df_top = tabulate(df_top, headers='keys', tablefmt='psql')
        print(formatted_df_top)

    def run_call_scanner(self, put_contract):

        # fetch contract stats
        sheets_extractor = SheetsPortfolioExtractor()
        portfolio_df = sheets_extractor.fetch('\'Active Positions\'!B5:R1000')
        contract = portfolio_df[portfolio_df['Contract (F)'] == put_contract]
        put_ticker = put_contract.split(' ')[0]
        put_strike = float(put_contract.split(' ')[4][1:])
        if contract.shape[0] > 0: cost_basis = float(contract.iloc[0]['Cost Basis (F)'][1:])
        else: cost_basis = put_strike

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
        df = df[(df['premium'] + df['underlying_gain']) > 0]

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