from src.util.sheets import SheetsPortfolioExtractor
from src.api.tradier import TradierAPI
from datetime import datetime, date
from tabulate import tabulate
import pandas as pd
import numpy as np


class PortfolioExecutor:

    def num_to_float(self, num):
        if '$' in num: num = num[1:]
        num = num.replace(',', '')
        num = float(num)
        return num

    def run_portfolio_read(self, 
                           print_general_stats=True,
                           return_results=False,
                           min_close_days=3):

        contract_data = [[], [], [], [], [], [], [], [], [], [], []] 
        api = TradierAPI()
        portfolio_pl = 0
        max_portfolio_pl = 0
        cash_util = 0

        # load portfolio
        sheets_extractor = SheetsPortfolioExtractor()
        portfolio_df = sheets_extractor.fetch('\'Active Positions\'!B5:Q1000')
        portfolio_df = portfolio_df[portfolio_df['Stage (F)'] != 'Done']

        # analyze contract data
        for _, contract in portfolio_df.iterrows():
            contract_string = contract['Contract (F)']
            qty = self.num_to_float(contract['Quantity (F)'])
            sell_price = self.num_to_float(contract['Premium']) / 100.0 / qty
            contract_comps = contract_string.split(' ')

            # get quotes
            underlying_quote = api.fetch_underlying(contract_comps[0])[0]['last']
            contract_query = api.fetch_contract(contract_string)[0]
            contract_data[0].append(round(underlying_quote, 2))
            contract_data[1].append(round(contract_query['last'], 2))

            # get dte
            dt = datetime.strptime(' '.join(contract_comps[1:4]), '%B %d %Y').date()
            dte = (dt - date.today()).days
            contract_data[2].append(dte)

            # get strike
            strike = self.num_to_float(contract_comps[4])
            moneyness = strike / underlying_quote
            if np.abs(1.0 - moneyness) <= 0.015: moneyness_status = 'ATM'
            elif contract_comps[5] == 'Put' and moneyness < 1.0: moneyness_status = 'OTM'
            elif contract_comps[5] == 'Call' and moneyness > 1.0: moneyness_status = 'OTM'
            else: moneyness_status = 'ITM'
            contract_data[3].append(strike - sell_price)
            contract_data[4].append(round(moneyness * 100, 2))
            contract_data[5].append(moneyness_status)

            # get return and p/l
            ret = (sell_price - contract_query['ask']) / sell_price
            pl = (sell_price - contract_query['ask']) * 100 * qty
            contract_data[6].append(round(ret * 100, 2))
            contract_data[7].append(pl)
            contract_data[8].append(contract_query['ask'])

            # get tied-up capital
            tuc = self.num_to_float(contract['Tied-Up Capital (F)'])

            # calculate annualized ROCs
            a_roc = float(contract['Annualized ROC'][:-1]) / 100
            contract_data[9].append(round(a_roc * 100, 2))
            close_ret = ((sell_price - contract_query['ask']) * qty * 100) / tuc
            dt = datetime.strptime(contract['Date Opened (F)'], '%m/%d/%Y').date()
            dse = (date.today() - dt).days
            if dse < min_close_days or contract_comps[5] == 'Call': 
                cur_a_roc = 0
            else:
                try: cur_a_roc = (1 + close_ret) ** (365.2425 / dse) - 1
                except: cur_a_roc = float('inf')
            contract_data[10].append(round(cur_a_roc * 100, 2))
 
            # update general stats
            if contract_comps[5] == 'Put': portfolio_pl += pl
            max_portfolio_pl += sell_price * 100 * qty
            cash_util += tuc

        # build dataframe
        df = pd.DataFrame(np.transpose(contract_data))
        df.index = portfolio_df['Contract (F)'].values
        df.columns = [
            'und_price ($)', 'cont_price ($)', 'dte (D)', 
            'be ($)', 'moneyness (%)', 'status', 'return (%)', 
            'p/l ($)', 'target_ask ($)', 'a_roc (%)', 
            'cur_a_roc (%)'
        ]

        # rearrange columns
        df = df[[
            'target_ask ($)', 'dte (D)', 'status', 
            'moneyness (%)', 'return (%)',
            'a_roc (%)', 'cur_a_roc (%)'
        ]].sort_values(['cur_a_roc (%)'], ascending=False)
        
        # print general stats
        print()
        print('Portfolio Scan')
        if print_general_stats:
            print()
            print('Portfolio size: {}'.format(portfolio_df.shape[0]))
            print('Realized portfolio p/l: {} USD'.format(round(portfolio_pl, 2)))
            print('Max portfolio p/l: {} USD'.format(round(max_portfolio_pl, 2)))
            print('Cash utilization: {} USD'.format(round(cash_util, 2)))
            print()

        # print dataframe
        formatted_df = tabulate(df, headers='keys', tablefmt='psql')
        print(formatted_df)

        return df