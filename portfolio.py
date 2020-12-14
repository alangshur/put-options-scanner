from src.util.sheets import SheetsPortfolioExtractor
from src.api.tradier import TradierAPI
from datetime import datetime, date
from tabulate import tabulate
import pandas as pd
import numpy as np
import csv


def num_to_float(num):
    if '$' in num: num = num[1:]
    if '.' in num: num = num[:-3]
    num = num.replace(',', '')
    num = float(num)
    return num


if __name__ == '__main__':
    contract_data = [[], [], [], [], [], [], [], [], []] 
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
        qty = num_to_float(contract['Quantity (F)'])
        sell_price = num_to_float(contract['Premium']) / 100.0 / qty
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
        strike = num_to_float(contract_comps[4])
        moneyness = strike / underlying_quote
        if np.abs(1.0 - moneyness) <= 0.015: moneyness_status = 'ATM'
        elif contract_comps[5] == 'Put' and moneyness < 1.0: moneyness_status = 'OTM'
        elif contract_comps[5] == 'Call' and moneyness > 1.0: moneyness_status = 'OTM'
        else: moneyness_status = 'ITM'
        contract_data[3].append(strike - sell_price)
        contract_data[4].append(round(moneyness * 100, 2))
        contract_data[5].append(moneyness_status)

        # get return and p/l
        ret = round((contract_query['ask'] - sell_price) / sell_price * -100, 2)
        pl = (sell_price - contract_query['ask']) * 100 * qty
        contract_data[6].append(ret)
        contract_data[7].append(pl)
        contract_data[8].append(contract_query['ask'])

        # get tied-up capital
        tuc = num_to_float(contract['Tied-Up Capital (F)'])

        # update general stats
        if contract_comps[5] == 'Put': portfolio_pl += pl
        max_portfolio_pl += sell_price * 100 * qty
        cash_util += tuc

    # build dataframe
    df = pd.DataFrame(np.transpose(contract_data))
    df.index = portfolio_df['Contract (F)'].values
    df.columns = [
        'underlying_price ($)', 'contract_price ($)', 
        'dte', 'be ($)', 'moneyness (%)', 'status', 
        'return (%)', 'p/l ($)', 'target_ask ($)'
    ]

    # print general stats
    print()
    print('Portfolio size: {}'.format(portfolio_df.shape[0]))
    print('Realized portfolio p/l: {} USD'.format(round(portfolio_pl, 2)))
    print('Max portfolio p/l: {} USD'.format(round(max_portfolio_pl, 2)))
    print('Cash utilization: {} USD'.format(round(cash_util, 2)))
    print()

    # print dataframe
    formatted_df = tabulate(df, headers='keys', tablefmt='psql')
    print(formatted_df)
    print()