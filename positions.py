from src.api.tradier import TradierAPI
from datetime import datetime, date
from tabulate import tabulate
import pandas as pd
import numpy as np


# set constants
portfolio_contracts = [
    ['ROKU November 13 2020 $180 Put', 4.28],
    ['SNAP November 20 2020 $22 Put', 0.49]
]


if __name__ == '__main__':

    # analyze contract data
    api = TradierAPI()
    contract_data = [[], [], [], [], []] 
    for contract in portfolio_contracts:
        contract_string, sell_price = contract
        contract_comps = contract_string.split(' ')

        # get dte
        dt = datetime.strptime(' '.join(contract_comps[1:4]), '%B %d %Y').date()
        dte = (dt - date.today()).days
        contract_data[0].append(dte)

        # get quotes
        underlying_quote = api.fetch_underlying(contract_comps[0])[0]['last']
        contract_quote = api.fetch_contract(contract_string)[0]['last']
        contract_data[1].append(round(underlying_quote, 2))
        contract_data[2].append(round(contract_quote, 2))

        # get strike
        strike = int(contract_comps[4][1:])
        moneyness = strike / underlying_quote
        if np.abs(1.0 - moneyness) <= 0.015: moneyness_status = 'ATM'
        elif contract_comps[5] == 'Put' and moneyness < 1.0: moneyness_status = 'OTM'
        elif contract_comps[5] == 'Call' and moneyness > 1.0: moneyness_status = 'OTM'
        else: moneyness_status = 'ITM'
        contract_data[3].append(round(moneyness * 100, 2))
        contract_data[4].append(moneyness_status)

    # build dataframe
    df = pd.DataFrame(np.transpose(contract_data))
    df.index = np.array(portfolio_contracts)[:, 0]
    df.columns = ['dte', 'underlying_price ($)', 'contract_price ($)', 'moneyness (%)', 'status']
    df['return (%)'] = round((np.array(portfolio_contracts)[:, 1].astype(np.float64) / df['contract_price ($)'].astype(np.float64) - 1) * 100, 2)

    # print dataframe
    formatted_df = tabulate(df, headers='keys', tablefmt='psql')
    print(formatted_df)