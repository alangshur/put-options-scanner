from datetime import datetime
import requests
import os
from dotenv import load_dotenv
load_dotenv(verbose=True)


class TradierAPI:

    def fetch_chain(self, symbol, expiration, greeks=True):
        try:

            # correct symbol
            symbol = symbol.replace('.', '/')

            # format request
            url = os.environ.get('TRADIER_ENDPOINT') + \
                'options/chains?' + \
                'symbol={}&'.format(symbol) + \
                'expiration={}&'.format(expiration) + \
                'greeks={}'.format(str(greeks).lower())
            headers = {
                'Authorization': 'Bearer ' + os.environ.get('TRADIER_API_KEY'), 
                'Accept': 'application/json'
            }

            # send request
            r_data = requests.get(url, headers=headers)
            chain = r_data.json()['options']['option']
            rate_available = int(r_data.headers['X-Ratelimit-Available'])
            rate_allowed = int(r_data.headers['X-Ratelimit-Allowed'])
            rate_expiry = int(r_data.headers['X-Ratelimit-Expiry'])

        except: return None
        return (
            chain, 
            rate_available,
            rate_allowed,
            rate_expiry
        )

    def fetch_expirations(self, symbol):
        try:

            # correct symbol
            symbol = symbol.replace('.', '/')

            # format request
            url = os.environ.get('TRADIER_ENDPOINT') + \
                'options/expirations?' + \
                'symbol={}'.format(symbol)
            headers = {
                'Authorization': 'Bearer ' + os.environ.get('TRADIER_API_KEY'), 
                'Accept': 'application/json'
            }

            # send request
            r_data = requests.get(url, headers=headers)
            expirations = r_data.json()['expirations']['date']
            rate_available = int(r_data.headers['X-Ratelimit-Available'])
            rate_allowed = int(r_data.headers['X-Ratelimit-Allowed'])
            rate_expiry = int(r_data.headers['X-Ratelimit-Expiry'])

        except: return None
        return (
            expirations, 
            rate_available,
            rate_allowed,
            rate_expiry
        )

    def fetch_underlying(self, symbol):
        try:

            # correct symbol
            symbol = symbol.replace('.', '/')
            
            # format request
            url = os.environ.get('TRADIER_ENDPOINT') + \
                'quotes?' + \
                'symbols={}'.format(symbol)
            headers = {
                'Authorization': 'Bearer ' + os.environ.get('TRADIER_API_KEY'), 
                'Accept': 'application/json'
            }

            # send request
            r_data = requests.get(url, headers=headers)
            underlying = r_data.json()['quotes']['quote']
            rate_available = int(r_data.headers['X-Ratelimit-Available'])
            rate_allowed = int(r_data.headers['X-Ratelimit-Allowed'])
            rate_expiry = int(r_data.headers['X-Ratelimit-Expiry'])
            
        except: return None
        return (
            underlying, 
            rate_available,
            rate_allowed,
            rate_expiry
        )

    def fetch_contract(self, contract_string):
        try:

            # format contract symbol
            contract_comps = contract_string.split(' ')
            dt = datetime.strptime(' '.join(contract_comps[1:4]), '%B %d %Y')
            contract_symbol = '{}{}{}{}{}{}'.format(
                contract_comps[0],
                str(dt.year)[-2:],
                str(dt.month).zfill(2),
                str(dt.day).zfill(2),
                contract_comps[5][0].upper(),
                str(int(float(contract_comps[4][1:]) * 1000)).zfill(8)
            )

            # format request
            url = os.environ.get('TRADIER_ENDPOINT') + \
                'quotes?' + \
                'symbols={}&'.format(contract_symbol) + \
                'greeks=false'
            headers = {
                'Authorization': 'Bearer ' + os.environ.get('TRADIER_API_KEY'), 
                'Accept': 'application/json'
            }

            # send request
            r_data = requests.get(url, headers=headers)
            contract_quote = r_data.json()['quotes']['quote']
            rate_available = int(r_data.headers['X-Ratelimit-Available'])
            rate_allowed = int(r_data.headers['X-Ratelimit-Allowed'])
            rate_expiry = int(r_data.headers['X-Ratelimit-Expiry'])
            
        except: return None
        return (
            contract_quote, 
            rate_available,
            rate_allowed,
            rate_expiry
        )