import requests
import os
from dotenv import load_dotenv
load_dotenv(verbose=True)


class TradierAPI():

    def fetch_chain(self, symbol, expiration, greeks=False):
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
