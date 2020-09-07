import requests
import os
from dotenv import load_dotenv
load_dotenv(verbose=True)


class TradierAPI():

    def fetch_chain(self, symbol, expiration):
        try:

            # format request
            url = os.environ.get('TRADIER_ENDPOINT') + \
                'options/chains?' + \
                'symbol={}&'.format(symbol) + \
                'expiration={}&'.format(expiration) + \
                'greeks=true'
            headers = {
                'Authorization': 'Bearer ' + os.environ.get('TRADIER_API_KEY'), 
                'Accept': 'application/json'
            }

            # send request
            r_data = requests.get(url, headers=headers)
            chain = r_data.json()['options']['option']
            rate_allowed = r_data.headers['X-Ratelimit-Allowed']

        except: return None, 0
        return chain, rate_allowed

    def fetch_expirations(self, symbol):
        try:

            # format request
            url = os.environ.get('TRADIER_ENDPOINT') + \
                '/options/expirations?' + \
                'symbol={}'.format(symbol)
            headers = {
                'Authorization': 'Bearer ' + os.environ.get('TRADIER_API_KEY'), 
                'Accept': 'application/json'
            }

            # send request
            r_data = requests.get(url, headers=headers)
            expirations = r_data.json()['expirations']['date']
            rate_allowed = r_data.headers['X-Ratelimit-Allowed']

        except: return None, 0
        return expirations, rate_allowed

    def fetch_underlying(self, symbol):
        try:
            
            # format request
            url = os.environ.get('TRADIER_ENDPOINT') + \
                '/quotes?' + \
                'symbols={}'.format(symbol)
            headers = {
                'Authorization': 'Bearer ' + os.environ.get('TRADIER_API_KEY'), 
                'Accept': 'application/json'
            }

            # send request
            r_data = requests.get(url, headers=headers)
            underlying = r_data.json()['quotes']['quote']
            rate_allowed = r_data.headers['X-Ratelimit-Allowed']
            
        except: return None, 0
        return expirations, rate_allowed