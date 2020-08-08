import requests
from dotenv import load_dotenv
load_dotenv(verbose=True)


class TradierAPI():

    def fetch_chain(self, symbol, expiration):
        try:

            # format request
            url = os.environ.get('TRADIER_ENDPOINT') + 
                'options/chains?' + \
                'symbol={}&' + \
                'expiration={}&' + \
                'greeks=true'
                .format(symbol, expiration)
            headers = {
                'Authorization': 'Bearer ' + os.environ.get('TRADIER_API_KEY'), 
                'Accept': 'application/json'
            }

            # send request
            r_data = requests.get(url, headers=headers)
            chain = r_data.json()['options']['option']
        except: return None
        return chain

    def fetch_expirations(self, symbol):
        try:

            # format request
            url = os.environ.get('TRADIER_ENDPOINT') + 
                'markets/options/expirations?' + \
                'symbol={}'
                .format(symbol)
            headers = {
                'Authorization': 'Bearer ' + os.environ.get('TRADIER_API_KEY'), 
                'Accept': 'application/json'
            }

            # send request
            r_data = requests.get(url, headers=headers)
            expirations = r_data.json()['expirations']['date']
        except: return None
        return expirations

    def fetch_underlying(self, symbol):
        try:
            
            # format request
            url = os.environ.get('TRADIER_ENDPOINT') + 
                'markets/quotes?' + \
                'symbols={}'
                .format(symbol)
            headers = {
                'Authorization': 'Bearer ' + os.environ.get('TRADIER_API_KEY'), 
                'Accept': 'application/json'
            }

            # send request
            r_data = requests.get(url, headers=headers)
            underlying = r_data.json()['quotes']['quote']
        except: return None
        return underlying

