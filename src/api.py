import requests
import time


class MarketAPI():
    
    def __init__(self, symbol):
        self.symbol = symbol

    def fetch_chain(self, expiration):
        try:
            url = 'https://sandbox.tradier.com/v1/markets/options/chains?symbol={}&expiration={}&greeks=true'.format(self.symbol, expiration)
            headers = {'Authorization': 'Bearer 04Y6IBpk0kQ7n4KeoNwYt57Blnm1', 'Accept': 'application/json'}
            r_data = requests.get(url, headers=headers)
            chain = r_data.json()['options']['option']
            time.sleep(1)
        except: return None
        return chain

    def fetch_expirations(self):
        try:
            url = 'https://sandbox.tradier.com/v1/markets/options/expirations?symbol={}'.format(self.symbol)
            headers = {'Authorization': 'Bearer 04Y6IBpk0kQ7n4KeoNwYt57Blnm1', 'Accept': 'application/json'}
            r_data = requests.get(url, headers=headers)
            expirations = r_data.json()['expirations']['date']
            time.sleep(1)
        except: return None
        return expirations

    def fetch_underlying(self):
        try:
            url = 'https://sandbox.tradier.com/v1/markets/quotes?symbols={}'.format(self.symbol)
            headers = {'Authorization': 'Bearer 04Y6IBpk0kQ7n4KeoNwYt57Blnm1', 'Accept': 'application/json'}
            r_data = requests.get(url, headers=headers)
            underlying = r_data.json()['quotes']['quote']
        except: return None
        return underlying