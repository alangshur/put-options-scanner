from bs4 import BeautifulSoup
from datetime import date
from pathlib import Path
import multiprocessing
import requests
import atexit
import json
import os
from dotenv import load_dotenv
load_dotenv(verbose=True)


class YFinanceAPI:

    def __init__(self):

        # save date
        self.date = date.today().strftime('%Y-%m-%d')
        
        # load cache
        Path('cache').mkdir(exist_ok=True)
        if Path('cache/yfinance.json').exists():
            f = open('cache/yfinance.json', 'r')
            cache = json.load(f)
            f.close()

            # validate dates
            for symbol in list(cache.keys()):
                if cache[symbol]['updated_at'] != self.date:
                    del cache[symbol]

        # reset cache
        else: cache = {}

        # add cache to shared mem
        manager = multiprocessing.Manager()
        self.cache = manager.dict(cache)

        # register cache save
        atexit.register(self.__save_cache)

    def fetch_annual_yield(self, symbol):
        try:

            # check cache
            if symbol in self.cache:
                return self.cache[symbol]['div_yield']

            # send request
            url = os.environ.get('YFINANCE_ENDPOINT') + symbol
            r_data = requests.get(url)
            html = r_data.content
            
            # parse html
            soup = BeautifulSoup(html, 'lxml')
            text = soup.get_text()

            # extract yields
            div_yield = self.__extract_equity_yield(text)
            if div_yield is None: div_yield = self.__extract_etf_yield(text)
            if div_yield is None: div_yield = 0.0

            # cache yield
            self.cache[symbol] = {
                'div_yield': div_yield,
                'updated_at': self.date
            }

        except: return 0.0
        return div_yield

    def __extract_equity_yield(self, text):
        try:
            key = 'Forward Dividend & Yield'
            pos = text.find(key)
            if pos == -1: return None
            text = text[pos + len(key):]
            text = text[text.find('(') + 1:text.find('%')]
            div_yield = round(float(text) / 100, 5)
            return div_yield
        except: return None

    def __extract_etf_yield(self, text):
        try:
            key = 'Yield'
            pos = text.find(key)
            if pos == -1: return None
            text = text[pos + len(key):]
            text = text[:text.find('%')]
            div_yield = round(float(text) / 100, 5)
            return div_yield
        except: return None

    def __save_cache(self):
        f = open('cache/yfinance.json', 'w+')
        json.dump(self.cache._getvalue(), f)
        f.close()

        # remove empty cache
        if len(self.cache) == 0:
            Path('cache/yfinance.json').unlink()
