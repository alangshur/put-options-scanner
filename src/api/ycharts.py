from bs4 import BeautifulSoup
from datetime import date
from pathlib import Path
import requests
import atexit
import json
import os
from dotenv import load_dotenv
load_dotenv(verbose=True)


class YChartsAPI:

    def __init__(self):

        # save date
        self.date = date.today().strftime('%Y-%m-%d')
        
        # load cache
        Path('cache').mkdir(exist_ok=True)
        if Path('cache/ycharts.json').exists():
            f = open('cache/ycharts.json', 'r')
            self.cache = json.load(f)
            f.close()

            # validate date
            if self.cache['updated_at'] != self.date:
                self.cache = {}

        # reset cache
        else: self.cache = {}

        # register cache save
        atexit.register(self.__save_cache)

    def fetch_risk_free_rate(self):
        try:

            # check cache
            if 'risk_free_rate' in self.cache:
                return self.cache['risk_free_rate']

            # send request
            target = '10_year_treasury_rate'
            url = os.environ.get('YCHARTS_ENDPOINT') + target
            r_data = requests.get(url)
            html = r_data.content

            # parse html
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text()

            # extract risk-free rate
            key = '10 Year Treasury Rate is at '
            pos = text.find(key)
            text = text[pos + len(key):]
            text = text[:text.find('%')]
            risk_free_rate = round(float(text) / 100, 5)

            # cache yield
            self.cache = {
                'risk_free_rate': risk_free_rate,
                'updated_at': self.date
            }

        except: return 0.0
        return risk_free_rate

    def __save_cache(self):
        f = open('cache/ycharts.json', 'w+')
        json.dump(self.cache, f)
        f.close()

        # remove empty cache
        if len(self.cache) == 0:
            Path('cache/ycharts.json').unlink()