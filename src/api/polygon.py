import os
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
load_dotenv(verbose=True)


class PolygonAPI():

    def fetch_quotes_year(self, symbol):
        try:

            # get date range
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d')
            then = now - relativedelta(years=1)
            then_str = then.strftime('%Y-%m-%d')

            # format request
            url = os.environ.get('POLYGON_ENDPOINT') + \
                'aggs/ticker/{}/range/1/day/'.format(symbol) + \
                '{}/{}?'.format(then_str, now_str) + \
                'apiKey={}'.format(os.environ.get('POLYGON_API_KEY'))
            headers = {
                'Accept': 'application/json'
            }

            # send request
            r_data = requests.get(url, headers=headers)
            quotes = r_data.json()['results']
        except: return None

        # format quotes
        formatted_quotes = []
        for quote in quotes:
            formatted_quotes.append({
                'open': quote['o'],
                'high': quote['h'],
                'low': quote['l'],
                'close': quote['c'],
                'volume': quote['v']
            })

        return formatted_quotes