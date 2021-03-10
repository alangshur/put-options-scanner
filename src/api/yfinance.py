import os
import requests
import yfinance as yf
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
load_dotenv(verbose=True)


class YFinanceAPI:

    def fetch_last_quote(self, symbol):
        try:

            # get last quote
            ticker = yf.Ticker(symbol)
            data = ticker.history()
            last_quote = (data.tail(1)['Close'].iloc[0])

        except: return None
        return round(last_quote, 2)

    def fetch_year_quotes(self, symbol):
        try:

            # get year quotes
            ticker = yf.Ticker(symbol)
            data = ticker.history(period='1y', interval='1d')
            data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
            data.columns = ['open', 'high', 'low', 'close', 'volume']
            year_quotes = data.to_dict(orient='records')
        except: return None
        return year_quotes