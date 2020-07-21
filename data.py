import pandas_market_calendars as mcal
from json.decoder import JSONDecodeError
from datetime import datetime
import requests
import time
import csv


def get_market_holidays(start_date, end_date):
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)
    valid_days = nyse.valid_days(start_date=start_date, end_date=end_date)
    early_days = nyse.early_closes(schedule=schedule)
    valid_dates = [str(t.date()) for t in valid_days.to_list()]
    early_dates = [str(t.date()) for t in early_days.index.to_list()]
    comb_dates = list(set(valid_dates).union(set(early_dates)))
    dates = sorted(comb_dates, key=lambda d: datetime.strptime(d, '%Y-%m-%d'))
    return dates, set(early_dates)


def write_data(writer, results):
    date_all = set()
    data_all = []

    # write each bar in results
    for i in range(len(results)):
        bar = results[i]

        # get bar timing
        timestamp = bar['t']
        utc_date = time.gmtime(timestamp / 1000)
        date = time.strftime('%Y-%m-%d', utc_date)
        date_all.add(date)

        # build datapoint
        data_all.append([date, bar['o'], bar['h'], \
            bar['l'], bar['c'], bar['v']])
    
    # bulk write bars   
    writer.writerows(data_all)
    return date_all


def collect_data(ticker, start_year='2005', end_year='2020'):
    market_days = set(get_market_holidays(
        str(start_year) + '-01-01',
        str(end_year) + '-6-21'
    )[0])

    # write data iteratively
    with open('data/{}-1d.csv'.format(ticker), 'w+') as raw_file:
        file_writer = csv.writer(raw_file)

        # loop over years
        for year in range(int(start_year), int(end_year) + 1):
            start_date = str(year) + '-01-01'
            end_date = str(year) + '-12-31'

            # fetch data
            while True:
                try:
                    r_data = requests.get(
                        'https://api.polygon.io/v2/aggs/ticker/{}/range/1/day/'.format(ticker) \
                        + '{}/{}'.format(start_date, end_date) \
                        + '?apiKey=AK952QW390M7XSKYCHQQ'
                    ).json()
                    break
                except JSONDecodeError:
                    continue

            # verify data
            if r_data['status'] == 'ERROR': continue
            elif r_data['resultsCount'] == 0: continue
            else: 
                days_added = write_data(file_writer, r_data['results'])
                market_days.difference_update(days_added)

    return len(market_days)


if __name__ == '__main__':

    # specify ticker targets
    target_tickers = ['SPY', 'AAPL', 'TSLA', 'GLD']

    # iteratively collet ticker data
    for ticker in target_tickers:
        errors = collect_data(ticker)
        print('Loaded {}: {} missed days.'.format(ticker, errors))