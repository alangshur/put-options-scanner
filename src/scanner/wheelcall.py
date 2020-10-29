from src.scanner.base import ScannerBase
from src.api.tradier import TradierAPI
from src.api.polygon import PolygonAPI
from datetime import datetime, date
from scipy.stats import norm
import numpy as np


class WheelCallScanner(ScannerBase):

    def __init__(self,
        put_ticker,
        put_strike,
        cost_basis,
        save_scan=True,
        scan_name=None,

        option_price_floor=0.10,
        open_interest_floor=3,
        volume_floor=3,
        strike_threshold=0.05
    ):

        self.put_ticker = put_ticker
        self.put_strike = put_strike
        self.cost_basis = cost_basis
        self.save_scan = save_scan
        self.scan_name = scan_name

        self.option_price_floor = option_price_floor
        self.open_interest_floor = open_interest_floor
        self.volume_floor = volume_floor
        self.strike_threshold = strike_threshold

        # build scan name
        if self.scan_name is None:
            k = 'wheelcall'
            d = str(datetime.today()).split(' ')[0]
            t = str(datetime.today()).split(' ')[-1].split('.')[0]
            self.scan_name = '{}_{}_{}'.format(k, d, t)
        
    def run(self):

        # build resources
        self.option_api = TradierAPI()
        self.stock_api = PolygonAPI()
        results = []

        # pull underyling and option expirations
        underlying = self.stock_api.fetch_last_quote(self.put_ticker)
        expirations = self.option_api.fetch_expirations(self.put_ticker)[0]
        for expiration in expirations:

            # target dte range
            now_dt = date.today()
            exp_dt = datetime.strptime(expiration, '%Y-%m-%d').date()
            dte = (exp_dt - now_dt).days
            if dte >= 60: continue

            # iterate over chain levels
            chain = self.option_api.fetch_chain(self.put_ticker, expiration)[0]
            for level in chain:
                if not self.__filter_call_levels(self.put_ticker, underlying, level):
                    continue

                # calculate contract stats
                premium = level['bid']
                roc = (premium * 100) / (self.put_strike * 100)
                new_cost_basis = self.cost_basis - premium
                underlying_gain = (level['strike'] - self.put_strike) * 100
                moneyness = level['strike'] / underlying
                if moneyness < (1 - self.strike_threshold): continue
                if moneyness > (1 + self.strike_threshold): continue

                # calculate movement stats
                iv = level['greeks']['bid_iv']
                std = underlying * iv * np.sqrt(dte / 365)
                prob_itm_delta = level['greeks']['delta']
                prob_itm_iv = norm.cdf((level['strike'] - underlying) / std)
                
                # save results
                results.append([
                    level['description'], # contract description
                    round(underlying, 2), # underlying price
                    round(premium * 100, 2), # upfront premium
                    round(dte, 0), # days to expiration
                    round(roc, 5), # return-on-capital
                    round(new_cost_basis, 2), # updated cost basis
                    round(underlying_gain, 2), # gain from underlying movement
                    round(moneyness, 5), # strike moneyness
                    round(prob_itm_delta, 5), # probability of itm (with delta) 
                    round(prob_itm_iv, 5), # probability of itm (with iv)
                    round(iv, 5), # contract implied volatility (percentage)
                ])

        # save results
        if self.save_scan:
            self.__save_scan(results)

        return {
            'results': results
        }

    def __filter_call_levels(self, symbol, underlying, level):
        if level['option_type'] != 'call': return False # ignore put options
        if level['root_symbol'] != symbol: return False # ignore adjusted options
        if level['last'] is None or level['last'] <= self.option_price_floor: return False # ignore low last price
        if level['bid'] is None or level['bid'] <= self.option_price_floor: return False # ignore low bid price
        if level['ask'] is None or level['ask'] <= self.option_price_floor: return False # ignore low ask price
        if level['open_interest'] <= self.open_interest_floor: return False # ignore low open interest
        if level['volume'] <= self.volume_floor: return False # ignore low volume
        return True

    def __save_scan(self, scan):

        # save file
        Path('scan').mkdir(exist_ok=True)
        f = open('scan/{}.csv'.format(self.scan_name), 'w+')
        csv.writer(f, delimiter=',').writerows(scan)
        f.close()



