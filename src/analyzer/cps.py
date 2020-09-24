from py_vollib.black_scholes_merton.implied_volatility import implied_volatility
from py_vollib.black_scholes_merton.greeks import analytical
from src.analyzer.base import OptionAnalyzerBase
import numpy.polynomial.polynomial as poly
from datetime import datetime, date
from itertools import permutations
from numpy import warnings
import numpy as np
import time


class CreditPutSpreadAnalyzer(OptionAnalyzerBase):

    def __init__(self,
        min_filtered_levels=5,
        option_price_floor=0.10,
        open_interest_floor=5,
        volume_floor=5,
        max_spread_width=20
    ):

        super().__init__('cps')
        self.min_filtered_levels = min_filtered_levels
        self.option_price_floor = option_price_floor
        self.open_interest_floor = open_interest_floor
        self.volume_floor = volume_floor
        self.max_spread_width = max_spread_width

    def run(self, 
        symbol, 
        underlying,
        dividend,
        expiration, 
        chain,
        risk_free_rate
    ):

        # get dte
        now_dt = date.today()
        exp_dt = datetime.strptime(expiration, '%Y-%m-%d').date()
        dte = (exp_dt - now_dt).days

        # filter bad levels
        filt_chain = []
        for level in chain:
            if self.__filter_level(symbol, underlying, level):
                filt_chain.append(level)

        # load greeks/iv
        greeks_lookup = self.__load_greeks(underlying, dividend, filt_chain, dte, risk_free_rate)
        if len(greeks_lookup) < self.min_filtered_levels: return []
        coefs = self.__build_delta_curve(greeks_lookup)
 
        # iterate over spreads
        spread_collection = []
        for buy_level, sell_level in permutations(filt_chain, 2):
            if buy_level['strike'] >= sell_level['strike']: continue

            # calculate/filter p/l
            width = sell_level['strike'] - buy_level['strike']
            premium = sell_level['bid'] - buy_level['ask']
            max_loss = premium - width
            be = sell_level['strike'] - premium
            risk_reward_ratio = abs(premium / max_loss)
            if width > self.max_spread_width: continue
            if premium <= 0.0: continue
            if be <= buy_level['strike']: continue
            if be >= sell_level['strike']: continue

            # fetch greeks
            buy_greeks = greeks_lookup[buy_level['strike']]
            sell_greeks = greeks_lookup[sell_level['strike']]

            # get probabilities
            prob_max_loss = abs(buy_greeks['delta'])
            prob_max_profit = 1 - abs(sell_greeks['delta'])

            # calculated/filter expected profits
            expected_spread_profit, total_spread_prob = self.__approximate_risk_adjusted_spread_profit(
                width, premium, buy_level['strike'], sell_level['strike'], coefs, resolution=0.1)
            expected_profit = prob_max_loss * max_loss
            expected_profit += prob_max_profit * premium
            expected_profit += expected_spread_profit
            if expected_profit <= 0.0: continue
            if abs(1.0 - (prob_max_loss + total_spread_prob + prob_max_profit)) >= 0.1: continue

            # calulate net greeks
            net_delta = buy_greeks['delta'] - sell_greeks['delta']
            net_theta = buy_greeks['theta'] - sell_greeks['theta']
            net_vega = buy_greeks['vega'] - sell_greeks['vega']
            net_gamma = buy_greeks['gamma'] - sell_greeks['gamma']
            
            # add valid spreads
            spread_collection.append([
                '{} {} +{}/-{}'.format(symbol, expiration, buy_level['strike'], sell_level['strike']), # description
                round(width, 2), # width
                round(premium * 100, 2), # premium
                round(max_loss * 100, 2), # max loss
                round(be, 2), # break even
                round(risk_reward_ratio, 2), # risk reward ratio
                round(prob_max_loss, 2), # probability of max loss
                round(prob_max_profit, 2), # probability of max profit
                round(expected_profit * 100, 2), # risk-adjusted profit
                round(net_delta, 2), # position delta
                round(net_theta, 2), # position theta
                round(net_vega, 2), # position vega
                round(net_gamma, 2) # position gamma
            ])

        return spread_collection

    def validate(self, 
        symbol=None, 
        underlying=None,
        dividend=None,
        expiration=None, 
        chain=None
    ):
        
        # filter expirations
        if expiration is not None:

            # get dte
            now_dt = date.today()
            exp_dt = datetime.strptime(expiration, '%Y-%m-%d').date()
            dte = (exp_dt - now_dt).days

            # target dte range
            if dte < 21 or dte > 91: return False
            else: return True

        # filter symbols
        elif symbol is not None:
            
            # ignore toronto exchange
            if symbol.endswith('.TO'): return False
            else: return True

        else: return True

    def __load_greeks(self, underlying, dividend, chain, dte, risk_free_rate):
        greeks_lookup = {}

        # load greeks for chain
        for level in chain:
            greeks = self.__calculate_greeks(underlying, dividend, level, dte, risk_free_rate)
            greeks_lookup[level['strike']] = greeks

        return greeks_lookup

    def __calculate_greeks(self, underlying, dividend, level, dte, risk_free_rate):

        # get BS components
        price = level['last']
        S = underlying
        K = level['strike']
        t = dte / 365.2422
        r = risk_free_rate
        q = dividend
        flag = 'p'

        # calculate IV
        sigma = implied_volatility(price, S, K, t, r, q, flag)

        # maplc greeks
        return {
            'iv': sigma,
            'delta': analytical.delta(flag, S, K, t, r, sigma, q),
            'theta': analytical.theta(flag, S, K, t, r, sigma, q),
            'vega': analytical.vega(flag, S, K, t, r, sigma, q),
            'gamma': analytical.gamma(flag, S, K, t, r, sigma, q),
            'rho': analytical.rho(flag, S, K, t, r, sigma, q)
        }

    def __build_delta_curve(self, greeks_lookup):
        fit_data = np.array([(k, v['delta']) for k, v in greeks_lookup.items()])

        # fit with dynamic order
        order, coefs = 11, None
        with warnings.catch_warnings():
            warnings.filterwarnings('error')
            while coefs is None:
                try: coefs = poly.polyfit(fit_data[:, 0], np.abs(fit_data[:, 1]), order)
                except np.polynomial.polyutils.RankWarning: order -= 1
        return coefs

    def __approximate_risk_adjusted_spread_profit(self, width, premium, buy_strike, 
            sell_strike, coefs, resolution=0.1):

        total_spread_prob = 0
        expected_profit = 0
        for i in range(int(width / resolution)):

            # resolve price interval
            lo = round(buy_strike + resolution * i, 2)
            hi = round(lo + resolution, 2)
            future_price = round((lo + hi) / 2, 2)
            
            # approximate interval probability
            lo_delta = self.__interpolate_delta(lo, coefs)
            hi_delta = self.__interpolate_delta(hi, coefs)
            interval_prob = hi_delta - lo_delta
            if interval_prob < 0: interval_prob = 0
            
            # calculate expected interval return
            interval_return = (future_price - sell_strike) + premium
            expected_profit += interval_prob * interval_return
            total_spread_prob += interval_prob
        
        return expected_profit, total_spread_prob

    def __interpolate_delta(self, price, coefs):
        return poly.polyval(price, coefs)

    def __filter_level(self, symbol, underlying, level):
        if level['option_type'] != 'put': return False # ignore put options
        if underlying <= level['strike']: return False # ignore ATM/ITM options
        if level['root_symbol'] != symbol: return False # ignore adjusted options
        if level['last'] is None or level['last'] <= self.option_price_floor: return False # ignore low last price
        if level['bid'] is None or level['bid'] <= self.option_price_floor: return False # ignore low bid price
        if level['ask'] is None or level['ask'] <= self.option_price_floor: return False # ignore low ask price
        if level['open_interest'] <= self.open_interest_floor: return False # ignore low open interest
        if level['volume'] <= self.volume_floor: return False # ignore low volume
        return True