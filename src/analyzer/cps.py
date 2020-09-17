from src.analyzer.base import OptionAnalyzerBase
from datetime import datetime, date
from itertools import permutations
from scipy.interpolate import interp1d
import mibian
import time

class CreditPutSpreadAnalyzer(OptionAnalyzerBase):

    def __init__(self,
        risk_free_rate=1.0,
        option_price_floor=0.10,
        open_interest_floor=10,
        volume_floor=5,
        max_spread_width=20
    ):

        super().__init__()
        self.risk_free_rate = risk_free_rate
        self.option_price_floor = option_price_floor
        self.open_interest_floor = open_interest_floor
        self.volume_floor = volume_floor
        self.max_spread_width = max_spread_width

    def run(self, 
        symbol, 
        underlying,
        expiration, 
        chain
    ):

        # get dte
        now_dt = date.today()
        exp_dt = datetime.strptime(expiration, '%Y-%m-%d').date()
        dte = (exp_dt - now_dt).days

        # filter bad levels
        filtered_levels = []
        for level in chain:
            if level['option_type'] != 'put': continue
            if underlying <= level['strike']: continue
            if level['last'] is None or level['last'] <= self.option_price_floor: continue
            if level['bid'] is None or level['bid'] <= self.option_price_floor: continue
            if level['ask'] is None or level['ask'] <= self.option_price_floor: continue
            if level['open_interest'] <= self.open_interest_floor: continue
            if level['volume'] <= self.volume_floor: continue
            filtered_levels.append(level)
 
        # iterate over spreads
        spread_collection = []
        for buy_level, sell_level in permutations(filtered_levels, 2):
            if buy_level['strike'] >= sell_level['strike']: continue

            # calculate/filter p/l
            width = sell_level['strike'] - buy_level['strike']
            premium = sell_level['bid'] - buy_level['ask']
            max_loss = width - premium
            be = sell_level['strike'] - premium
            if width > self.max_spread_width: continue
            if be <= buy_level['strike']: continue
            if be >= sell_level['strike']: continue

            # build pricing model
            buy_greeks = self.__get_pricing_model(underlying, buy_level, dte)
            sell_greeks = self.__get_pricing_model(underlying, sell_level, dte)

            # calculate/filter r/r
            risk_reward_ratio = premium / max_loss
            if risk_reward_ratio <= 0.0: continue

            # interpolate b/e prob
            prob_buy_itm = abs(buy_greeks['delta'])
            prob_sell_itm = abs(sell_greeks['delta'])
            prob_be_itm = self.__interpolate_dual_delta(
                buy_level, sell_level, 
                prob_buy_itm, prob_sell_itm, 
                be
            )

            # calculated/filter profits (TODO: FIX AND BETTER INTERPOLATE BE PROFIT)
            prob_full_profit = 1.0 - prob_sell_itm
            prob_be_profit = 1.0 - prob_be_itm
            adjusted_full_profit = prob_full_profit * premium + (1 - prob_full_profit) * -max_loss
            adjusted_be_profit = prob_be_profit * premium + (1 - prob_be_profit) * -max_loss
            if adjusted_full_profit <= 0.0: continue
            if adjusted_be_profit <= 0.0: continue

            # add valid spreads
            spread_collection.append('{} {} +{}/-{}'.format(
                symbol, expiration, buy_level['strike'], 
                sell_level['strike']
            ))

        return spread_collection

    def validate(self, 
        symbol=None, 
        underlying=None,
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

        else: return True

    def __get_pricing_model(self, underlying, level, dte):
        option_data = [underlying, level['strike'], self.risk_free_rate, dte]
        bs = mibian.BS(option_data, putPrice=level['last'])
        bs = mibian.BS(option_data, volatility=bs.impliedVolatility)

        # map greeks
        return {
            'delta': bs.putDelta,
            'dual_delta': bs.putDelta2,
            'theta': bs.putTheta,
            'vega': bs.vega,
            'gamma': bs.gamma,
            'rho': bs.putRho
        }

    def __interpolate_dual_delta(self,
        buy_level, 
        sell_level, 
        prob_buy_itm, 
        prob_sell_itm,
        be
    ):

        # approximate dual delta curve
        func = interp1d(
            x=[buy_level['strike'], sell_level['strike']], 
            y=[prob_buy_itm, prob_sell_itm],
            kind='linear'
        )

        # fit b/e price
        return func(be)