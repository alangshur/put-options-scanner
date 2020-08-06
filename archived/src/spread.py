import matplotlib.pyplot as plt
from datetime import datetime, date
import pandas as pd
import numpy as np
import itertools
import tqdm
import sys
import json
import api
import time


def main():
    now_dt = date.today()
    collection = []
    errors = 0

    # load large universe
    f = open('../universes/large', 'r')
    large = f.read().split(',')
    f.close()

    # load active universe
    f = open('../universes/active', 'r')
    active = f.read().split(',')
    f.close()

    # combine universes
    active.extend(large)
    universe = list(set(active))

    # iterate over symbols
    for symbol in tqdm.tqdm(universe):

        # build api
        mapi = api.MarketAPI(symbol)

        # fetch underlying
        underlying = mapi.fetch_underlying()
        if underlying is None: 
            errors += 1
            continue
        underlying_price = underlying['last']

        # skip big underlyings
        if underlying_price > 500 or underlying_price < 5: continue

        # iterate over expirations
        expirations = mapi.fetch_expirations()
        if expirations is None: 
            errors += 1
            continue
        for expiration in expirations:
            try:

                # filter expirations
                exp_dt = datetime.strptime(expiration, '%Y-%m-%d').date()
                dte = (exp_dt - now_dt).days
                if dte < 63 and dte > 21:
                
                    # fetch chain
                    chain = mapi.fetch_chain(expiration)
                    if chain is None:
                        time.sleep(1.05)
                        errors += 1
                        continue
                    then = time.time()

                    # filter chain levels
                    filtered_levels = []
                    strad_call_diff = float('inf')
                    strad_put_diff = float('inf')
                    strad_call_level = None
                    strad_put_level = None
                    total_diff = 0
                    total_civ = 0
                    for level in chain:
                        
                        # find straddle levels
                        diff = level['strike'] - underlying_price
                        if level['option_type'] == 'call' and abs(diff) < strad_call_diff and diff <= 0:
                            strad_call_diff = abs(diff)
                            strad_call_level = level
                        elif level['option_type'] == 'put' and abs(diff) < strad_put_diff and diff >= 0:
                            strad_put_diff = abs(diff)
                            strad_put_level = level

                        # skip call options
                        if level['option_type'] != 'put': continue

                        # skip ITM/ATM options 
                        if underlying_price <= level['strike']: continue
                            
                        # skip options with small premiums
                        if not level['last'] or level['last'] <= 0.10: continue
                            
                        # add weighted IVs 
                        total_diff += diff
                        total_civ += diff * level['greeks']['mid_iv']

                        # add valid levels
                        filtered_levels.append(level)
                        
                    # calculate IV/strad STD
                    civ = total_civ / total_diff
                    iv_std = underlying_price * civ * np.sqrt(dte / 365)
                    try: strad_std = strad_call_level['last'] + strad_put_level['last']
                    except: strad_std = iv_std

                    # filter put credit spreads
                    for sell_level, buy_level in itertools.permutations(filtered_levels, 2):
                        
                        # skip unordered pairs
                        if sell_level['strike'] <= buy_level['strike']: continue
                            
                        # calculate premium/max loss
                        sell_price = sell_level['bid']
                        buy_price = buy_level['ask']
                        premium = sell_price - buy_price
                        max_loss = (sell_level['strike'] - buy_level['strike']) - premium
                        
                        # calculate risk-reward ratio
                        risk_reward = premium / max_loss
                        prob_profit = 1.0 - abs(sell_level['greeks']['delta'])
                        adjusted_profit = prob_profit * premium + (1 - prob_profit) * -max_loss
                        
                        # skip negative risk-reward
                        if risk_reward <= 0.0: continue

                        # record valid spread
                        width = sell_level['strike'] - buy_level['strike']
                        if (width <= 5.0) and (adjusted_profit > 0.0) and (risk_reward >= 0.33) and \
                            (-sell_level['greeks']['delta'] < 0.3):
                            collection.append([
                                symbol,
                                underlying_price,
                                expiration,
                                dte,
                                sell_level['strike'],
                                buy_level['strike'],
                                round(width, 1),
                                round(premium * 100, 3),
                                round(max_loss * 100, 3),
                                round(sell_level['strike'] - premium, 3),
                                round(adjusted_profit * 100, 3),
                                round(risk_reward * 100, 3),
                                round((underlying_price - sell_level['strike']) / iv_std, 3),
                                round((underlying_price - sell_level['strike']) / strad_std, 3),
                                round(-sell_level['greeks']['delta'] * 100, 3),
                                round(-buy_level['greeks']['delta'] * 100, 3)
                            ])

                    # throttle requests
                    now = time.time()           
                    spent = then - now
                    if spent < 1.05: 
                        time.sleep(1.05 - (then - now))

            except KeyboardInterrupt: return
            except Exception as e: continue


    # build dataframe
    if len(collection) > 0:
        df = pd.DataFrame.from_records(collection)
        df.columns = [
            'symbol', 'udl_price', 'exp', 'dte', \
            's_strike', 'l_strike', 'width', 'premium', \
            'max_loss', 'be_price', 'adj_profit', 'rr_percent', \
            'iv_z_score', 'strad_z_score', 's_delta', 'l_delta'
        ]
        df.to_csv('../scans/{}.csv'.format(now_dt.strftime('%Y-%m-%d')))
    print('\n\nTotal results: {}'.format(len(collection)))
    print('Total errors: {}'.format(errors))


if __name__ == '__main__':
    main()