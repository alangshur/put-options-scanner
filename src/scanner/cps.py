from py_vollib.black_scholes_merton.implied_volatility import implied_volatility
from py_vollib.black_scholes_merton.greeks import analytical
import numpy.polynomial.polynomial as poly
from src.scanner.base import ScannerBase
from src.util.atomic import AtomicBool
from src.api.tradier import TradierAPI
from src.api.polygon import PolygonAPI
from src.api.yfinance import YFinanceAPI
from src.api.ycharts import YChartsAPI
from datetime import datetime, date
from itertools import permutations
from queue import Queue, Empty
from numpy import warnings
from pathlib import Path
import multiprocessing
from tqdm import tqdm
import numpy as np
import threading
import math
import time
import csv
import os


class OptionCreditPutSpreadScanner(ScannerBase):

    def __init__(self, 
        uni_list=None, 
        uni_file=None,
        num_processes=6,
        save_scan=True,
        log_changes=True
    ):

        self.uni_list = uni_list
        self.uni_file = uni_file
        self.num_processes = num_processes
        self.save_scan = save_scan
        self.log_changes = log_changes

        # fetch universe
        if self.uni_file is not None:
            f = open(self.uni_file, 'r')
            uni_list = list(csv.reader(f))
            self.uni = [row[0] for row in uni_list[1:]]        
            f.close()
        elif self.uni_list is not None:
            self.uni = self.uni_list
        else:
            raise Exception('No universe specified.')

        # build scan name
        k = 'cps'
        d = str(datetime.today()).split(' ')[0]
        t = str(datetime.today()).split(' ')[-1].split('.')[0]
        self.scan_name = '{}_{}_{}'.format(k, d, t)

    def run(self):

        # build resources
        option_api = TradierAPI()
        stock_api = PolygonAPI()
        dividend_api = YFinanceAPI()
        risk_free_rate_api = YChartsAPI()
        manager = multiprocessing.Manager()
        symbol_queue = manager.Queue()
        api_rate_cv = manager.Condition()
        api_rate_avail = manager.Value('i', 200)
        result_map = manager.dict()
        director_exit_flag = AtomicBool(value=False)

        # build meta resources
        fetch_failure_counter = manager.Value('i', 0)
        analysis_failure_counter = manager.Value('i', 0)
        log_queue = manager.Queue()

        # fetch risk-free rate
        risk_free_rate = risk_free_rate_api.fetch_risk_free_rate()

        # load queue
        for symbol in self.uni:
            symbol_queue.put(symbol)

        # run scanner threads
        s_processes = []
        for i in range(self.num_processes):
            s_process = OptionChainScannerProcess(
                process_num=i + 1, 
                option_api=option_api, 
                stock_api=stock_api,
                dividend_api=dividend_api,
                symbol_queue=symbol_queue, 
                api_rate_cv=api_rate_cv,
                api_rate_avail=api_rate_avail,
                result_map=result_map,
                fetch_failure_counter=fetch_failure_counter,
                analysis_failure_counter=analysis_failure_counter,
                log_queue=log_queue,
                risk_free_rate=risk_free_rate
            )
            s_process.start()
            s_processes.append(s_process)

        # run director thread
        d_thread = OptionDirectorThread(
            api_rate_cv=api_rate_cv,
            api_rate_avail=api_rate_avail,
            exit_flag=director_exit_flag,
            log_queue=log_queue
        )
        d_thread.start()

        # run progress bar
        self.__run_progress_bar(symbol_queue, log_queue, s_processes, 
            d_thread, director_exit_flag)

        # save results
        if self.save_scan:
            self.__save_scan(result_map._getvalue())

        return {
            'results': result_map._getvalue(),
            'fetch_failure_count': fetch_failure_counter.value,
            'analysis_failure_count': analysis_failure_counter.value
        }

    def __run_progress_bar(self, symbol_queue, log_queue, s_processes, 
        d_thread, director_exit_flag):

        size = symbol_queue.qsize()
        pbar = tqdm(total=size)

        # build log file
        if self.log_changes:
            Path('log').mkdir(exist_ok=True)
            f = open('log/{}.log'.format(self.scan_name), 'w+')

        # update prog bar
        while not symbol_queue.empty():
            if self.log_changes: self.__flush_logs(f, log_queue)
            new_size = symbol_queue.qsize()
            pbar.update(size - new_size)
            size = new_size
            time.sleep(0.1)
        new_size = symbol_queue.qsize()
        pbar.update(size - new_size)

        # wait for queue
        symbol_queue.join()
        pbar.close()

        # wait for threads
        for p in s_processes: p.join()
        director_exit_flag.update(True)
        d_thread.join()

        # flush logs
        if self.log_changes:
            self.__flush_logs(f, log_queue)
            f.close()

    def __save_scan(self, scan):
        vals = sum(scan.values(), [])

        # save file
        Path('scan').mkdir(exist_ok=True)
        f = open('scan/{}.csv'.format(self.scan_name), 'w+')
        csv.writer(f, delimiter=',').writerows(vals)
        f.close()

    def __flush_logs(self, f, log_queue):
        while not log_queue.empty():
            f.write(log_queue.get() + '\n')
        f.flush()
        os.fsync(f)


class OptionChainScannerProcess(multiprocessing.Process):

    def __init__(self, 
        process_num, 
        option_api,
        stock_api,
        dividend_api,
        symbol_queue, 
        api_rate_cv,
        api_rate_avail,
        result_map,
        fetch_failure_counter,
        analysis_failure_counter,
        risk_free_rate,
        log_queue,
        max_fetch_attempts=5,
        min_filtered_levels=5,
        option_price_floor=0.10,
        open_interest_floor=5,
        volume_floor=5,
        max_spread_width=20
    ):

        multiprocessing.Process.__init__(self)
        self.process_num = process_num
        self.option_api = option_api
        self.stock_api = stock_api
        self.dividend_api = dividend_api
        self.symbol_queue = symbol_queue
        self.api_rate_cv = api_rate_cv
        self.api_rate_avail = api_rate_avail
        self.result_map = result_map
        self.fetch_failure_counter = fetch_failure_counter
        self.analysis_failure_counter = analysis_failure_counter
        self.risk_free_rate = risk_free_rate
        self.log_queue=log_queue

        self.max_fetch_attempts = max_fetch_attempts
        self.min_filtered_levels = min_filtered_levels
        self.option_price_floor = option_price_floor
        self.open_interest_floor = open_interest_floor
        self.volume_floor = volume_floor
        self.max_spread_width = max_spread_width
        self.process_name = self.__class__.__name__ + str(self.process_num)

    def run(self):
        self.__log_message('INFO', 'starting scanner process')

        # iteratively execute tasks
        while True:
            try: symbol = self.symbol_queue.get(block=False)
            except Empty: break
            self.__execute_task(symbol)
            self.symbol_queue.task_done()

        self.__log_message('INFO', 'shutting down scanner process')

    def __execute_task(self, symbol):

        # fetch underlying
        if not self.__validate_symbol(symbol): return
        underlying = self.__fetch_underlying(symbol)
        if underlying is None:
            self.__report_fetch_failure('underlying', (symbol,))
            return

        # fetch dividend/expiration
        dividend = self.dividend_api.fetch_annual_yield(symbol)
        expirations = self.__fetch_expirations(symbol)
        if expirations is None:
            self.__report_fetch_failure('expirations', (symbol,))
            return

        # iterate/validate expirations
        for expiration in expirations:

            # fetch chains
            if not self.__validate_expiration(expiration): continue
            chain = self.__fetch_chain(symbol, expiration)
            if chain is None: 
                self.__report_fetch_failure('chain', (symbol, expiration))
                continue

            # run analysis
            try:
                self.result_map[(symbol, expiration)] = self.__analyze_chain(
                    symbol=symbol, 
                    underlying=underlying, 
                    dividend=dividend, 
                    expiration=expiration, 
                    chain=chain, 
                    risk_free_rate=self.risk_free_rate
                )
            except Exception as e:
                self.__report_analysis_failure((symbol, expiration), str(e))

        return True

    def __validate_symbol(self, symbol):

        # ignore toronto exchange
        if symbol.endswith('.TO'): return False
        else: return True

    def __validate_expiration(self, expiration):

        # get dte
        now_dt = date.today()
        exp_dt = datetime.strptime(expiration, '%Y-%m-%d').date()
        dte = (exp_dt - now_dt).days

        # target dte range
        if dte < 21 or dte > 91: return False
        else: return True
    
    def __fetch_expirations(self, symbol):
        expirations = None
        attempts = 0

        # retry api fetch
        while expirations is None:
            if attempts >= self.max_fetch_attempts: return None

            # acquire api call
            self.__wait_api_rate()
            fetch_results = self.option_api.fetch_expirations(symbol)
            attempts += 1

            # validate fetch results
            if fetch_results is not None:
                expirations, available, _, _ = fetch_results
                self.api_rate_avail.value = available

        return expirations

    def __fetch_chain(self, symbol, expiration):
        chain = None
        attempts = 0

        # retry api fetch
        while chain is None:
            if attempts >= self.max_fetch_attempts: return None

            # acquire api call
            self.__wait_api_rate()
            fetch_results = self.option_api.fetch_chain(symbol, expiration)
            attempts += 1

            # validate fetch results
            if fetch_results is not None:
                chain, available, _, _ = fetch_results
                self.api_rate_avail.value = available

        return chain

    def __fetch_underlying(self, symbol):
        underlying = None
        attempts = 0

        # retry api fetch
        while underlying is None:
            if attempts >= self.max_fetch_attempts: return None
            underlying = self.stock_api.fetch_last_quote(symbol)
            attempts += 1

        return underlying

    def __wait_api_rate(self):
        with self.api_rate_cv:
            self.api_rate_cv.wait()

    def __report_fetch_failure(self, component, fetch_data):
        self.fetch_failure_counter.value += 1
        self.__log_message('ERROR', '{} fetch failed for {}'.format(component, fetch_data))

    def __report_analysis_failure(self, analysis_data, error_msg):
        self.analysis_failure_counter.value += 1
        self.__log_message('ERROR', 'analysis failed for {} with error \"{}\"'.format(analysis_data, error_msg))

    def __log_message(self, tag, msg):
        log = str(datetime.today())
        log += ' ' + tag
        log += ' [' + self.process_name + ']'
        log += ': ' + msg
        self.log_queue.put(log)

    def __analyze_chain(self, 
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
        t = dte / 365.0
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


class OptionDirectorThread(threading.Thread):

    def __init__(self,
        api_rate_cv,
        api_rate_avail,
        exit_flag,
        log_queue
    ):

        threading.Thread.__init__(self)
        self.api_rate_cv = api_rate_cv
        self.api_rate_avail = api_rate_avail
        self.exit_flag = exit_flag
        self.log_queue = log_queue

        self.thread_name = self.__class__.__name__

    def run(self):
        self.__log_message('INFO', 'starting director thread')
        last_rate = 0

        while True:

            # check exit condition
            if self.exit_flag.get(): break
            
            # throttle api
            available_rate = self.api_rate_avail.value
            if available_rate > 100: current_rate = 500
            elif available_rate <= 30: current_rate = 30
            else: current_rate = 2 * available_rate

            # log rate change
            if last_rate != current_rate:
                tag = 'WARNING' if current_rate <= 100 else 'INFO'
                msg = 'api rate changed from {} to {}'.format(last_rate, current_rate)
                self.__log_message(tag, msg)
                last_rate = current_rate

            # notify processes
            time.sleep(60 / current_rate)
            self.__notify_api_rate()

        self.__log_message('INFO', 'shutting down director thread')

    def __notify_api_rate(self):
        with self.api_rate_cv:
            self.api_rate_cv.notify(n=1)

    def __log_message(self, tag, msg):
        log = str(datetime.today())
        log += ' ' + tag
        log += ' [' + self.thread_name + ']'
        log += ': ' + msg
        self.log_queue.put(log)