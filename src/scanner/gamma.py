from py_vollib.black_scholes_merton.implied_volatility import implied_volatility
from py_vollib.black_scholes_merton.greeks import analytical
from sklearn.linear_model import LinearRegression
import numpy.polynomial.polynomial as poly
from src.scanner.base import ScannerBase
from src.api.atomicfinance import AtomicYFinanceAPI
from src.util.atomic import AtomicBool
from src.api.tradier import TradierAPI
from src.api.yfinance import YFinanceAPI
from src.api.ycharts import YChartsAPI
from datetime import datetime, date
from itertools import permutations
from scipy.stats import pearsonr
from queue import Queue, Empty
from scipy.stats import norm
from numpy import warnings
from pathlib import Path
import multiprocessing
from tqdm import tqdm
import numpy as np
import threading
import math
import time
import csv
import sys
import os


class GammaScanner(ScannerBase):

    def __init__(self, 
        uni_list=None, 
        uni_file=None,
        num_processes=6,
        save_scan=True,
        log_changes=True,
        manual_greeks=False,
        scan_name=None
    ):

        self.uni_list = uni_list
        self.uni_file = uni_file
        self.num_processes = num_processes
        self.save_scan = save_scan
        self.log_changes = log_changes
        self.manual_greeks = manual_greeks
        self.scan_name = scan_name

        # fetch universe
        if self.uni_file is not None:
            f = open(self.uni_file, 'r')
            uni_list = list(csv.reader(f))
            self.uni = [row[0] for row in uni_list]        
            f.close()
        elif self.uni_list is not None:
            self.uni = self.uni_list
        else:
            raise Exception('No universe specified.')

        # build scan name
        if self.scan_name is None:
            k = 'gamma'
            d = str(datetime.today()).split(' ')[0]
            t = str(datetime.today()).split(' ')[-1].split('.')[0]
            self.scan_name = '{}_{}_{}'.format(k, d, t)

    def run(self):

        # build resources
        option_api = TradierAPI()
        stock_api = YFinanceAPI()
        dividend_api = AtomicYFinanceAPI()
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
        if self.manual_greeks: 
            risk_free_rate = risk_free_rate_api.fetch_risk_free_rate()
        else: risk_free_rate = 0.0

        # load queue
        for symbol in self.uni:
            symbol_queue.put(symbol)

        # run scanner threads
        s_processes = []
        for i in range(self.num_processes):
            s_process = GammaScannerWorkerProcess(
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
                risk_free_rate=risk_free_rate,
                manual_greeks=self.manual_greeks
            )
            s_process.start()
            s_processes.append(s_process)

        # run director thread
        d_thread = GammaScannerDirectorThread(
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


class GammaScannerWorkerProcess(multiprocessing.Process):

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
        log_queue,
        risk_free_rate,
        manual_greeks,

        max_fetch_attempts=5,
        option_price_floor=0.10,
        open_interest_floor=5,
        volume_floor=5,
        index='SPY',
        regression_range=30,
        volatility_period=30,
        max_net_delta=0.03,
        min_contract_delta=0.45,
        min_relative_be_dist=0.80
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
        self.log_queue=log_queue
        self.risk_free_rate = risk_free_rate
        self.manual_greeks = manual_greeks

        self.max_fetch_attempts = max_fetch_attempts
        self.option_price_floor = option_price_floor
        self.open_interest_floor = open_interest_floor
        self.volume_floor = volume_floor
        self.index = index
        self.regression_range = regression_range
        self.volatility_period = volatility_period
        self.max_net_delta = max_net_delta
        self.min_contract_delta = min_contract_delta
        self.min_relative_be_dist = min_relative_be_dist
        self.process_name = self.__class__.__name__ + str(self.process_num)

    def run(self):
        self.__log_message('INFO', 'starting scanner process')
        self.complete_symbols = {}

        # iteratively execute tasks
        if self.__fetch_index():
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

        # fetch dividend
        if self.manual_greeks: 
            dividend = self.dividend_api.fetch_annual_yield(symbol)
        else: dividend = 0.0
        
        # fetch quotes
        quotes = self.__fetch_quotes(symbol)
        if quotes is None: 
            self.__report_fetch_failure('quotes', (symbol,))
            return

        # fetch expiration
        if not self.__validate_quotes(quotes): return
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

                # analyze options chain
                contracts, atm_iv = self.__analyze_chain(
                    symbol=symbol, 
                    underlying=underlying, 
                    dividend=dividend, 
                    expiration=expiration, 
                    chain=chain, 
                    risk_free_rate=self.risk_free_rate
                )

                # analyze underlying quotes
                if len(contracts) == 0: continue
                quotes_data = self.__analyze_quotes(
                    symbol=symbol,
                    quotes=quotes,
                    atm_iv=atm_iv
                )

                # compile analysis data
                contracts = [c + quotes_data for c in contracts]
                self.result_map[(symbol, expiration)] = contracts

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
        if dte <= 35 or dte >= 90: return False
        else: return True

    def __validate_quotes(self, quotes):
        return len(quotes) == len(self.index_quotes)

    def __fetch_index(self):

        # pull year quotes
        self.index_quotes = self.stock_api.fetch_year_quotes(self.index)
        if self.index_quotes is None:
            self.__report_fetch_failure('index', None)
            return False

        # convert prices to returns
        self.index_rets = self.__convert_price_to_return(
            np.array([q['close'] for q in self.index_quotes])
        )

        return True
    
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

    def __fetch_quotes(self, symbol):
        quotes = None
        attempts = 0

        # retry api fetch
        while quotes is None:
            if attempts >= self.max_fetch_attempts: return None
            quotes = self.stock_api.fetch_year_quotes(symbol) 
            attempts += 1
            
        return quotes

    def __wait_api_rate(self):
        with self.api_rate_cv:
            self.api_rate_cv.wait()

    def __report_fetch_failure(self, component, fetch_data):
        self.fetch_failure_counter.value += 1
        self.__log_message('ERROR', '{} fetch failed for {}'.format(
            component, fetch_data))

    def __report_analysis_failure(self, analysis_data, error_msg):
        line_no = sys.exc_info()[-1].tb_lineno
        self.analysis_failure_counter.value += 1
        self.__log_message('ERROR', 'analysis failed for {} with error \"{}\" at line {}'.format(
            analysis_data, error_msg, line_no))

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

        # load greeks & iv skew
        greeks_lookup = self.__load_greeks(underlying, dividend, chain, dte, risk_free_rate)
        atm_iv = self.__get_atm_iv(chain, greeks_lookup)

        # iterate over levels
        contract_collection = []
        for put_level, call_level in permutations(chain, 2):
            if put_level['option_type'] != 'put': continue
            if call_level['option_type'] != 'call': continue
            if put_level['strike'] > call_level['strike']: continue
            if put_level['strike'] / call_level['strike'] < 0.95: continue
            if put_level['strike'] > underlying: continue
            if call_level['strike'] < underlying: continue
            if put_level['description'] not in greeks_lookup: continue
            if call_level['description'] not in greeks_lookup: continue

            # enforce delta neutrality
            put_delta = greeks_lookup[put_level['description']]['delta']
            call_delta = greeks_lookup[call_level['description']]['delta']
            net_delta = put_delta + call_delta
            if np.abs(put_delta) < self.min_contract_delta: continue
            if np.abs(call_delta) < self.min_contract_delta: continue
            if net_delta > self.max_net_delta or net_delta < -self.max_net_delta:
                continue

            # calculate gamma-theta ratio
            put_theta = greeks_lookup[put_level['description']]['theta']
            put_gamma = greeks_lookup[put_level['description']]['gamma']
            call_theta = greeks_lookup[call_level['description']]['theta']
            call_gamma = greeks_lookup[call_level['description']]['gamma']
            net_theta = put_theta + call_theta
            net_gamma = put_gamma + call_gamma
            gamma_theta_ratio = net_gamma / net_theta 

            # calculate contract stats
            cost = put_level['ask'] + call_level['ask']
            width = call_level['strike'] - put_level['strike']
            put_be = put_level['strike'] - cost
            call_be = call_level['strike'] + cost
            relative_be_dist = 1 - (call_be - put_be) / underlying
            if relative_be_dist < self.min_relative_be_dist: continue

            # calculate movement
            std = underlying * atm_iv * np.sqrt(dte / 365)
            put_iv = greeks_lookup[put_level['description']]['iv']
            call_iv = greeks_lookup[call_level['description']]['iv']

            # save contract
            contract_collection.append([
                '{} {} +{}p/+{}c'.format(symbol, expiration, put_level['strike'], call_level['strike']), # description
                round(underlying, 2), # underlying price
                round(cost, 2), # upfront cost
                round(width, 5), # strike width
                round(put_be, 2), # break-even on put side
                round(call_be, 2), # break-even on call side
                round(relative_be_dist, 5), # relative break-even distance
                round(net_delta, 5), # net position delta
                round(gamma_theta_ratio, 5), # net gamma-theta ratio
                round(std, 5), # implied standard deviation
                round(put_iv, 5), # put contract implied volatility
                round(call_iv, 5), # call contract implied volatility
                round(atm_iv, 5), # ATM implied volatility
            ])

        return contract_collection, atm_iv

    def __analyze_quotes(self,
        symbol,
        quotes,
        atm_iv
    ):

        # calculate reg & corr & vol scores
        if symbol not in self.complete_symbols:
            quotes = np.array([q['close'] for q in quotes])
            ret, score = self.__regress_range(quotes, self.regression_range)
            symbol_rets = self.__convert_price_to_return(quotes)
            corr = pearsonr(symbol_rets, self.index_rets)[0]
            vols, curr_vol = self.__calc_vol_windows(symbol_rets)
            self.complete_symbols[symbol] = (quotes, ret, score, corr, vols, curr_vol)
        else:
            quotes, ret, score, corr, vols, curr_vol = self.complete_symbols[symbol]
            
        # calculate vol/be percentiles
        hv_percentile = (vols < curr_vol).sum() / vols.shape[0]
        iv_percentile = (vols < atm_iv).sum() / vols.shape[0]

        return [
            round(ret, 5), # period return over regression range
            round(score, 5), # regression score over regression range
            round(corr, 5), # current annual market correlation
            round(curr_vol, 5), # current historical volatility
            round(hv_percentile - iv_percentile, 5), # hvp-ivp diff
        ]

    def __regress_range(self, quotes, end_range, max_ret=1.0):
         
        # get variables
        if end_range >= quotes.shape[0]: y = quotes
        else: y = quotes[quotes.shape[0] - end_range:]
        x = np.arange(y.shape[0]).reshape(-1, 1)

        # get return
        ret = (y[-1] - y[0]) / y[0]
        if ret > max_ret: ret = max_ret
        if ret < -max_ret: ret = -max_ret

        # fit regression
        reg = LinearRegression()
        reg.fit(x, y).coef_[0]
        score = reg.score(x, y)

        return ret, score

    def __convert_price_to_return(self, quotes):
        last_quote = quotes[0]
        rets = []

        # iteratively convert p-to-r
        for quote in quotes[1:]:
            ret = (quote - last_quote) / last_quote
            rets.append(ret)
            last_quote = quote

        return np.array(rets)

    def __calc_vol_windows(self, symbol_rets):
        ret_history = symbol_rets.shape[0]
        vols = []

        # calculate vols over moving period
        for i in range(ret_history - self.volatility_period):
            window = symbol_rets[i:i + self.volatility_period]
            annualized_vol = np.std(window) * np.sqrt(ret_history)
            vols.append(annualized_vol)
        vols = np.array(vols) 
        
        return vols[:-1], vols[-1]
    
    def __load_greeks(self, underlying, dividend, chain, dte, risk_free_rate):
        greeks_lookup = {}

        # calculate greeks for chain
        for level in chain:
            try: greeks = self.__calculate_greeks(underlying, dividend, level, dte, risk_free_rate)
            except: continue

            # map contract description to greeks
            greeks_lookup[level['description']] = greeks

        return greeks_lookup

    def __calculate_greeks(self, underlying, dividend, level, dte, risk_free_rate):
        if not self.manual_greeks:
            return {
                'iv': level['greeks']['mid_iv'],
                'delta': level['greeks']['delta'],
                'theta': level['greeks']['theta'],
                'vega': level['greeks']['vega'],
                'gamma': level['greeks']['gamma'],
                'rho': level['greeks']['rho']
            }

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

        # map greeks
        return {
            'iv': sigma,
            'delta': analytical.delta(flag, S, K, t, r, sigma, q),
            'theta': analytical.theta(flag, S, K, t, r, sigma, q),
            'vega': analytical.vega(flag, S, K, t, r, sigma, q),
            'gamma': analytical.gamma(flag, S, K, t, r, sigma, q),
            'rho': analytical.rho(flag, S, K, t, r, sigma, q)
        }

    def __get_atm_iv(self, chain, greeks_lookup):
        delta_put_diff, delta_call_diff = float('inf'), float('inf')
        delta_put_level, delta_call_level = None, None 

        # iterate over raw chain
        for level in chain:
            if level['description'] not in greeks_lookup: 
                continue

            # find closest delta levels
            diff = np.abs(np.abs(greeks_lookup[level['description']]['delta']) - 0.50)
            if level['option_type'] == 'put':
                if diff < delta_put_diff: 
                    delta_put_diff = diff
                    delta_put_level = level
            elif level['option_type'] == 'call':
                if diff < delta_call_diff: 
                    delta_call_diff = diff
                    delta_call_level = level

        # calculate atm iv
        atm_iv = (greeks_lookup[delta_put_level['description']]['iv'] + \
            greeks_lookup[delta_call_level['description']]['iv']) / 2

        return atm_iv

    def __filter_level(self, symbol, underlying, level):
        if level['option_type'] != 'put': return False # ignore call options
        if underlying <= level['strike']: return False # ignore ATM/ITM options
        if level['root_symbol'] != symbol: return False # ignore adjusted options
        if level['last'] is None or level['last'] <= self.option_price_floor: return False # ignore low last price
        if level['bid'] is None or level['bid'] <= self.option_price_floor: return False # ignore low bid price
        if level['ask'] is None or level['ask'] <= self.option_price_floor: return False # ignore low ask price
        if level['open_interest'] <= self.open_interest_floor: return False # ignore low open interest
        if level['volume'] <= self.volume_floor: return False # ignore low volume
        return True


class GammaScannerDirectorThread(threading.Thread):

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