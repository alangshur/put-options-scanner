from sklearn.linear_model import LinearRegression
from src.scanner.base import ScannerBase
from src.api.yfinance import YFinanceAPI
from scipy.stats import pearsonr
from queue import Queue, Empty
from pathlib import Path
import multiprocessing
from tqdm import tqdm
import numpy as np
import datetime
import time
import csv
import sys
import os


class EquityHistoryScanner(ScannerBase):

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
            self.uni = [row[0] for row in uni_list]
            f.close()
        elif self.uni_list is not None:
            self.uni = self.uni_list
        else:
            raise Exception('No universe specified.')
            
        # build scan name
        k = 'reg'
        d = str(datetime.datetime.today()).split(' ')[0]
        t = str(datetime.datetime.today()).split(' ')[-1].split('.')[0]
        self.scan_name = '{}_{}_{}'.format(k, d, t)

    def run(self):

        # build resources
        api = YFinanceAPI()
        manager = multiprocessing.Manager()
        symbol_queue = manager.Queue()
        result_map = manager.dict()

        # build meta resources
        fetch_failure_counter = manager.Value('i', 0)
        analysis_failure_counter = manager.Value('i', 0)
        log_queue = manager.Queue()

        # load queue
        for symbol in self.uni:
            symbol_queue.put(symbol)

        # run processes
        s_processes = []
        for i in range(self.num_processes):
            s_process = EquityScannerProcess(
                process_num=i + 1, 
                api=api, 
                symbol_queue=symbol_queue, 
                result_map=result_map,
                fetch_failure_counter=fetch_failure_counter,
                analysis_failure_counter=analysis_failure_counter,
                log_queue=log_queue
            )
            s_process.start()
            s_processes.append(s_process)

        # run progress bar
        self.__run_progress_bar(symbol_queue, log_queue, s_processes)

        # save results
        if self.save_scan:
            self.__save_scan(result_map._getvalue())

        return {
            'results': result_map._getvalue(),
            'fetch_failure_count': fetch_failure_counter.value,
            'analysis_failure_count': analysis_failure_counter.value
        }
    
    def __run_progress_bar(self, symbol_queue, log_queue, s_processes):
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

        # wait for processes
        for p in s_processes: p.join()

        # flush logs
        if self.log_changes:
            self.__flush_logs(f, log_queue)
            f.close()

    def __save_scan(self, scan):
        vals = [[k, *v] for k, v in scan.items()]

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


class EquityScannerProcess(multiprocessing.Process):

    def __init__(self, 
        process_num, 
        api,
        symbol_queue, 
        result_map,
        fetch_failure_counter,
        analysis_failure_counter,
        log_queue,
        max_fetch_attempts=5,
        index='SPY',
        regression_range=30,
        volatility_period=30
    ):

        multiprocessing.Process.__init__(self)
        self.process_num = process_num
        self.api = api
        self.symbol_queue = symbol_queue
        self.result_map = result_map
        self.fetch_failure_counter = fetch_failure_counter
        self.analysis_failure_counter = analysis_failure_counter
        self.log_queue = log_queue

        self.max_fetch_attempts = max_fetch_attempts
        self.index = index
        self.regression_range = regression_range
        self.volatility_period = volatility_period
        self.process_name = self.__class__.__name__ + str(self.process_num)

        # fetch index
        self.index_quotes = YFinanceAPI().fetch_year_quotes(self.index)
        if self.index_quotes is None: raise Exception('Failed to fetch index data.')
        self.index_rets = self.__convert_price_to_return(
            np.array([q['close'] for q in self.index_quotes])
        )

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

        # fetch quotes
        quotes = self.__fetch_quote(symbol)
        if quotes is None: 
            self.__report_fetch_failure('quotes', (symbol,))
            return
        
        # validate quotes
        if not self.__validate_quotes(quotes): 
            return

        # run analysis
        try:
            self.result_map[symbol] = self.__analyze_quotes(
                symbol=symbol,
                quotes=quotes
            )
        except Exception as e:
            self.__report_analysis_failure((symbol,), str(e))

    def __validate_quotes(self, quotes):
        return len(quotes) == len(self.index_quotes)

    def __fetch_quote(self, symbol):
        quotes = None
        attempts = 0

        # retry api fetch
        while quotes is None:
            if attempts >= self.max_fetch_attempts: return None
            quotes = self.api.fetch_year_quotes(symbol) 
            attempts += 1
            
        return quotes

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
        log = str(datetime.datetime.today())
        log += ' ' + tag
        log += ' [' + self.process_name + ']'
        log += ': ' + msg
        self.log_queue.put(log)

    def __analyze_quotes(self, symbol, quotes):
        quotes = np.array([q['close'] for q in quotes])

        # calculate regression score
        ret, score = self.__regress_range(quotes, self.regression_range)

        # calculate correlation
        symbol_rets = self.__convert_price_to_return(quotes)
        corr_coef = pearsonr(symbol_rets, self.index_rets)[0]

        # calculate hvp
        hv_percentile = self.__calc_hv_percentile(symbol_rets)
        
        return (
            round(ret, 3), # period return over regression range
            round(score, 3), # regression score over regression range
            round(corr_coef, 3), # current annual market correlation
            round(hv_percentile, 3) # current historical volatility percentile
        )

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

    def __calc_hv_percentile(self, symbol_rets):
        ret_history = symbol_rets.shape[0]
        volatilities = []

        # calculate vols over moving period
        for i in range(ret_history - self.volatility_period):
            window = symbol_rets[i:i + self.volatility_period]
            annualized_vol = np.std(window) * np.sqrt(ret_history)
            volatilities.append(annualized_vol)
        
        # calculate percentile
        return volatilities[-1]