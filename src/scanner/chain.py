from src.scanner.base import ScannerBase
from src.util.atomic import AtomicBool
from src.api.tradier import TradierAPI
from src.api.polygon import PolygonAPI
from src.api.yfinance import YFinanceAPI
from src.api.ycharts import YChartsAPI
from queue import Queue, Empty
from pathlib import Path
import multiprocessing
from tqdm import tqdm
import threading
import datetime
import math
import time
import csv
import os


class OptionChainScanner(ScannerBase):

    def __init__(self, 
        analyzer,
        uni_list=None, 
        uni_file=None,
        num_processes=6,
        save_scan=True,
        log_changes=True
    ):

        self.analyzer = analyzer
        self.uni_list = uni_list
        self.uni_file = uni_file
        self.num_processes = num_processes
        self.save_scan = save_scan
        self.log_changes= log_changes

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
        k = 'option'
        a = self.analyzer.get_name()
        d = str(datetime.datetime.today()).split(' ')[0]
        t = str(datetime.datetime.today()).split(' ')[-1].split('.')[0]
        self.scan_name = '{}_{}_{}_{}'.format(k, a, d, t)

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
        analyzer_failure_counter = manager.Value('i', 0)
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
                analyzer=self.analyzer,
                option_api=option_api, 
                stock_api=stock_api,
                dividend_api=dividend_api,
                symbol_queue=symbol_queue, 
                api_rate_cv=api_rate_cv,
                api_rate_avail=api_rate_avail,
                result_map=result_map,
                fetch_failure_counter=fetch_failure_counter,
                analyzer_failure_counter=analyzer_failure_counter,
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
        self.__run_progress_bar(symbol_queue, log_queue)

        # wait for threads
        for p in s_processes: p.join()
        director_exit_flag.update(True)
        d_thread.join()

        # save results
        if self.save_scan:
            self.__save_scan(result_map._getvalue())

        return {
            'results': result_map._getvalue(),
            'fetch_failure_count': fetch_failure_counter.value,
            'analyzer_failure_count': analyzer_failure_counter.value
        }

    def __run_progress_bar(self, symbol_queue, log_queue):
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
        analyzer,
        option_api,
        stock_api,
        dividend_api,
        symbol_queue, 
        api_rate_cv,
        api_rate_avail,
        result_map,
        fetch_failure_counter,
        analyzer_failure_counter,
        risk_free_rate,
        log_queue,
        max_fetch_attempts=5
    ):

        multiprocessing.Process.__init__(self)
        self.process_num = process_num
        self.analyzer = analyzer
        self.option_api = option_api
        self.stock_api = stock_api
        self.dividend_api = dividend_api
        self.symbol_queue = symbol_queue
        self.api_rate_cv = api_rate_cv
        self.api_rate_avail = api_rate_avail
        self.result_map = result_map
        self.fetch_failure_counter = fetch_failure_counter
        self.analyzer_failure_counter = analyzer_failure_counter
        self.risk_free_rate = risk_free_rate
        self.log_queue=log_queue

        self.max_fetch_attempts = max_fetch_attempts
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

        # validate symbol
        if not self.analyzer.validate(symbol=symbol): return

        # fetch/validate underlying
        underlying = self.__fetch_underlying(symbol)
        if underlying is None:
            self.__report_fetch_failure('underlying', (symbol,))
            return
        if not self.analyzer.validate(underlying=underlying): return

        # fetch/validate dividend yield
        dividend = self.dividend_api.fetch_annual_yield(symbol)
        if not self.analyzer.validate(dividend=dividend): return
        
        # fetch expirations
        expirations = self.__fetch_expirations(symbol)
        if expirations is None:
            self.__report_fetch_failure('expirations', (symbol,))
            return

        # iterate/validate expirations
        for expiration in expirations:
            if not self.analyzer.validate(expiration=expiration): continue
            
            # fetch/validate chains
            chain = self.__fetch_chain(symbol, expiration)
            if chain is None: 
                self.__report_fetch_failure('chain', (symbol, expiration))
                continue
            if not self.analyzer.validate(chain=chain): continue

            # run analyzer
            try:
                self.result_map[(symbol, expiration)] = self.analyzer.run(
                    symbol=symbol, 
                    underlying=underlying, 
                    dividend=dividend, 
                    expiration=expiration, 
                    chain=chain, 
                    risk_free_rate=self.risk_free_rate
                )
            except Exception as e:
                self.__report_analyzer_failure((symbol, expiration), str(e))

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

    def __wait_api_rate(self):
        with self.api_rate_cv:
            self.api_rate_cv.wait()

    def __report_fetch_failure(self, component, fetch_data):
        self.fetch_failure_counter.value += 1
        self.__log_message('ERROR', '{} fetch failed for {}'.format(component, fetch_data))

    def __report_analyzer_failure(self, analyzer_data, error_msg):
        self.analyzer_failure_counter.value += 1
        self.__log_message('ERROR', 'analyzer failed for {} with error \"{}\"'.format(analyzer_data, error_msg))

    def __log_message(self, tag, msg):
        log = str(datetime.datetime.today())
        log += ' ' + tag
        log += ' [' + self.process_name + ']'
        log += ': ' + msg
        self.log_queue.put(log)


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
        log = str(datetime.datetime.today())
        log += ' ' + tag
        log += ' [' + self.thread_name + ']'
        log += ': ' + msg
        self.log_queue.put(log)