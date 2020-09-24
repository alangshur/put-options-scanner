from src.scanner.base import ScannerBase
from src.api.polygon import PolygonAPI
from queue import Queue, Empty
from pathlib import Path
import multiprocessing
from tqdm import tqdm
import datetime
import time
import csv
import os


class EquityScanner(ScannerBase):

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
        self.log_changes = log_changes
        
        # fetch universe
        if self.uni_file is not None:
            f = open(self.uni_file, 'r')
            uni_list = list(csv.reader(f))
            self.uni = [row[0] for row in uni_list[0:]]
            f.close()
        elif self.uni_list is not None:
            self.uni = self.uni_list
        else:
            raise Exception('No universe specified.')
            
        # build scan name
        k = 'equity'
        a = self.analyzer.get_name()
        d = str(datetime.datetime.today()).split(' ')[0]
        t = str(datetime.datetime.today()).split(' ')[-1].split('.')[0]
        self.scan_name = '{}_{}_{}_{}'.format(k, a, d, t)

    def run(self):

        # build resources
        api = PolygonAPI()
        manager = multiprocessing.Manager()
        symbol_queue = manager.Queue()
        result_map = manager.dict()

        # build meta resources
        fetch_failure_counter = manager.Value('i', 0)
        analyzer_failure_counter = manager.Value('i', 0)
        log_queue = manager.Queue()

        # load queue
        for symbol in self.uni:
            symbol_queue.put(symbol)

        # run processes
        s_processes = []
        for i in range(self.num_processes):
            s_process = EquityScannerProcess(
                process_num=i + 1, 
                analyzer=self.analyzer,
                api=api, 
                symbol_queue=symbol_queue, 
                result_map=result_map,
                fetch_failure_counter=fetch_failure_counter,
                analyzer_failure_counter=analyzer_failure_counter,
                log_queue=log_queue
            )
            s_process.start()
            s_processes.append(s_process)

        # run progress bar
        self.__run_progress_bar(symbol_queue, log_queue)

        # wait for processes
        for p in s_processes: p.join()

        # save results
        if self.save_scan:
            self.__save_scan(result_map._getvalue())

        # return results
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
        analyzer,
        api,
        symbol_queue, 
        result_map,
        fetch_failure_counter,
        analyzer_failure_counter,
        log_queue,
        max_fetch_attempts=5
    ):

        multiprocessing.Process.__init__(self)
        self.process_num = process_num
        self.analyzer = analyzer
        self.api = api
        self.symbol_queue = symbol_queue
        self.result_map = result_map
        self.fetch_failure_counter = fetch_failure_counter
        self.analyzer_failure_counter = analyzer_failure_counter
        self.log_queue = log_queue

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

        # fetch and validate quotes
        quotes = self.__fetch_quote(symbol)
        if quotes is None: 
            self.__report_fetch_failure('quotes', (symbol,))
            return
        if not self.analyzer.validate(quotes=quotes): return

        # run analyzer
        try:
            self.result_map[symbol] = self.analyzer.run(
                symbol=symbol, 
                quotes=quotes
            )
        except Exception as e:
            self.__report_analyzer_failure((symbol,), str(e))

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