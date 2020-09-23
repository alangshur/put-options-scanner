from src.scanner.base import ScannerBase
from src.api.polygon import PolygonAPI
from queue import Queue, Empty
from pathlib import Path
import multiprocessing
from tqdm import tqdm
from time import sleep
import datetime
import csv


class EquityScanner(ScannerBase):

    def __init__(self, 
        analyzer,
        uni_list=None, 
        uni_file=None,
        num_processes=10,
        save_scan=True
    ):
    
        self.analyzer = analyzer
        self.uni_list = uni_list
        self.uni_file = uni_file
        self.num_processes = num_processes
        self.save_scan = save_scan
        
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
        k = 'equity'
        d = str(datetime.datetime.today()).split(' ')[0]
        t = str(datetime.datetime.today()).split(' ')[-1].split('.')[0]
        self.scan_name = '{}_{}_{}'.format(k, d, t)

    def run(self):

        # build resources
        api = PolygonAPI()
        manager = multiprocessing.Manager()
        queue = manager.Queue()
        result_map = manager.dict()
        fetch_failure_counter = manager.Value('i', 0)
        analyzer_failure_counter = manager.Value('i', 0)

        # load queue
        for symbol in self.uni:
            queue.put(symbol)

        # run processes
        s_processes = []
        for i in range(self.num_processes):
            s_process = EquityScannerProcess(
                process_num=i + 1, 
                analyzer=self.analyzer,
                api=api, 
                queue=queue, 
                result_map=result_map,
                fetch_failure_counter=fetch_failure_counter,
                analyzer_failure_counter=analyzer_failure_counter
            )
            s_process.start()
            s_processes.append(s_process)

        # run progress bar
        self.__run_progress_bar(queue)

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
    
    def __run_progress_bar(self, queue):
        size = queue.qsize()
        pbar = tqdm(total=size)

        # update prog bar
        while not queue.empty():
            sleep(0.1)
            new_size = queue.qsize()
            pbar.update(size - new_size)
            size = new_size
    
        # wait for queue
        queue.join()
        pbar.close()

    def __save_scan(self, scan):
        vals = [[k, v] for k, v in scan.items()]

        # save file
        Path('scan').mkdir(exist_ok=True)
        f = open('scan/{}.csv'.format(self.scan_name), 'w+')
        csv.writer(f, delimiter=',').writerows(vals)
        f.close()


class EquityScannerProcess(multiprocessing.Process):

    def __init__(self, 
        process_num, 
        analyzer,
        api,
        queue, 
        result_map,
        fetch_failure_counter,
        analyzer_failure_counter,
        max_fetch_attempts=5
    ):

        multiprocessing.Process.__init__(self)
        self.process_num = process_num
        self.analyzer = analyzer
        self.api = api
        self.queue = queue
        self.result_map = result_map
        self.fetch_failure_counter = fetch_failure_counter
        self.analyzer_failure_counter = analyzer_failure_counter
        self.max_fetch_attempts = max_fetch_attempts

    def run(self):
        while True:

            # start task
            try: symbol = self.queue.get(block=False)
            except Empty: break

            # execute task
            success = self.__execute_task(symbol)
            if not success: self.fetch_failure_counter.value += 1

            # complete task    
            self.queue.task_done()

    def __execute_task(self, symbol):

        # validate symbol
        if not self.analyzer.validate(symbol=symbol):
            return True

        # fetch and validate quotes
        quotes = self.__fetch_quote(symbol)
        if quotes is None: return False
        if not self.analyzer.validate(quotes=quotes):
            return True

        # run analyzer
        try:
            self.result_map[symbol] = self.analyzer.run(
                symbol=symbol, 
                quotes=quotes
            )
        except: 
            self.analyzer_failure_counter.value += 1

        return True

    def __fetch_quote(self, symbol):
        quotes = None
        attempts = 0

        # retry api fetch
        while quotes is None:
            if attempts >= self.max_fetch_attempts: return None
            quotes = self.api.fetch_year_quotes(symbol)
            attempts += 1

        return quotes