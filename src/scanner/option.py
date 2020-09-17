from src.util.atomic import AtomicBool
from src.api.tradier import TradierAPI
from src.api.polygon import PolygonAPI
from queue import Queue, Empty
import multiprocessing
from tqdm import tqdm
import threading
import math
import csv
import time


class OptionScanner:

    def __init__(self, 
        uni_file, 
        analyzer,
        num_processes=10
    ):

        self.uni_file = uni_file
        self.analyzer = analyzer
        self.num_processes = num_processes

        # fetch universe
        f = open(self.uni_file, 'r')
        uni_list = list(csv.reader(f))
        self.uni = [row[0] for row in uni_list[1:]]        
        f.close()

    def run(self):

        # build resources
        api = TradierAPI()
        secondary_api = PolygonAPI()
        manager = multiprocessing.Manager()
        queue = manager.Queue()
        api_rate_cv = manager.Condition()
        api_rate_avail = manager.Value('i', 200)
        result_map = manager.dict()
        failure_counter = manager.Value('i', 0)
        director_exit_flag = AtomicBool(value=False)

        # load queue
        for symbol in self.uni:
            queue.put(symbol)

        # run scanner threads
        s_processes = []
        for i in range(self.num_processes):
            s_process = OptionScannerProcess(
                thread_num=i + 1, 
                analyzer=self.analyzer,
                api=api, 
                secondary_api=secondary_api,
                queue=queue, 
                api_rate_cv=api_rate_cv,
                api_rate_avail=api_rate_avail,
                result_map=result_map,
                failure_counter=failure_counter
            )
            s_process.start()
            s_processes.append(s_process)

        # run director thread
        d_thread = OptionDirectorThread(
            api=api,
            api_rate_cv=api_rate_cv,
            api_rate_avail=api_rate_avail,
            exit_flag=director_exit_flag
        )
        d_thread.start()

        # run progress bar
        self.__run_progress_bar(queue)

        # wait for threads
        for p in s_processes: p.join()
        director_exit_flag.update(True)
        d_thread.join()

        # return results
        return {
            'results': result_map._getvalue(),
            'failures': failure_counter.value
        }

    def __run_progress_bar(self, queue):
        size = queue.qsize()
        pbar = tqdm(total=size)

        # update prog bar
        while not queue.empty():
            time.sleep(0.1)
            new_size = queue.qsize()
            pbar.update(size - new_size)
            size = new_size

        # wait for queue
        queue.join()
        pbar.close()


class OptionScannerProcess(multiprocessing.Process):

    def __init__(self, 
        thread_num, 
        analyzer,
        api,
        secondary_api,
        queue, 
        api_rate_cv,
        api_rate_avail,
        result_map,
        failure_counter,
        max_fetch_attempts=5
    ):

        multiprocessing.Process.__init__(self)
        self.thread_num = thread_num
        self.analyzer = analyzer
        self.api = api
        self.secondary_api = secondary_api
        self.queue = queue
        self.api_rate_cv = api_rate_cv
        self.api_rate_avail = api_rate_avail
        self.result_map = result_map
        self.failure_counter = failure_counter
        self.max_fetch_attempts = max_fetch_attempts

    def run(self):
        while True:

            # start task
            try: symbol = self.queue.get(block=False)
            except Empty: break

            # execute task
            success = self.__execute_task(symbol)
            if not success: self.failure_counter.value += 1

            # complete task    
            self.queue.task_done()
        
    def __execute_task(self, symbol):

        # validate symbol
        if not self.analyzer.validate(symbol=symbol):
            return True

        # fetch and validate underlying
        underlying = self.__fetch_underlying(symbol)
        if underlying is None: return False
        if not self.analyzer.validate(underlying=underlying):
            return True

        # fetch and validate expirations
        expirations = self.__fetch_expirations(symbol)
        if expirations is None: return False
        for expiration in expirations:
            if not self.analyzer.validate(expiration=expiration):
                continue
            
            # fetch and validate chains
            chain = self.__fetch_chain(symbol, expiration)
            if chain is None: continue
            if not self.analyzer.validate(chain=chain):
                continue

            # run analyzer
            result = self.analyzer.run(symbol, underlying, expiration, chain)
            self.result_map[(symbol, expiration)] = result

        return True

    def __fetch_expirations(self, symbol):
        expirations = None
        attempts = 0

        # retry api fetch
        while expirations is None:
            if attempts >= self.max_fetch_attempts: return None

            # acquire api call
            self.__wait_api_rate()
            fetch_results = self.api.fetch_expirations(symbol)
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
            fetch_results = self.api.fetch_chain(symbol, expiration)
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
            underlying = self.secondary_api.fetch_last_quote(symbol)
            attempts += 1

        return underlying

    def __wait_api_rate(self):
        with self.api_rate_cv:
            self.api_rate_cv.wait()
    

class OptionDirectorThread(threading.Thread):

    def __init__(self,
        api,
        api_rate_cv,
        api_rate_avail,
        exit_flag
    ):

        threading.Thread.__init__(self)
        self.api = api
        self.api_rate_cv = api_rate_cv
        self.api_rate_avail = api_rate_avail
        self.exit_flag = exit_flag

    def run(self):
        while True:

            # check exit condition
            if self.exit_flag.get(): break
            
            # throttle api
            available_rate = self.api_rate_avail.value
            if available_rate > 100: current_rate = 500
            elif available_rate <= 30: current_rate = 30
            else: current_rate = 2 * available_rate

            # notify processes
            time.sleep(60 / current_rate)
            self.__notify_api_rate()

    def __notify_api_rate(self):
        with self.api_rate_cv:
            self.api_rate_cv.notify(n=1)