from src.util.atomic import AtomicInteger, AtomicNestedMap, AtomicBool
from src.api.tradier import TradierAPI
from queue import Queue, Empty
from tqdm import tqdm
import threading
import math
import csv
import time


class OptionScanner:

    def __init__(self, 
        uni_file, 
        analyzer,
        num_threads=10
    ):

        self.uni_file = uni_file
        self.analyzer = analyzer
        self.num_threads = num_threads

        # fetch universe
        f = open(self.uni_file, 'r')
        uni_list = list(csv.reader(f))
        self.uni = [row[0] for row in uni_list[1:]]
        f.close()

    def run(self):

        # build resources
        api = TradierAPI()
        queue = Queue()
        failure_counter = AtomicInteger()
        result_map = AtomicNestedMap()
        api_rate_cv = threading.Condition()
        api_rate_expiry = AtomicInteger()
        api_rate_available = AtomicInteger()
        director_exit_flag = AtomicBool(value=False)

        # load queue
        for symbol in self.uni:
            queue.put(symbol)

        # run scanner threads
        s_threads = []
        for i in range(self.num_threads):
            s_thread = OptionScannerThread(
                thread_num=i + 1, 
                queue=queue, 
                api=api, 
                analyzer=self.analyzer,
                result_map=result_map,
                failure_counter=failure_counter,
                api_rate_cv=api_rate_cv,
                api_rate_expiry=api_rate_expiry,
                api_rate_available=api_rate_available
            )
            s_thread.start()
            s_threads.append(s_thread)

        # run director thread
        d_thread = OptionDirectorThread(
            num_threads=self.num_threads,
            exit_flag=director_exit_flag,
            api=api,
            api_rate_cv=api_rate_cv,
            api_rate_expiry=api_rate_expiry,
            api_rate_available=api_rate_available
        )
        d_thread.start()

        # run progress bar
        self.__run_progress_bar(queue)

        # wait for threads
        for t in s_threads: t.join()
        director_exit_flag.update(True)
        d_thread.join()

        # return results
        return {
            'results': result_map.get(),
            'failures': failure_counter.get()
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


class OptionScannerThread(threading.Thread):

    def __init__(self, 
        thread_num, 
        queue, 
        api,
        analyzer,
        result_map,
        failure_counter,
        api_rate_cv,
        api_rate_expiry,
        api_rate_available,
        max_fetch_attempts=5
    ):

        threading.Thread.__init__(self)

        self.id = thread_num
        self.queue = queue
        self.api = api
        self.analyzer = analyzer
        self.result_map = result_map
        self.failure_counter = failure_counter
        self.api_rate_cv = api_rate_cv
        self.api_rate_expiry = api_rate_expiry
        self.api_rate_available = api_rate_available
        self.max_fetch_attempts = max_fetch_attempts

    def run(self):
        while True:

            # start task
            try: symbol = self.queue.get(block=False)
            except Empty: return

            # validate symbol
            if not self.analyzer.validate(symbol=symbol):
                self.queue.task_done()
                continue

            # fetch expirations
            expirations = self.__fetch_expirations(symbol)
            if expirations is None: self.failure_counter.increment()
            else:
                for expiration in expirations:

                    # validate expiration
                    if not self.analyzer.validate(expiration=expiration):
                        continue
                    
                    # fetch chains
                    chain = self.__fetch_chain(symbol, expiration)
                    if chain is None: self.failure_counter()
                    else:

                        # validate chain
                        if not self.analyzer.validate(chain=chain):
                            continue

                        # run analyzer
                        name = self.analyzer.get_name()
                        result = self.analyzer.run(symbol, expiration, chain)
                        self.result_map.update(
                            key1=name,
                            key2=(symbol, expiration),
                            value=result
                        )

            # complete task    
            self.queue.task_done()

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
                expirations, available, _, expiry = fetch_results
                self.api_rate_expiry.update(expiry)
                self.api_rate_available.update(available)

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
                chain, available, _, expiry = fetch_results
                self.api_rate_expiry.update(expiry)
                self.api_rate_available.update(available)

        return chain

    def __wait_api_rate(self):
        with self.api_rate_cv:
            self.api_rate_cv.wait()
    

class OptionDirectorThread(threading.Thread):

    def __init__(self,
        num_threads,
        exit_flag,
        api,
        api_rate_cv,
        api_rate_expiry,
        api_rate_available,
        api_rate_buffer=10,
        api_base_rate=0.5
    ):

        threading.Thread.__init__(self)

        self.num_threads = num_threads
        self.exit_flag = exit_flag
        self.api = api
        self.api_rate_cv = api_rate_cv
        self.api_rate_expiry = api_rate_expiry
        self.api_rate_available = api_rate_available
        self.api_rate_buffer = api_rate_buffer
        self.api_base_rate = api_base_rate

    def run(self):
        while True:

            # check exit condition
            if self.exit_flag.get(): return
                
            # get api rate data
            available = self.api_rate_available.get() - self.api_rate_buffer
            expiry = self.api_rate_expiry.get()

            # throttle api
            if available > 0 and expiry > 0:
                now = int(time.time() * 1000)
                new_rate = (expiry - now) / available

                # set new rate
                if new_rate < 0: 
                    self.__notify_api_rate()
                else:
                    time.sleep(new_rate / 1000)
                    self.__notify_api_rate()

            # warmup api
            else:
                time.sleep(self.api_base_rate)
                self.__notify_api_rate()

    def __notify_api_rate(self):
        with self.api_rate_cv:
            self.api_rate_cv.notify(n=1)