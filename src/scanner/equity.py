from src.util.atomic import AtomicInteger, AtomicNestedMap
from src.api.polygon import PolygonAPI
from queue import Queue, Empty
from tqdm import tqdm
import threading
import csv
import math
import time


class EquityScanner:

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
        api = PolygonAPI()
        queue = Queue()
        failure_counter = AtomicInteger()
        result_map = AtomicNestedMap()

        # load queue
        for symbol in self.uni:
            queue.put(symbol)

        # run regression threads
        s_threads = []
        for i in range(self.num_threads):
            s_thread = EquityScannerThread(
                thread_num=i + 1, 
                queue=queue, 
                api=api, 
                analyzer=self.analyzer,
                result_map=result_map,
                failure_counter=failure_counter
            )
            s_thread.start()
            s_threads.append(s_thread)

        # run progress bar
        self.__run_progress_bar(queue)

        # wait for threads
        for t in s_threads: t.join()

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


class EquityScannerThread(threading.Thread):

    def __init__(self, 
        thread_num, 
        queue, 
        api,
        analyzer,
        result_map,
        failure_counter,
        max_fetch_attempts=5
    ):

        threading.Thread.__init__(self)

        self.id = thread_num
        self.queue = queue
        self.api = api
        self.analyzer = analyzer
        self.result_map = result_map
        self.failure_counter = failure_counter
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

            # fetch quotes
            quotes = self.__fetch_quote(symbol)
            if quotes is None: self.failure_counter.increment()
            else:

                # validate quotes
                if not self.analyzer.validate(quotes=quotes):
                    self.queue.task_done()
                    continue

                # run analyzer
                name = self.analyzer.get_name()
                result = self.analyzer.run(symbol, quotes)
                self.result_map.update(
                    key1=name, 
                    key2=symbol, 
                    value=result
                )

            # complete task    
            self.queue.task_done()

    def __fetch_quote(self, symbol):
        quotes = None
        attempts = 0

        # retry api fetch
        while quotes is None:
            if attempts >= self.max_fetch_attempts: return None
            quotes = self.api.fetch_quotes_year(symbol)
            attempts += 1

        return quotes