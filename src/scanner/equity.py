from src.api.polygon import PolygonAPI
from queue import Queue, Empty
import multiprocessing
from tqdm import tqdm
from time import sleep
import csv


class EquityScanner:

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
        api = PolygonAPI()
        manager = multiprocessing.Manager()
        queue = manager.Queue()
        result_map = manager.dict()
        failure_counter = manager.Value('i', 0)

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
                failure_counter=failure_counter
            )
            s_process.start()
            s_processes.append(s_process)

        # run progress bar
        self.__run_progress_bar(queue)

        # wait for processes
        for p in s_processes: p.join()

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
            sleep(0.1)
            new_size = queue.qsize()
            pbar.update(size - new_size)
            size = new_size
    
        # wait for queue
        queue.join()
        pbar.close()


class EquityScannerProcess(multiprocessing.Process):

    def __init__(self, 
        process_num, 
        analyzer,
        api,
        queue, 
        result_map,
        failure_counter,
        max_fetch_attempts=5
    ):

        multiprocessing.Process.__init__(self)
        self.process_num = process_num
        self.analyzer = analyzer
        self.api = api
        self.queue = queue
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

        # fetch and validate quotes
        quotes = self.__fetch_quote(symbol)
        if quotes is None: return False
        if not self.analyzer.validate(quotes=quotes):
            return True

        # run analyzer
        result = self.analyzer.run(symbol, quotes)
        self.result_map[symbol] = result

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