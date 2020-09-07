from src.api.polygon import PolygonAPI
from src.util.atomic import AtomicInteger, AtomicList
from sklearn.linear_model import LinearRegression
from queue import Queue, Empty
import numpy as np
import threading
import csv
import math
import pprint


class ScannerThread(threading.Thread):

    def __init__(self, 
        thread_num, 
        queue, 
        api,
        result_list,
        failure_counter
    ):

        threading.Thread.__init__(self)
        self.id = thread_num
        self.queue = queue
        self.api = api
        self.result_list = result_list
        self.failure_counter = failure_counter

    def run(self):
        while True:

            # start task
            try: symbol = self.queue.get(block=False)
            except Empty: return

            # fetch quotes
            quotes = self.__fetch_quote(symbol)

            # build regression score
            if quotes is None: self.failure_counter.increment()
            else: 
                score = self.__build_regression_score(quotes)
                self.result_list.append({
                    'symbol': symbol,
                    'slope': score[0],
                    'r2': score[1]
                })

            # complete task    
            self.queue.task_done()

    def __build_regression_score(self, quotes):

        # fit regression ranges
        quotes = np.array([q['close'] for q in quotes])
        year_reg = self.__regress_range(quotes, 253)
        half_year_reg = self.__regress_range(quotes, 126)
        three_month_reg = self.__regress_range(quotes, 63)
        month_reg = self.__regress_range(quotes, 21)
        week_reg = self.__regress_range(quotes, 5)

        # calculate score
        return three_month_reg

    def __fetch_quote(self, symbol, max_attempts=5):
        quotes = None
        attempts = 0

        # retry api fetch
        while quotes is None:
            if attempts >= max_attempts: return None
            quotes = self.api.fetch_quotes_year(symbol)
            attempts += 1

        return quotes

    def __regress_range(self, quotes, end_range):
         
        # get variables
        if end_range >= quotes.shape[0]: y = quotes
        else: y = quotes[quotes.shape[0] - end_range:]
        x = np.arange(y.shape[0]).reshape(-1, 1)

        # fit regression
        reg = LinearRegression()
        slope = reg.fit(x, y).coef_[0]
        score = reg.score(x, y)

        return slope, score


class StockScanner:

    def __init__(self, 
        uni_file, 
        num_threads=10
    ):
    
        self.uni_file = uni_file
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
        result_list = AtomicList()

        # load queue
        for symbol in self.uni:
            queue.put(symbol)

        # run scanner threads
        for i in range(self.num_threads):
            ScannerThread(
                i + 1, 
                queue, 
                api, 
                result_list,
                failure_counter
            ).start()
        queue.join()

        # sort results
        results = result_list.get()
        results = sorted(
            results, 
            key=lambda item: item['slope'],
            reverse=True
        )

        pprint.pprint(results)

        # top_symbols = [item['symbol'] for item in results]
        # print(top_symbols)






