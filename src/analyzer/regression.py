from sklearn.linear_model import LinearRegression
from src.analyzer.base import EquityAnalyzerBase
import numpy as np


class RegressionAnalyzer(EquityAnalyzerBase):

    def __init__(self):
        super().__init__()

        # define day ranges
        self.ranges = [15, 45]

    def run(self, symbol, quotes):
        quotes = np.array([q['close'] for q in quotes])
        range_data = []

        # calculate regression scores
        for r in self.ranges:
            ret, score = self.__regress_range(quotes, r)
            value = score ** (1 - ret)
            range_data.append(value)

        # verify ranges
        range_data = np.array(range_data)
        return range_data.mean()

    def validate(self, 
        symbol=None, 
        quotes=None
    ):

        # validate quotes length
        if quotes is not None: 
            return len(quotes) >= max(self.ranges)
        else: 
            return True

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