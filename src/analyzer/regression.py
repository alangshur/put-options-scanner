from sklearn.linear_model import LinearRegression
from src.analyzer.base import EquityAnalyzerBase
import numpy as np


class RegressionAnalyzer(EquityAnalyzerBase):

    def __init__(self):
        super().__init__()

        # define day ranges
        self.YEAR_DAYS = 250
        self.HALF_YEAR_DAYS = 126
        self.THREE_MONTH_DAYS = 63
        self.MONTH_DAYS = 21
        self.WEEK_DAYS = 5

    def run(self, symbol, quotes):

        # fit regression ranges
        quotes = np.array([q['close'] for q in quotes])
        year_reg = self.__regress_range(quotes, self.YEAR_DAYS)
        half_year_reg = self.__regress_range(quotes, self.HALF_YEAR_DAYS)
        three_month_reg = self.__regress_range(quotes, self.THREE_MONTH_DAYS)
        month_reg = self.__regress_range(quotes, self.MONTH_DAYS)
        week_reg = self.__regress_range(quotes, self.WEEK_DAYS)

        # calculate score
        return {
            'week': week_reg,
            'month': month_reg,
            'three_month': three_month_reg,
            'half_year': half_year_reg,
            'year': year_reg
        }

    def validate(self, 
        symbol=None, 
        quotes=None
    ):

        # validate quotes length
        if quotes is not None: 
            return len(quotes) >= self.YEAR_DAYS
        else: 
            return True

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