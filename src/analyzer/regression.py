from sklearn.linear_model import LinearRegression
from src.analyzer.base import EquityAnalyzerBase
from src.api.polygon import PolygonAPI
from scipy.stats import pearsonr
import numpy as np


class RegressionAnalyzer(EquityAnalyzerBase):

    def __init__(self, 
        index='SPY'
    ):

        super().__init__()
        self.range = 30

        # fetch index
        index_quotes = PolygonAPI().fetch_year_quotes(index)
        if index_quotes is None: raise Exception('Failed to fetch index data.')
        index_quotes = np.array([q['close'] for q in index_quotes])
        self.index_rets = self.__convert_price_to_return(index_quotes)

    def run(self, symbol, quotes):
        quotes = np.array([q['close'] for q in quotes])

        # calculate regression score
        ret, score = self.__regress_range(quotes, self.range)
        if ret < 0.0: return 0.0

        # calculate correlation
        symbol_rets = self.__convert_price_to_return(quotes)
        min_history = min(symbol_rets.shape[0], self.index_rets.shape[0])
        corr_coef = pearsonr(symbol_rets[-min_history:], self.index_rets[-min_history:])[0]
        
        # verify ranges
        range_data = np.array(range_data)
        return (round(score, 3), round(corr_coef, 3))

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

    def __convert_price_to_return(self, quotes):
        last_quote = quotes[0]
        rets = []

        # iteratively convert p-to-r
        for quote in quotes[1:]:
            ret = (quote - last_quote) / last_quote
            rets.append(ret)
            last_quote = quote

        return np.array(rets)
