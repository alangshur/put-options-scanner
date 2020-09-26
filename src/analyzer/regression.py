from sklearn.linear_model import LinearRegression
from src.analyzer.base import AnalyzerBase
from src.api.polygon import PolygonAPI
from scipy.stats import pearsonr
import numpy as np


class RegressionAnalyzer(AnalyzerBase):

    def __init__(self,
        index='SPY',
        regression_range=30,
        volatility_period=30
    ):

        super().__init__('reg')
        self.index = index
        self.regression_range = regression_range
        self.volatility_period = volatility_period

        # fetch index
        self.index_quotes = PolygonAPI().fetch_year_quotes(self.index)
        if self.index_quotes is None: raise Exception('Failed to fetch index data.')
        self.index_rets = self.__convert_price_to_return(
            np.array([q['close'] for q in self.index_quotes])
        )

    def run(self, symbol, quotes):
        quotes = np.array([q['close'] for q in quotes])

        # calculate regression score
        ret, score = self.__regress_range(quotes, self.regression_range)

        # calculate correlation
        symbol_rets = self.__convert_price_to_return(quotes)
        corr_coef = pearsonr(symbol_rets, self.index_rets)[0]

        # calculate hvp
        hv_percentile = self.__calc_hv_percentile(symbol_rets)
        
        return (
            round(ret, 3), # period return over regression range
            round(score, 3), # regression score over regression range
            round(corr_coef, 3), # current annual market correlation
            round(hv_percentile, 3) # current historical volatility percentile
        )

    def validate(self, 
        symbol=None, 
        quotes=None
    ):

        # validate quotes length
        if quotes is not None: 
            return len(quotes) == len(self.index_quotes)
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

    def __calc_hv_percentile(self, symbol_rets):
        ret_history = symbol_rets.shape[0]
        volatilities = []

        # calculate vols over moving period
        for i in range(ret_history - self.volatility_period):
            window = symbol_rets[i:i + self.volatility_period]
            annualized_vol = np.std(window) * np.sqrt(ret_history)
            volatilities.append(annualized_vol)
        
        # calculate percentile
        return volatilities[-1]
        