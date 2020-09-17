from abc import abstractmethod


class AnalyzerBase:

    @abstractmethod
    def run(self):
        raise NotImplementedError

    @abstractmethod
    def validate(self):
        raise NotImplementedError


class EquityAnalyzerBase(AnalyzerBase):

    @abstractmethod
    def run(self, symbol, quotes):
        raise NotImplementedError

    @abstractmethod
    def validate(self, 
        symbol=None, 
        quotes=None
    ):
    
        raise NotImplementedError


class OptionAnalyzerBase(AnalyzerBase):

    @abstractmethod
    def run(self, 
        symbol, 
        underlying,
        expiration, 
        chain
    ):
    
        raise NotImplementedError

    @abstractmethod
    def validate(self, 
        symbol=None, 
        underlying=None,
        expiration=None, 
        chain=None
    ):
    
        raise NotImplementedError
