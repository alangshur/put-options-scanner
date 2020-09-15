from abc import abstractmethod


class AnalyzerBase:

    def __init__(self, name):
        self.name = name

    def get_name(self):
        return self.name


class EquityAnalyzerBase(AnalyzerBase):

    def __init__(self, name):
        super().__init__(name)
    
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

    def __init__(self, name):
        super().__init__(name)
    
    @abstractmethod
    def run(self, symbol, expiration, chain):
        raise NotImplementedError

    @abstractmethod
    def validate(self, 
        symbol=None, 
        expiration=None, 
        chain=None
    ):
    
        raise NotImplementedError

