from abc import abstractmethod


class AnalyzerBase:

    def __init__(self, name):
        self.name = name

    @abstractmethod
    def run(self):
        raise NotImplementedError

    @abstractmethod
    def validate(self):
        raise NotImplementedError

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
    def run(self, 
        symbol, 
        underlying,
        dividend,
        expiration, 
        chain
    ):
    
        raise NotImplementedError

    @abstractmethod
    def validate(self, 
        symbol=None, 
        underlying=None,
        dividend=None,
        expiration=None, 
        chain=None,
    ):
    
        raise NotImplementedError
