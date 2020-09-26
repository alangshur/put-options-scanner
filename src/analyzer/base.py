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