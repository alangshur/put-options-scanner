from abc import abstractmethod


class ScannerBase:

    @abstractmethod
    def run(self):
        raise NotImplementedError