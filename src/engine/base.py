from abc import abstractmethod


class EngineBase:

    @abstractmethod
    def run(self):
        raise NotImplementedError