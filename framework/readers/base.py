from abc import ABC, abstractmethod

class DataReader(ABC):
    @abstractmethod
    def read(self, source):
        pass 