from abc import ABC, abstractmethod

class DataWriter(ABC):
    @abstractmethod
    def write(self, data, destination):
        pass 