from abc import ABC, abstractmethod


class AbstractOperator(ABC):
    @abstractmethod
    def operate(self):
        return