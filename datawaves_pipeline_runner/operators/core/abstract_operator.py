from abc import ABC, abstractmethod
from ...data import Dataset

class AbstractOperator(ABC):
    """
    The base class which all pipeline operators should inherit.
    """
    def __init__(self, name: str):
        self.__name = name
    
    def get_name(self) -> str:
        """
        Returns the name of the operator.
        """
        return self.__name

    @abstractmethod
    def _operate(self, ds: Dataset):
        """
        Executes the defined operation on the given dataset.
        """
        pass