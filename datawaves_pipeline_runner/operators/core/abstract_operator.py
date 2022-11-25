from abc import ABC, abstractmethod

from omegaconf import OmegaConf

from ...data import Dataset


class AbstractOperator(ABC):
    """
    The base class which all pipeline operators should inherit.
    """

    def __init__(self, name: str):
        self.__name = name

    def __classname(self):
        cls = type(self)
        module = cls.__module__
        name = cls.__qualname__
        if module is not None and module != "__builtin__":
            name = module + "." + name
        return name

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

    def to_dictionary(self) -> OmegaConf:
        """
        Transforms the operator to dictionary format. The dictionary should contain all data
        needed to reconstruct the operator.

        - returns : an __classnameOmegaConf dictionary
        """

        dict = OmegaConf.create()
        dict.name = self.get_name()
        dict._target_ = self.__classname()
        self._populate_dictionary(dict)
        return dict

    @abstractmethod
    def _populate_dictionary(self, dict: OmegaConf):
        """
        Populates the given dictionary with all arguments needed for the operator to be constructed.
        Does not need to set the name and class of the operator.
        """
        pass
