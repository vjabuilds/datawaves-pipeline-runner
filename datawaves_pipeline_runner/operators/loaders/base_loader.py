from ..core import AbstractOperator
from abc import abstractmethod
from ...data import Dataset

class BaseLoader(AbstractOperator):
    """
    Base class which should be inherited by all data loaders
    """
    def __init__(self, name: str, data_container_name: str):
        """
        Constructs a new loader object.
        - name : the name of the operator
        - data_container_name : the name of the data container that will be generated
        """
        super().__init__(name)
        self._data_container_name = data_container_name

    def _operate(self, ds: Dataset):
        """
        Loads the dataset in and checks if the dataset is placed in the correct location.
        Will raise a KeyError if the dataset isn't placed correctly. 
        """
        self._load(ds)
        assert ds.get_data(self._data_container_name)

    @abstractmethod
    def _load(self, ds: Dataset):
        pass