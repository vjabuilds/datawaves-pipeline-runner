from .data_container import DataContainer
from typing import Dict

class Dataset:
    """
    A class that stores a collection of data containers
    """
    def __init__(self):
        self._data_containers : Dict[str, DataContainer] = {}
        pass

    def insert_data(self, dc: DataContainer):
        """
        Inserts a data container into the collection.
        """
        self._data_containers[dc.get_name()] = dc

    def get_data(self, name: str):
        """
        Gets the data with the given name.
        """
        return self._data_containers[name]