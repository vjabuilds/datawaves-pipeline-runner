from abc import abstractmethod
from .data_container import DataContainer
from typing import Dict, List, Callable

class StructuredDataContainer(DataContainer):
    """
    A container that holds structured data which can be fed to the pipeline.
    """

    def __init__(self, name: str):
        super().__init__(name)

    @abstractmethod
    def get_field_names(self) -> List[str]:
        """
        Retruns a list of all field names as defined in the data container.
        """
        pass

    @abstractmethod
    def get_field_types(self) -> Dict[str, str]:
        """
        Returns a list of all field types as defined in the data container
        """
        pass

    @abstractmethod
    def rename_field(self, old_name: str, new_name: str):
        """
        Renames the field called old_name to  new_name
        """
        pass

    @abstractmethod
    def map_field(self, field_name: str, mapping_func: Callable):
        """
        Transforms the specified field using the mapping function. 
        """
        pass
    
    @abstractmethod
    def read_field(self, field_name: str) -> List:
        """
        Reads the value from a given field, as a list.
        """
        pass