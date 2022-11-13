from abc import ABC, abstractmethod
from typing import Dict, List, Callable

class DataContainer(ABC):
    """
    A container that holds data which can be fed to the pipeline.
    """
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
    
