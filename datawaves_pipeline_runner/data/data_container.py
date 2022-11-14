from abc import ABC, abstractmethod
from typing import List

class DataContainer(ABC):
    """
    A container that holds data which can be fed to the pipeline.
    """

    def __init__(self, name: str):
        self._name = name

    def get_name(self) -> str:
        """
        Returns the name of the data container
        """
        return self._name

    @abstractmethod
    def get_shape(self) -> List[int]:
        """
        Returns the shape of the data. It should be a:
            2D tensor for structured data
            3D tensor for sequence data
            4D tensor for image data
        The first axis always refers to a specific data instance
        """
        pass