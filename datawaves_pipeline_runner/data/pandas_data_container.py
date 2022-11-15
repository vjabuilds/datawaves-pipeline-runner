from .structured_data_container import StructuredDataContainer
from typing import Dict, List, Callable, Iterable, Optional
import pandas as pd

class PandasDataContainer(StructuredDataContainer):
    """
    A structured data container backed by a pandas dataframe.
    """
    def __init__(self, name:str, df: pd.DataFrame):
        super().__init__(name)
        self._df = df

    def get_field_names(self) -> List[str]:
        """
        Returns a list of all field names as defined in the data container.
        """
        return list(self._df.columns)

    def get_field_types(self) -> Dict[str, str]:
        """
        Returns a list of all field types as defined in the data container
        """
        result = {}
        for col, dtype in zip(self._df.columns, self._df.dtypes):
            result[col] = str(dtype)
        return result

    def rename_field(self, old_name: str, new_name: str):
        """
        Renames the field called old_name to  new_name
        """
        self._df[new_name] = self._df[old_name]
        self._df.drop(old_name, axis=1, inplace=True)
        return True

    def map_field(self, field_name: str, mapping_func: Callable, new_name: Optional[str] = None):
        """
        Transforms the specified field using the mapping function
        - field_name - the field which is to be transformed
        - mapping_func - the function which will be applied to the field
        - new_name - optional name of the transformed data. If none, will overwrite old data
        """
        if(new_name is None):
            new_name = field_name
        self._df[new_name] = self._df[field_name].apply(mapping_func)

    def read_field(self, field_name: str) -> List:
        """
        Reads the value from a given field, as a list.
        """
        return list(self._df[field_name])

    def get_shape(self) -> List[int]:
        """
        Returns a 2D tensor in the form of [rows, cols]
        """
        return list(self._df.shape)

    def insert_field(self, name: str, data: Iterable):
        self._df[name] = data