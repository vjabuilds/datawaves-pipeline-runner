from .structured_data_container import StructuredDataContainer
from typing import Dict, List, Callable
import pandas as pd

class PandasCsvDataContainer(StructuredDataContainer):
    """
    A structured data contianer backed by a pandas csv file.
    """
    def __init__(self, name:str, csv_path: str):
        super().__init__(name)
        self._csv_path = csv_path
        self._df = pd.read_csv(csv_path)

    def get_field_names(self) -> List[str]:
        """
        Retruns a list of all field names as defined in the data container.
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

    def map_field(self, field_name: str, mapping_func: Callable):
        """
        Transforms the specified field using the mapping function. 
        """
        self._df[field_name] = self._df[field_name].apply(mapping_func)

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