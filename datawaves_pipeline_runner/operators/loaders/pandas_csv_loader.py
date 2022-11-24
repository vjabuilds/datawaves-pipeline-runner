from .base_loader import BaseLoader
from ...data import Dataset, PandasDataContainer
import pandas as pd
from omegaconf import OmegaConf

class PandasCsvLoader(BaseLoader):
    """
    Class that produces a PandasDataContainer and inserts it into the current Dataset.
    """
    def __init__(self, name: str, data_container_name: str, csv_path: str):
        """
        Constructs a new loader object.
        - name : the name of the operator
        - data_container_name : the name of the data container that will be generated
        - csv_path : the filesystem path to get the csv file
        """
        super().__init__(name, data_container_name)
        self._csv_path = csv_path

    def _load(self, ds: Dataset):
        """
        Loads in the PandasDataContainer.
        """
        dc = PandasDataContainer(self._data_container_name, pd.read_csv(self._csv_path))
        ds.insert_data(dc)

    def _populate_dictionary(self, dict: OmegaConf):
        dict.data_container_name = self._data_container_name
        dict.csv_path = self._csv_path