from abc import abstractmethod

from omegaconf import OmegaConf
from pyspark.sql import SparkSession

from ....util import get_configuration
from .. import BaseLoader


class SparkLoader(BaseLoader):
    def __init__(self, name: str, data_container_name: str, spark: SparkSession):
        super().__init__(name, data_container_name)
        self._spark = spark

    def _populate_dictionary(self, dict: OmegaConf):
        dict.data_container_name = self._data_container_name
        hydra_path, config_name = get_configuration(self._spark)
        dict.spark = f"${{spark_resolver:{hydra_path}, {config_name}}}"
        self._populate_query(dict)

    @abstractmethod
    def _populate_query(self, dict: OmegaConf):
        """
        Populates all query data. SparkSession data should not be loaded in.
        """
        pass
