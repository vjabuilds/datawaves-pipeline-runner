from typing import Dict

from omegaconf import OmegaConf
from pyspark.sql import SparkSession

from ....data import Dataset, SparkDataframeContainer
from .spark_loader import SparkLoader


class SparkSqlLoader(SparkLoader):
    def __init__(
        self,
        name: str,
        data_container_name: str,
        spark: SparkSession,
        query: str,
        url: str,
        props: Dict[str, str],
    ):
        """
        Constructs a new loader object.
        - name : the name of the operator
        - data_container_name : the name of the data container that will be generated
        - query : the name of the table in the database
        - props : the props dictionary which will be passed to spark. Must contain the following keys:
            user : the username to be used when connecting to the database
            password : the password to be used when connecting to the database
            driver : the JDBC driver which will be used
        """
        super().__init__(name, data_container_name, spark)
        self._query = query
        self._url = url
        self._props = props

    def _load(self, ds: Dataset):
        """
        Loads in the SparkDataframeContainer from the table.
        """
        builder = self._spark.read.format("jdbc").option("url", self._url)
        for key in self._props:
            print(key)
            builder.option(key, self._props[key])
        builder.option("query", self._query)
        dc = SparkDataframeContainer(
            self._data_container_name, self._spark, builder.load()
        )
        ds.insert_data(dc)

    def _populate_query(self, dict: OmegaConf):
        dict.query = self._query
        dict.url = self._url
        dict.props = self._props
