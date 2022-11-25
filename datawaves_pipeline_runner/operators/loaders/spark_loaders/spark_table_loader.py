from typing import Dict

from omegaconf import OmegaConf
from pyspark.sql import SparkSession

from ....data import Dataset, SparkDataframeContainer
from .spark_loader import SparkLoader


class SparkTableLoader(SparkLoader):
    """
    Class that produces a SparkDataframeContainer from a SQL table and inserts it into the current Dataset.
    """

    def __init__(
        self,
        name: str,
        data_container_name: str,
        spark: SparkSession,
        table_name: str,
        url: str,
        props: Dict[str, str],
    ):
        """
        Constructs a new loader object.
        - name : the name of the operator
        - data_container_name : the name of the data container that will be generated
        - table_name : the name of the table in the database
        - props : the props def _populate_query(self, dict: OmegaConf):
        dict.query = self._query
        dict.url = self._url
        dict.props = self._propsdictionary which will be passed to spark. Must contain the following keys:
            user : the username to be used when connecting to the database
            password : the password to be used when connecting to the database
            driver : the JDBC driver which will be used
        """
        super().__init__(name, data_container_name, spark)
        self._table_name = table_name
        self._url = url
        self._props = props

    def _load(self, ds: Dataset):
        """
        Loads in the SparkDataframeContainer from the table.
        """
        df = self._spark.read.jdbc(self._url, self._table_name, properties=self._props)
        dc = SparkDataframeContainer(self._data_container_name, self._spark, df)
        ds.insert_data(dc)

    def _populate_query(self, dict: OmegaConf):
        dict.table_name = self._table_name
        dict.url = self._url
        dict.props = self._props
