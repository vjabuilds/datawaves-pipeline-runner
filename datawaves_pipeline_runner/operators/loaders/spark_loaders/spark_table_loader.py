from .spark_loader import SparkLoader
from ....data import SparkDataframeContainer, Dataset
from pyspark.sql import SparkSession
from typing import Dict

class SparkTableLoader(SparkLoader):
    """
    Class that produces a SparkDataframeContainer from a SQL table and inserts it into the current Dataset.
    """
    def __init__(self, name: str, data_container_name: str, spark: SparkSession, table_name: str, url: str, props: Dict[str, str]):
        """
        Constructs a new loader object.
        - name : the name of the operator
        - data_container_name : the name of the data container that will be generated
        - table_name : the name of the table in the database
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