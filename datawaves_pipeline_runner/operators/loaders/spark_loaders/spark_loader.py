from .. import BaseLoader
from pyspark.sql import SparkSession

class SparkLoader(BaseLoader):
    def __init__(self, name: str, data_container_name: str, spark: SparkSession):
        super().__init__(name, data_container_name)
        self._spark = spark
