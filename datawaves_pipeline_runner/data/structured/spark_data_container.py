from .structured_data_container import StructuredDataContainer
from typing import List, Dict, Callable, Optional, Iterable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as f

class SparkDataframeContainer(StructuredDataContainer):
    def __init__(self, name: str, spark: SparkSession, df: DataFrame):
        """
        Constructs a SparkSqlContainer.
        - name : the name of the container
        - spark : the spark session to be used to run the operations
        - df : the dataframe used as the backing data store
        """
        super().__init__(name)
        self._spark = spark
        self._df = df

    def get_field_names(self) -> List[str]:
        """
        Retruns a list of all field names as defined in the data container.
        """
        return self._df.columns

    def get_field_types(self) -> Dict[str, str]:
        """
        Returns a list of all field types as defined in the data container
        """
        res = {}
        for field in self._df.schema.fields:
            res[field.name] = str(field.dataType)
        return res

    def rename_field(self, old_name: str, new_name: str):
        self._df = self._df.withColumnRenamed(old_name, new_name)
        return True

    def map_field(self, field_name: str, mapping_func: Callable, new_name: Optional[str] = None):
        mapping_func = f.udf(mapping_func, DoubleType()) # TODO: currently only works for DoubleTypes, would need to
                                                         # register function based on return type of callable
        if new_name is None:
            new_name = field_name
        self._df = self._df.withColumn(new_name, mapping_func(field_name))
    
    def read_field(self, field_name: str) -> List:
        return [c[field_name] for c in self._df.select(field_name).collect()]

    def get_shape(self) -> List[int]:
        return [self._df.count(), len(self._df.columns)]

    def insert_field(self, name: str, data: Iterable):
        """
        Currently not implemented for spark based containers.
        """
        raise NotImplementedError
    
    def serialize(self, format: str, **kwargs):
        partition_count = 1 if 'coalesce' not in kwargs else kwargs['coalesce']
        self._df =self._df.coalesce(partition_count)
        obj =self._df.write.format(format)
        for key in kwargs:
            obj.option(key, kwargs[key])
        obj.save()