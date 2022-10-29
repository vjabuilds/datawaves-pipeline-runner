from abc import ABC, abstractmethod
from pyspark import SparkContext


class AbstractOperator(ABC):
    @abstractmethod
    def operate(self, spark: SparkContext):
        return