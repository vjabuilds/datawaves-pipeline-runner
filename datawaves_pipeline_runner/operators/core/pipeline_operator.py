from .abstract_operator import AbstractOperator
from ...data import Dataset
from typing import List, Optional

class PipelineOperator(AbstractOperator):
    def __init__(self, name: str, ops: Optional[List[AbstractOperator]] = None):
        super().__init__(name)
        if ops is None:
            self._ops = []
        else:
            self._ops = ops
        self._ds = Dataset()

    def _operate(self, ds: Dataset):
        for op in self._ops:
            op._operate(ds)

    def run(self):
        """
        Executes the pipeline.
        """
        self._operate(self._ds)