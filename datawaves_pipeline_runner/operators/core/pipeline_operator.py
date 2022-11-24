from .abstract_operator import AbstractOperator
from ...data import Dataset
from typing import List, Optional
from omegaconf import OmegaConf

class PipelineOperator(AbstractOperator):
    def __init__(self, name: str, operators: Optional[List[AbstractOperator]] = None):
        super().__init__(name)
        if operators is None:
            self._ops = []
        else:
            self._ops = operators
        self._ds = Dataset()

    def _operate(self, ds: Dataset):
        for op in self._ops:
            op._operate(ds)

    def run(self):
        """
        Executes the pipeline.
        """
        self._operate(self._ds)

    def _populate_dictionary(self, dict: OmegaConf):
        dicts = []
        for op in self._ops:
            dicts.append(op.to_dictionary())
        dict.operators = dicts
        