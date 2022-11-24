from ..core import AbstractOperator
from ...data import Dataset
from omegaconf import OmegaConf

class PrinterOperator(AbstractOperator):
    def __init__(self, name: str, msg: str):
        super().__init__(name)
        self._msg = msg

    def _operate(self, ds: Dataset):
        print(self._msg)

    def _populate_dictionary(self, dict: OmegaConf):
        dict.msg = self._msg