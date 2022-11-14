from ..core import AbstractOperator
from ...data import Dataset

class PrinterOperator(AbstractOperator):
    def __init__(self, name: str, msg: str):
        super().__init__(name)
        self._msg = msg

    def _operate(self, ds: Dataset):
        print(self._msg)