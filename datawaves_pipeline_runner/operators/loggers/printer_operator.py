from ..core import AbstractOperator
from ...data import Dataset

class PrinterOperator(AbstractOperator):
    def __init__(self, msg: str):
        self._msg = msg

    def _operate(self, ds: Dataset):
        print(self._msg)