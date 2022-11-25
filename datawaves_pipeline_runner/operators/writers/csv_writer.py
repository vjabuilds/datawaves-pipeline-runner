from omegaconf import OmegaConf

from ...data import Dataset, PandasDataContainer, SparkDataframeContainer
from ..core import AbstractOperator


class CsvWriter(AbstractOperator):
    def __init__(self, name: str, dc_name: str, path: str):
        super().__init__(name)
        self._dc_name = dc_name
        self._path = path

    def _operate(self, ds: Dataset):
        dc = ds.get_data(self._dc_name)
        if isinstance(dc, SparkDataframeContainer):
            dc.serialize("csv", path=self._path, header="true", coalesce=1)
        elif isinstance(dc, PandasDataContainer):
            dc.serialize("csv", path_or_buf=self._path, index=False)
        else:
            raise NotImplementedError
        return super()._operate(ds)

    def _populate_dictionary(self, dict: OmegaConf):
        dict.dc_name = self._dc_name
        dict.path = self._path
