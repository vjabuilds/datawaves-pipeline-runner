from omegaconf import OmegaConf

from ...data import Dataset, FieldAggregation, StructuredDataContainer
from ..core import AbstractOperator


class NormalizeOperator(AbstractOperator):
    def __init__(self, name: str, dc_name: str, field_name: str):
        super().__init__(name)
        self._dc_name = dc_name
        self._field_name = field_name

    def _operate(self, ds: Dataset):
        """
        Executes the defined operation on the given dataset. Relies on the map_field
        operation of the StructuredDataContainer
        """
        dc: StructuredDataContainer = ds.get_data(self._dc_name)
        assert isinstance(dc, StructuredDataContainer)
        avg = dc.aggregate_field(
            self._field_name, FieldAggregation.Sum
        ) / dc.aggregate_field(self._field_name, FieldAggregation.Count)
        std = dc.aggregate_field(self._field_name, FieldAggregation.Std)
        dc.map_field(self._field_name, lambda x: (x - avg) / std, self._field_name)

    def _populate_dictionary(self, dict: OmegaConf):
        dict.dc_name = self._dc_name
        dict.field_name = self._field_name
