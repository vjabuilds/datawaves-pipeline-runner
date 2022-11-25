from typing import Callable, Optional

from omegaconf import OmegaConf

from ...data import Dataset, StructuredDataContainer
from ..core import AbstractOperator
from ..exceptions import SerializationNotSupported


class StructuredLambdaOperator(AbstractOperator):
    """
    The operator which invokes a lambda on a given DataContainer.
    """

    def __init__(
        self,
        name: str,
        lambda_op: Callable,
        dc_name: str,
        field_name: str,
        new_name: Optional[str] = None,
    ):
        """
        Constructor for the StructuredLambdaOperator.
        - name - the name of the operator
        - lambda_op - the operation which will be ran on the column
        - dc_name - the name of the DataContainer in the Dataset
        - field_name - the name of the field which will be mapped
        - new_name - the name of the generated column
        """
        super().__init__(name)
        self._lambda = lambda_op
        self._dc_name = dc_name
        self._field_name = field_name
        self._new_name = new_name

    def _operate(self, ds: Dataset):
        """
        Executes the defined operation on the given dataset. Relies on the map_field
        operation of the StructuredDataContainer
        """
        dc = ds.get_data(self._dc_name)
        assert isinstance(dc, StructuredDataContainer)
        dc.map_field(self._field_name, self._lambda, self._new_name)

    def _populate_dictionary(self, dict: OmegaConf):
        """
        Serialization of arbitrary functions is not supported currently.
        """
        raise SerializationNotSupported(type(self))
