import pandas as pd
import pytest
from omegaconf import OmegaConf

from datawaves_pipeline_runner.data import (Dataset, FieldAggregation,
                                            PandasDataContainer,
                                            StructuredDataContainer)
from datawaves_pipeline_runner.operators.transforms import NormalizeOperator


@pytest.fixture
def dataset() -> Dataset:
    ds = Dataset()
    ds.insert_data(PandasDataContainer("test", pd.read_csv("./tests/data/flowers.csv")))
    return ds


@pytest.mark.parametrize(
    "field_name", ["sepal length", "sepal width", "petal length", "petal width"]
)
def test_normalize(dataset: Dataset, field_name):
    op_name = "test_op"
    dc_name = "test"
    op = NormalizeOperator(op_name, dc_name, field_name)
    op._operate(dataset)

    dc: StructuredDataContainer = dataset.get_data(dc_name)

    assert -1e-5 < dc.aggregate_field(field_name, FieldAggregation.Mean) < 1e-5
    assert 1 - 1e-5 < dc.aggregate_field(field_name, FieldAggregation.Std) < 1 + 1e-5


def test_dictionary():
    """
    Checks to see if the structured lambda to dictionary operation will fail in a predictable way.
    """
    field_name = "sepal length"
    op_name = "test_op"
    dc_name = "test"
    op = NormalizeOperator(op_name, dc_name, field_name)
    conf = op.to_dictionary()
    target = OmegaConf.create(
        {
            "name": op_name,
            "_target_": "datawaves_pipeline_runner.operators.transforms.normalize_operator.NormalizeOperator",
            "dc_name": dc_name,
            "field_name": field_name,
        }
    )
    assert conf == target
