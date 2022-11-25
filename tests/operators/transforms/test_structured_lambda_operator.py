import pandas as pd
import pytest

from datawaves_pipeline_runner.data import (Dataset, PandasDataContainer,
                                            StructuredDataContainer)
from datawaves_pipeline_runner.operators.exceptions import \
    SerializationNotSupported
from datawaves_pipeline_runner.operators.transforms import \
    StructuredLambdaOperator


@pytest.fixture
def dataset() -> Dataset:
    ds = Dataset()
    ds.insert_data(PandasDataContainer("test", pd.read_csv("./tests/data/flowers.csv")))
    return ds


@pytest.mark.parametrize("func", [lambda x: x * x, lambda x: 0, lambda x: -x])
def test_structured_lambda(dataset: Dataset, func):
    old_name = "sepal length"
    new_name = "mapped sepal length"
    dc_name = "test"
    op = StructuredLambdaOperator("test_op", func, dc_name, old_name, new_name)
    op._operate(dataset)
    dc: StructuredDataContainer = dataset.get_data(dc_name)

    assert [func(x) for x in dc.read_field(old_name)] == dc.read_field(new_name)


@pytest.mark.parametrize("func", [lambda x: x * x, lambda x: 0, lambda x: -x])
def test_no_new_column_structured_lambda(dataset: Dataset, func):
    old_name = "sepal length"
    dc_name = "test"
    op = StructuredLambdaOperator("test_op", func, dc_name, old_name)

    dc: StructuredDataContainer = dataset.get_data(dc_name)
    old_data = dc.read_field(old_name)

    op._operate(dataset)

    assert [func(x) for x in old_data] == dc.read_field(old_name)


def test_dictionary():
    """
    Checks to see if the structured lambda to dictionary operation will fail in a predictable way.
    """
    old_name = "sepal length"
    dc_name = "test"
    op = StructuredLambdaOperator("test_op", lambda x: x * x, dc_name, old_name)
    try:
        op.to_dictionary()
    except SerializationNotSupported as e:
        return
    assert False
