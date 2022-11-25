import pandas as pd
import pytest
from pytest import fixture

from datawaves_pipeline_runner.data import Dataset, PandasDataContainer


@fixture
def dataset() -> Dataset:
    """
    Pytest fixture to create a dataset
    """
    ds = Dataset()
    ds.insert_data(PandasDataContainer("test", pd.read_csv("./tests/data/flowers.csv")))
    ds.insert_data(
        PandasDataContainer("test_2", pd.read_csv("./tests/data/flowers.csv"))
    )
    return ds


@pytest.mark.parametrize("name", ["test", "test_2"])
def test_get_data(dataset: Dataset, name: str):
    assert dataset.get_data(name).get_name() == name
