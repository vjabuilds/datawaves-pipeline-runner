from pytest import fixture
import pytest
from datawaves_pipeline_runner.data import Dataset, PandasCsvDataContainer



@fixture
def dataset() -> Dataset:
    """
    Pytest fixture to create a dataset
    """
    ds = Dataset()
    ds.insert_data(PandasCsvDataContainer('test', './tests/data/flowers.csv'))
    ds.insert_data(PandasCsvDataContainer('test_2', './tests/data/flowers.csv'))
    return ds

@pytest.mark.parametrize('name', ['test', 'test_2'])
def test_get_data(dataset: Dataset, name: str):
    assert dataset.get_data(name).get_name() == name
