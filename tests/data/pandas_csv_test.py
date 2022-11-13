import pytest
from pytest import fixture
from datawaves_pipeline_runner.data import PandasCsvDataContainer

@fixture
def dataset() -> PandasCsvDataContainer:
    """
    A pytest fixture which loads in the pandas csv dataset
    """
    return PandasCsvDataContainer('./tests/data/flowers.csv')

def test_loaded(dataset: PandasCsvDataContainer):
    """
    Test to check if the dataset has been instantiated
    """

    assert dataset is not None

def test_names(dataset: PandasCsvDataContainer):
    expected_names = ['sepal length', 'sepal width', 'petal length', 'petal width', 'species']
    assert expected_names == dataset.get_field_names()

def test_dtypes(dataset: PandasCsvDataContainer):
    assert dataset.get_field_types() == {
        'sepal length' : 'float64', 
        'sepal width' : 'float64', 
        'petal length' : 'float64', 
        'petal width' : 'float64', 
        'species': 'object'
    }

@pytest.mark.parametrize('old_name', ['sepal length', 'sepal width', 'petal length', 'petal width', 'species'])
def test_rename(dataset: PandasCsvDataContainer, old_name: str):
    new_name = old_name + '_new'
    rename_result = dataset.rename_field(old_name, new_name)
    all_names = dataset.get_field_names()
    assert rename_result and new_name in all_names and old_name not in all_names

@pytest.mark.parametrize('field_name', ['sepal length', 'sepal width', 'petal length', 'petal width'])
def test_rename(dataset: PandasCsvDataContainer, field_name: str):
    mapping = lambda x: x * x
    data = dataset.read_field(field_name)
    dataset.map_field(field_name, mapping)
    assert [mapping(d) for d in data] == dataset.read_field(field_name)