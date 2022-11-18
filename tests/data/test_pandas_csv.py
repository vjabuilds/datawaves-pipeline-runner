import pytest
from pytest import fixture
from datawaves_pipeline_runner.data import PandasDataContainer
import pandas as pd
import uuid
import os

@fixture
def dataset() -> PandasDataContainer:
    """
    A pytest fixture which loads in the pandas csv dataset
    """
    return PandasDataContainer('test', pd.read_csv('./tests/data/flowers.csv'))

def test_loaded(dataset: PandasDataContainer):
    """
    Test to check if the dataset has been instantiated
    """

    assert dataset is not None

def test_names(dataset: PandasDataContainer):
    expected_names = ['sepal length', 'sepal width', 'petal length', 'petal width', 'species']
    assert expected_names == dataset.get_field_names()

def test_dtypes(dataset: PandasDataContainer):
    assert dataset.get_field_types() == {
        'sepal length' : 'float64', 
        'sepal width' : 'float64', 
        'petal length' : 'float64', 
        'petal width' : 'float64', 
        'species': 'object'
    }

@pytest.mark.parametrize('old_name', ['sepal length', 'sepal width', 'petal length', 'petal width', 'species'])
def test_rename(dataset: PandasDataContainer, old_name: str):
    new_name = old_name + '_new'
    rename_result = dataset.rename_field(old_name, new_name)
    all_names = dataset.get_field_names()
    assert rename_result and new_name in all_names and old_name not in all_names

@pytest.mark.parametrize('field_name', ['sepal length', 'sepal width', 'petal length', 'petal width'])
def test_map_field(dataset: PandasDataContainer, field_name: str):
    mapping = lambda x: x * x
    data = dataset.read_field(field_name)
    dataset.map_field(field_name, mapping)
    assert [mapping(d) for d in data] == dataset.read_field(field_name)

def test_shape(dataset: PandasDataContainer):
    assert [150, 5] == dataset.get_shape()

def test_insert_field(dataset: PandasDataContainer):
    rows = dataset.get_shape()[0]
    dataset.insert_field('new field', range(rows))
    assert [150, 6] == dataset.get_shape() and list(range(rows)) == dataset.read_field('new field')

@pytest.mark.parametrize('field_name', ['sepal length', 'sepal width', 'petal length', 'petal width'])
def test_map_with_rename(dataset: PandasDataContainer, field_name: str):
    mapping = lambda x: x * x
    data = dataset.read_field(field_name)
    dataset.map_field(field_name, mapping, 'mapped')
    assert [150, 6] == dataset.get_shape() and [mapping(d) for d in data] == dataset.read_field('mapped')

def test_serialize(dataset: PandasDataContainer):
    file_name = str(uuid.uuid4()) + '.csv'
    dataset.serialize('csv', path_or_buf = file_name, index = False)
    df = pd.read_csv(file_name)
    read_data = PandasDataContainer('new', df)
    assert dataset.get_shape() == read_data.get_shape()
    fields = dataset.get_field_names()
    for f in fields:
        assert dataset.read_field(f) == read_data.read_field(f)
    os.remove(file_name)
