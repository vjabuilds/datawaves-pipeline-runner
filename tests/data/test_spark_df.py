import pytest
from datawaves_pipeline_runner.data import SparkDataframeContainer
import pandas as pd
from pyspark.sql import SparkSession

@pytest.fixture
def dataset(spark: SparkSession):
    pd_df = pd.read_csv('./tests/data/flowers.csv')
    columns = list(pd_df.columns)
    data = pd_df.values.tolist()
    df = spark.createDataFrame(data = data, schema = columns)
    return SparkDataframeContainer('test', spark, df)



def test_loaded(dataset: SparkDataframeContainer):
    """
    Test to check if the dataset has been instantiated
    """

    assert dataset is not None

def test_names(dataset: SparkDataframeContainer):
    expected_names = ['sepal length', 'sepal width', 'petal length', 'petal width', 'species']
    assert expected_names == dataset.get_field_names()

def test_dtypes(dataset: SparkDataframeContainer):
    assert dataset.get_field_types() == {
        'sepal length' : 'DoubleType', 
        'sepal width' : 'DoubleType', 
        'petal length' : 'DoubleType', 
        'petal width' : 'DoubleType', 
        'species': 'StringType'
    }

@pytest.mark.parametrize('old_name', ['sepal length', 'sepal width', 'petal length', 'petal width', 'species'])
def test_rename(dataset: SparkDataframeContainer, old_name: str):
    new_name = old_name + '_new'
    rename_result = dataset.rename_field(old_name, new_name)
    all_names = dataset.get_field_names()
    assert rename_result and new_name in all_names and old_name not in all_names
    return True

@pytest.mark.parametrize('field_name', ['sepal length', 'sepal width', 'petal length', 'petal width'])
def test_map_field(dataset: SparkDataframeContainer, field_name: str):
    mapping = lambda x: x * x
    data = dataset.read_field(field_name)
    dataset.map_field(field_name, mapping)
    assert [mapping(d) for d in data] == dataset.read_field(field_name)

def test_shape(dataset: SparkDataframeContainer):
    assert [150, 5] == dataset.get_shape()

def test_insert_field(dataset: SparkDataframeContainer):
    rows = dataset.get_shape()[0]
    try:
        dataset.insert_field('new field', range(rows))
    except NotImplementedError:
        pass

@pytest.mark.parametrize('field_name', ['sepal length', 'sepal width', 'petal length', 'petal width'])
def test_map_with_rename(dataset: SparkDataframeContainer, field_name: str):
    mapping = lambda x: x * x
    data = dataset.read_field(field_name)
    dataset.map_field(field_name, mapping, 'mapped')
    assert [150, 6] == dataset.get_shape() and [mapping(d) for d in data] == dataset.read_field('mapped')