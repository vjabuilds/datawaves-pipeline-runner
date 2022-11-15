import pytest
from datawaves_pipeline_runner.operators.loaders import PandasCsvLoader
from datawaves_pipeline_runner.data import PandasDataContainer
from datawaves_pipeline_runner.data import Dataset

def test_load_pandas_csv():
    """
    Tests to see if a pandas dataset will be loaded
    """
    loader = PandasCsvLoader('test', 'csv_pandas', './tests/data/flowers.csv')
    loader._operate(Dataset())

@pytest.mark.parametrize('name', ['test1', 'test2', 'test3'])
def test_load_read_pandas_csv(name: str):
    """
    Tests to see if a pandas dataset will be loaded and reads the result
    """
    ds = Dataset()
    loader = PandasCsvLoader('test', name, './tests/data/flowers.csv')
    loader._operate(ds)
    assert isinstance(ds.get_data(name), PandasDataContainer)

def test_load_schema_pandas_csv():
    ds = Dataset()
    name = 'test'
    loader = PandasCsvLoader('test', name, './tests/data/flowers.csv')
    loader._operate(ds)
    ds.get_data(name).get_field_names() == ['sepal length', 'sepal width', 'petal length', 'petal width', 'species']   
