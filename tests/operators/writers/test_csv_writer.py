import shutil
import pytest
import os
from pyspark.sql import SparkSession
from datawaves_pipeline_runner.operators.writers import CsvWriter
from datawaves_pipeline_runner.operators.loaders import PandasCsvLoader
from datawaves_pipeline_runner.operators.loaders.spark_loaders import SparkTableLoader
from datawaves_pipeline_runner.data import Dataset, PandasDataContainer, SparkDataframeContainer
from omegaconf import OmegaConf

@pytest.fixture
def dataset():
    return Dataset()

@pytest.fixture
def pandas_ds(dataset: Dataset):
    name = 'test_pandas'
    loader = PandasCsvLoader(name, name, './tests/data/flowers.csv')
    loader._operate(dataset)
    return dataset, name

@pytest.fixture()
def spark_ds(dataset: Dataset, spark: SparkSession):
    name = 'test_spark'
    props = {
            'user': 'datawaves',
            'password': 'pwd123',
            'driver': 'org.postgresql.Driver',
    }
    loader = SparkTableLoader(name, name, spark, 'flowers', 'jdbc:postgresql://localhost:5432/datawaves', props)
    loader._operate(dataset)
    return dataset, name

def test_serialize_pandas(pandas_ds):
    ds,  name = pandas_ds
    filename = 'test_123.csv'
    csv = CsvWriter('csv', name, filename)
    csv._operate(ds)
    assert os.path.exists(filename)
    os.remove(filename)

def test_serialize_spark(spark_ds):
    ds,  name = spark_ds
    path = 'test_123'
    csv = CsvWriter('csv', name, path)
    csv._operate(ds)
    assert os.path.exists(path) and os.path.exists(os.path.join(path, '_SUCCESS'))
    shutil.rmtree(path)

def test_dictionary():
    name = 'test'
    path = 'test_123'
    csv = CsvWriter(name, name, path)
    conf = csv.to_dictionary()
    assert conf == OmegaConf.create({
        'name': name,
        '_target_': 'datawaves_pipeline_runner.operators.writers.csv_writer.CsvWriter',
        'dc_name': name,
        'path': path
    })