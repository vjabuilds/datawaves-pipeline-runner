import pytest
from typing import Dict
from datawaves_pipeline_runner.operators.loaders.spark_loaders import SparkTableLoader
from datawaves_pipeline_runner.data import SparkDataframeContainer
from datawaves_pipeline_runner.data import Dataset
from pyspark.sql import SparkSession
from omegaconf import OmegaConf
from datawaves_pipeline_runner.util import get_configuration, get_spark

@pytest.fixture
def props():
    return {
            'user': 'datawaves',
            'password': 'pwd123',
            'driver': 'org.postgresql.Driver',
        }

def test_load_spark_table(spark: SparkSession, props: Dict[str, str]):
    """
    Tests to see if a SparkDataContainer will be loaded
    """
    loader = SparkTableLoader('test', 'test', spark, 'flowers', 'jdbc:postgresql://localhost:5432/datawaves', props)
    loader._operate(Dataset())

@pytest.mark.parametrize('name', ['test1', 'test2', 'test3'])
def test_load_read_spark_table(spark: SparkSession, props: Dict[str, str],  name: str):
    """
    Tests to see if a SparkDataContainer will be loaded and reads the result
    """
    ds = Dataset()
    loader = SparkTableLoader('test', name, spark, 'flowers', 'jdbc:postgresql://localhost:5432/datawaves', props)
    loader._operate(ds)
    assert isinstance(ds.get_data(name), SparkDataframeContainer)

def test_load_schema_spark_table(spark: SparkSession, props: Dict[str, str]):
    ds = Dataset()
    name = 'test'
    loader = SparkTableLoader('test', 'test', spark, 'flowers', 'jdbc:postgresql://localhost:5432/datawaves', props)
    loader._operate(ds)
    ds.get_data(name).get_field_names() == ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']   
    
def test_dictionary(props: Dict[str, str]):
    url = 'jdbc:postgresql://localhost:5432/datawaves'
    table_name = 'flowers'
    name = 'test'
    hydra_path = '../configs'
    config_name = 'config'
    spark = get_spark(hydra_path, config_name)

    loader = SparkTableLoader(name, name, spark, table_name, url, props)
    read_path, read_name = get_configuration(spark)

    assert read_path == hydra_path and read_name == config_name

    conf = loader.to_dictionary()
    target = OmegaConf.create({
        'name': name,
        '_target_': 'datawaves_pipeline_runner.operators.loaders.spark_loaders.spark_table_loader.SparkTableLoader',
        'data_container_name': name,
        'url': url,
        'table_name': table_name,
        'props': props,
        'spark': f'${{spark_resolver:{hydra_path}, {config_name}}}'
    })
    assert conf == target