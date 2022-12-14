from typing import Dict

import hydra
import pytest
from omegaconf import OmegaConf
from pyspark.sql import SparkSession

from datawaves_pipeline_runner.data import Dataset, SparkDataframeContainer
from datawaves_pipeline_runner.operators.loaders.spark_loaders import \
    SparkSqlLoader
from datawaves_pipeline_runner.util import get_configuration, get_spark


@pytest.fixture
def props():
    return {
        "user": "datawaves",
        "password": "pwd123",
        "driver": "org.postgresql.Driver",
    }


@pytest.fixture
def query():
    return "select p.productname, o.ordercode from orders o join order_products op on o.orderid = op.orderid join products p on o.orderid  = p.productid"


def test_load_spark_table(spark: SparkSession, query: str, props: Dict[str, str]):
    """
    Tests to see if a SparkDataContainer will be loaded
    """
    loader = SparkSqlLoader(
        "test",
        "test",
        spark,
        query,
        "jdbc:postgresql://localhost:5432/datawaves",
        props,
    )
    loader._operate(Dataset())


@pytest.mark.parametrize("name", ["test1", "test2", "test3"])
def test_load_read_spark_table(
    spark: SparkSession, query: str, props: Dict[str, str], name: str
):
    """
    Tests to see if a SparkDataContainer will be loaded and reads the result
    """
    ds = Dataset()
    loader = SparkSqlLoader(
        "test", name, spark, query, "jdbc:postgresql://localhost:5432/datawaves", props
    )
    loader._operate(ds)
    assert isinstance(ds.get_data(name), SparkDataframeContainer)


def test_load_schema_spark_table(
    spark: SparkSession, query: str, props: Dict[str, str]
):
    ds = Dataset()
    name = "test"
    loader = SparkSqlLoader(
        "test",
        "test",
        spark,
        query,
        "jdbc:postgresql://localhost:5432/datawaves",
        props,
    )
    loader._operate(ds)
    ds.get_data(name).get_field_names() == ["productname", "ordercode"]


def test_dictionary(query: str, props: Dict[str, str]):
    hydra_path = "../../../datawaves_pipeline_runner/configs"
    config_name = "config"
    with hydra.initialize(config_path=hydra_path):
        url = "jdbc:postgresql://localhost:5432/datawaves"
        name = "test"
        spark = get_spark(hydra_path, config_name)

        loader = SparkSqlLoader(name, name, spark, query, url, props)
        read_path, read_name = get_configuration(spark)

        assert read_path == hydra_path and read_name == config_name

        conf = loader.to_dictionary()
        OmegaConf.register_resolver("spark_resolver", get_spark)
        target = OmegaConf.create(
            {
                "name": name,
                "_target_": "datawaves_pipeline_runner.operators.loaders.spark_loaders.spark_sql_loader.SparkSqlLoader",
                "data_container_name": name,
                "url": url,
                "query": query,
                "props": props,
                "spark": f"${{spark_resolver:{hydra_path}, {config_name}}}",
            }
        )
    assert conf == target
