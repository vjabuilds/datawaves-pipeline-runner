import findspark

findspark.init()
findspark.add_jars("./postgresql-42.5.0.jar")

from hydra import compose, initialize
from pyspark.sql import SparkSession
from pytest import fixture


@fixture
def hydra_cfg():
    """
    A pytest fixture which loads in the default config supplied by the application
    """
    with initialize(
        version_base=None, config_path="../datawaves_pipeline_runner/configs"
    ):
        cfg = compose(config_name="config")
        return cfg


@fixture
def spark(hydra_cfg):
    """
    A pytest fixture which creates a spark context
    """
    return (
        SparkSession.builder.appName(hydra_cfg.name)
        .master(hydra_cfg.spark.head)
        .getOrCreate()
    )
