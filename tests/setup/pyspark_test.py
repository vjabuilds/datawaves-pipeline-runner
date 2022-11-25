from omegaconf import DictConfig
from pyspark.sql import SparkSession


def test_hydra_default(hydra_cfg: DictConfig):
    """
    Tests to see if the loaded config has been loaded correctly
    """
    assert (
        hydra_cfg.spark == {"head": "local[*]"}
        and hydra_cfg.name == "datawaves-pipeline-runner"
    )


def test_pyspark_local(spark: SparkSession):
    """
    Instantiates a local spark context. If instantiation fails, the test will fail.

    Note: Make sure to use Java 11 or lower (spark instantiation will fail on higher versions)
    """
    assert spark is not None
