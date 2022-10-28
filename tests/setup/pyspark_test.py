import pytest
import pyspark
from hydra import initialize, compose

@pytest.fixture
def hydra_cfg():
    with initialize(version_base=None, config_path="../../datawaves-pipeline-runner/configs"):
        cfg = compose(config_name="config")
        return cfg

def test_hydra_default(hydra_cfg):
    assert hydra_cfg == {
        "spark" : {
            "head" : "local[*]"
        },
        "name": "datawaves-pipeline-runner"
    }

def test_pyspark_install(hydra_cfg):
    sc = pyspark.SparkContext(hydra_cfg.spark.head, "PySpark install test")