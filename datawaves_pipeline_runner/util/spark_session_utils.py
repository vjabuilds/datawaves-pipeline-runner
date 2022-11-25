from typing import Tuple

import hydra
from pyspark.sql import SparkSession

__spark = {}
__configuration = {}


def get_spark(hydra_path: str, config_name: str) -> SparkSession:
    """
    Creates a SparkSession using the given config file. If called multiple times
    with the same arugments, will return the same object multiple times.

    - returns : a SparkSession
    """
    global __spark
    global __configuration

    key = (hydra_path, config_name)
    if key in __spark:
        return __spark[key]
    else:
        cfg = hydra.compose(config_name)
        __spark[key] = (
            SparkSession.builder.appName(cfg.name).master(cfg.spark.head).getOrCreate()
        )
        __configuration[__spark[key]] = key
        return __spark[key]


def get_configuration(spark: SparkSession) -> Tuple[str, str]:
    """
    Returns the associated hydra path and configuration name with the supplied
    SparkSession object;

    - returns : a pair in the form of (hydra_path, config_name)
    """
    global __configuration
    return __configuration[spark]
