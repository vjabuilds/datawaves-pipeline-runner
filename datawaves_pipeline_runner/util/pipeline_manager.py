import hydra
from hydra.utils import instantiate
import os
from omegaconf import OmegaConf

from .spark_session_utils import get_spark
from ..operators.core import AbstractOperator


class PipelineManager            :
    def __init__(self):
        self._pipeline = None
        OmegaConf.register_resolver("spark_resolver", get_spark)

    def load_pipeline(self, path: str, flat: bool = True):
        if flat:
            conf = OmegaConf.load(path)
            self.load_pipeline_from_conf(conf)
        else:
            raise NotImplemented

    def load_pipeline_from_conf(self, conf: OmegaConf):
        self._pipeline = instantiate(conf)

    def set_pipeline(self, pipeline: AbstractOperator):
        self._pipeline = pipeline

    def save_pipeline(self, path: str, flat: bool = True):
        conf = self._pipeline.to_dictionary()
        if flat:
            OmegaConf.save(conf, path)
        else:
            raise NotImplemented