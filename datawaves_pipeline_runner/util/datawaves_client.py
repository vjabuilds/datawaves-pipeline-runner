import requests
import json
from omegaconf import OmegaConf
from ..operators.core.pipeline_operator import PipelineOperator

class DatawavesClient:
    def __init__(self, username: str, password: str, root_url: str, experimenter_url: str):
        response = requests.post(
            url=root_url+'/users/login',
            json = {
                'username': username,
                'password': password
            }
        )
        self._root_url = root_url
        self._experimenter_url = experimenter_url
        self._current_token = response.text
        self._refresh_token = response.cookies['refresh_token']

    def __refresh_token(self):
        self._current_token = requests.get(url=self._root_url + '/users/refresh', cookies = {'refresh_token': self._refresh_token})

    def create_pipeline(self, pipeline: PipelineOperator):
        serialized = OmegaConf.to_yaml(pipeline.to_dictionary())
        self.__refresh_token()
        requests.post(
            url = self._experimenter_url + '/pipelines',
            json = {
                'description': 'just a simple pipeline that does things',
                'name': pipeline.get_name(),
                'version': '1.0.0',
                'yamlFormat': serialized
            }
        )

        # TODO: Check if the response code is anything other than 201 - if it is, raise a custom new exception