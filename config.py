import json
import os
from typing import Dict, Any


class ReserConfig:
    __config = dict()

    @staticmethod
    def get_config() -> dict:
        if not ReserConfig.__config:
            ReserConfig.__config = ReserConfig.__load_config()
        return ReserConfig.__config

    @staticmethod
    def __load_config():
        try:
            dir_path: str = os.path.dirname(os.path.realpath(__file__))
            config_file = os.path.join(dir_path, 'config.json')
            with open(config_file) as f:
                config: dict = json.load(f)
        except IOError:
            config = json.loads(os.environ["CONFIG"])
        # Insert all the secrets from env vars
        return ReserConfig.__insert_config_secrets(config)

    @staticmethod
    def __insert_config_secrets(config: Dict[str, Any]) -> Dict[str, Any]:
        for key, value in config.items():
            if value == "SECRET":
                # TODO - Find the corresponding environment variable
                config[key] = ReserConfig.__get_secret(key)
                pass
            elif value is dict:
                config[key] = ReserConfig.__insert_config_secrets(value)
        return config

    @staticmethod
    def __get_secret(name: str) -> str:
        """
        Get the secrets from the environment variables, then a backing file?
        :param name: secret name
        :return: secret value
        """
        try:
            return os.environ[name]
        except KeyError:
            # TODO - get from secrets.json file?
            raise
