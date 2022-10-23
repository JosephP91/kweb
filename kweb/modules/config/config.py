import os
import yaml

from munch import DefaultMunch


class ConfigReader:
    _config_object = None

    def read(self, env_name: str) -> DefaultMunch:
        if ConfigReader._config_object is not None:
            return ConfigReader._config_object[env_name]

        cur_abs_path = os.path.abspath(os.path.dirname(__file__))
        full_file_path = os.path.join(cur_abs_path, "../../config/application.yml")
        with open(full_file_path) as config_file_stream:
            config = yaml.safe_load(config_file_stream)

        ConfigReader._config_object = DefaultMunch.fromDict(config)
        return ConfigReader._config_object[env_name]

