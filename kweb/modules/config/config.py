import os

import yaml
from munch import DefaultMunch


class ConfigReader:
    @staticmethod
    def read(env_name: str) -> DefaultMunch:
        cur_abs_path = os.path.abspath(os.path.dirname(__file__))
        full_file_path = os.path.join(cur_abs_path, "../../config/application.yml")
        with open(full_file_path) as config_file_stream:
            yaml_content = yaml.safe_load(config_file_stream)[env_name]
            return DefaultMunch.fromDict(yaml_content)
