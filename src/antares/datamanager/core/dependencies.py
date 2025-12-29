from pathlib import Path
from antares.craft import APIconf
from antares.datamanager.APIGeneratorConfig.config import api_config
from antares.datamanager.env_variables import EnvVariableType
from antares.datamanager.generator.study_adapters import LocalStudyFactory, APIStudyFactory, StudyFactory

def get_study_factory() -> StudyFactory:
    if api_config.generation_mode == "LOCAL":
        env = EnvVariableType()
        nas_path_str = env.get_env_variable("NAS_PATH")
        root_path = Path(nas_path_str) if nas_path_str else Path(".")
        return LocalStudyFactory(path=root_path)
    else:
        conf = APIconf(
            api_host=api_config.host,
            token=api_config.token,
            verify=api_config.verify_ssl
        )
        return APIStudyFactory(api_conf=conf)