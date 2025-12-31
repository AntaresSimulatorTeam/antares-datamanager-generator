# Copyright (c) 2024, RTE (https://www.rte-france.com)
#
# See AUTHORS.txt
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# SPDX-License-Identifier: MPL-2.0
#
# This file is part of the Antares project.


from pathlib import Path

from antares.craft import APIconf
from antares.datamanager.APIGeneratorConfig.config import api_config
from antares.datamanager.env_variables import EnvVariableType
from antares.datamanager.generator.study_adapters import APIStudyFactory, LocalStudyFactory, StudyFactory


def get_study_factory() -> StudyFactory:
    if api_config.generation_mode == "LOCAL":
        env = EnvVariableType()
        nas_path_str = env.get_env_variable("NAS_PATH")
        root_path = Path(nas_path_str) if nas_path_str else Path(".")
        return LocalStudyFactory(path=root_path)
    else:
        conf = APIconf(api_host=api_config.host, token=api_config.token, verify=api_config.verify_ssl)
        return APIStudyFactory(api_conf=conf)
