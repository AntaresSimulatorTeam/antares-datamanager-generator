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

from antares.datamanager.env_variables import EnvVariableType


class APIGeneratorConfig:
    def __init__(self) -> None:
        env_vars = EnvVariableType()
        self.host = env_vars.get_env_variable("AW_API_HOST")
        self.token = env_vars.get_env_variable("AW_API_TOKEN")
        self.verify_ssl = False
        self.generation_mode = env_vars.get_env_variable("GENERATION_MODE")


api_config = APIGeneratorConfig()
