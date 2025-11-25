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

from antares.datamanager.env_variables import EnvVariableType


def resolve_directory(env_variable_name: str) -> Path:
    """
    Generic directory resolver:
    - reads the directory from env var
    - if the directory is not absolute, prepends NAS_PATH
    """
    env_vars = EnvVariableType()

    raw_path = Path(env_vars.get_env_variable(env_variable_name))

    # Prepend NAS_PATH if a path is relative
    if not raw_path.is_absolute():
        base = Path(env_vars.get_env_variable("NAS_PATH"))
        raw_path = base / raw_path

    return raw_path
