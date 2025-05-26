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

import os

from dotenv import load_dotenv

load_dotenv()


class EnvVariableType:
    def __init__(self) -> None:
        self.NAS_PATH = os.getenv("NAS_PATH")
        self.PEGASE_LOAD_OUTPUT_DIRECTORY = os.getenv("PEGASE_LOAD_OUTPUT_DIRECTORY")
        self.AW_API_HOST = os.getenv("AW_API_HOST")
        self.AW_API_TOKEN = os.getenv("AW_API_TOKEN")

    def get_env_variable(self, key: str) -> str:
        value = getattr(self, key, None)

        return str(value) if value else ""
