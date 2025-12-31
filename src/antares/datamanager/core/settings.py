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

from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class GenerationMode(str, Enum):
    API = "API"
    LOCAL = "LOCAL"


@dataclass(frozen=True)
class Settings:
    """
    Centralized app config (.env)
    """

    def _get_required(self, key: str) -> str:
        value = os.getenv(key)
        if value is None:
            raise ValueError(f"Missing environment variable: '{key}'")
        return value

    def _resolve_env_path(self, key: str) -> Path:
        raw_val = self._get_required(key)
        path = Path(raw_val)

        if path.is_absolute():
            return path

        # If relative, add NAS_PATH as base
        return self.nas_path / path

    # Properties for lazy loading (avoid crashes at import)
    @property
    def generation_mode(self) -> GenerationMode:
        return GenerationMode(os.getenv("GENERATION_MODE", "API"))

    @property
    def nas_path(self) -> Path:
        return Path(self._get_required("NAS_PATH"))

    @property
    def load_output_directory(self) -> Path:
        return self._resolve_env_path("PEGASE_LOAD_OUTPUT_DIRECTORY")

    @property
    def study_json_directory(self) -> Path:
        return self._resolve_env_path("PEGASE_STUDY_JSON_OUTPUT_DIRECTORY")

    @property
    def param_modulation_directory(self) -> Path:
        return self._resolve_env_path("PEGASE_PARAM_MODULATION_OUTPUT_DIRECTORY")

    @property
    def api_host(self) -> str:
        return os.getenv("AW_API_HOST", "")

    @property
    def api_token(self) -> str:
        return os.getenv("AW_API_TOKEN", "")

    @property
    def verify_ssl(self) -> bool:
        return False


settings = Settings()
