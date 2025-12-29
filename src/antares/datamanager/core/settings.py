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

from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class GenerationMode(str, Enum):
    API = "API"
    LOCAL = "LOCAL"


class AppSettings(BaseSettings):
    generation_mode: GenerationMode = Field(default=GenerationMode.API, validation_alias="GENERATION_MODE")

    host: str = Field(default="", validation_alias="AW_API_HOST")

    token: str = Field(default="", validation_alias="AW_API_TOKEN")

    verify_ssl: bool = False

    nas_path: Path = Field(default="", validation_alias="NAS_PATH")

    json_output_directory: Path = Field(default="", validation_alias="PEGASE_STUDY_JSON_OUTPUT_DIRECTORY")

    load_output_directory: Path = Field(default="", validation_alias="PEGASE_LOAD_OUTPUT_DIRECTORY")

    thermal_modulation_directory: Path = Field(default="", validation_alias="PEGASE_PARAM_MODULATION_OUTPUT_DIRECTORY")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @field_validator("generation_mode", mode="before")
    @classmethod
    def handle_empty_mode(cls, v: Any) -> Any:
        if isinstance(v, str) and not v.strip():
            return GenerationMode.API
        return v

    @model_validator(mode="after")
    def validate_mode(self) -> "AppSettings":
        if self.generation_mode == GenerationMode.LOCAL:
            if self.nas_path == Path(""):
                raise ValueError("Settings error: 'NAS_PATH' is missing.")
            if not self.nas_path.exists():
                raise ValueError(f"Settings error: NAS_PATH '{self.nas_path}' does not exist.")

        return self


settings = AppSettings()
