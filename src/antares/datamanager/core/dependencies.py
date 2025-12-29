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

from typing import Annotated

from fastapi import Depends

from antares.craft.api_conf.api_conf import APIconf
from antares.datamanager.core.settings import AppSettings, GenerationMode, settings
from antares.datamanager.generator.study_adapters import APIStudyFactory, LocalStudyFactory, StudyFactory


def get_study_factory(app_settings: Annotated[AppSettings, Depends(lambda: settings)]) -> StudyFactory:
    if app_settings.generation_mode == GenerationMode.LOCAL:
        print(f"Local mode, path: {app_settings.nas_path}")
        return LocalStudyFactory(path=app_settings.nas_path)

    print(f"API Mode, host URL: {app_settings.host}")
    api_conf = APIconf(app_settings.host, app_settings.token or "", app_settings.verify_ssl)
    return APIStudyFactory(api_conf)
