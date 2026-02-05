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
from typing import Protocol

from antares.craft.api_conf.api_conf import APIconf
from antares.craft.model.study import Study, create_study_api
from antares.craft.service.local_services.factory import create_study_local
from antares.datamanager.core.settings import settings


class StudyFactory(Protocol):
    def create_study(self, name: str, version: str = settings.study_version) -> Study: ...


class APIStudyFactory:
    """API Adapter"""

    def __init__(self, api_conf: APIconf):
        self.api_conf = api_conf

    def create_study(self, name: str, version: str = settings.study_version) -> Study:
        return create_study_api(name, version, self.api_conf)


class LocalStudyFactory:
    """Local adapter"""

    def __init__(self, path: Path):
        self.path = path

    def create_study(self, name: str, version: str = settings.study_version) -> Study:
        # create_study_local will throw if directaory already exists
        return create_study_local(name, version, self.path)
