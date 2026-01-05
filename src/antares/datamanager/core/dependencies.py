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


from antares.craft import APIconf
from antares.datamanager.core.settings import GenerationMode, settings
from antares.datamanager.generator.study_adapters import APIStudyFactory, LocalStudyFactory, StudyFactory


def get_study_factory() -> StudyFactory:
    if settings.generation_mode == GenerationMode.LOCAL:
        return LocalStudyFactory(path=settings.nas_path)
    else:
        conf = APIconf(api_host=settings.api_host, token=settings.api_token, verify=settings.verify_ssl)
        return APIStudyFactory(api_conf=conf)
