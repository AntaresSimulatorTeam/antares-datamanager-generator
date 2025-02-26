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


class APIGenerationError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class AreaGenerationError(Exception):
    def __init__(self, area_name: str, message: str) -> None:
        self.message = f"Could not create the area {area_name}: " + message
        super().__init__(self.message)


class LinkGenerationError(Exception):
    def __init__(self, area_from: str, area_to: str, message: str) -> None:
        self.message = f"Could not create the link {area_from} / {area_to}: " + message
        super().__init__(self.message)
