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

from dataclasses import dataclass, field
from typing import Any


@dataclass
class StudyData:
    name: str
    areas: dict[str, Any] = field(default_factory=dict)
    links: dict[str, Any] = field(default_factory=dict)
    area_loads: dict[str, list[str]] = field(default_factory=dict)
    area_thermals: dict[str, Any] = field(default_factory=dict)
    area_sts: dict[str, Any] = field(default_factory=dict)
    enable_random_ts: bool = True
    nb_years: int = 1
