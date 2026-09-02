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
    area_dsr: dict[str, Any] = field(default_factory=dict)
    area_misc: dict[str, Any] = field(default_factory=dict)
    area_res: dict[str, Any] = field(default_factory=dict)
    area_hydro: dict[str, Any] = field(default_factory=dict)
    area_nuclear: dict[str, Any] = field(default_factory=dict)
    nuclear_modulation_binding_constraints: dict[str, Any] | None = None
    nuclear_talon_binding_constraint: dict[str, Any] | None = None
    flowbased: dict[str, Any] | None = None
    enable_random_ts: bool = True
    # TODO JSON must contain this field, mean while it is default to 0
    seed_tsgen_link: int = 0
    settings: dict[str, Any] = field(default_factory=dict)
    adequacy_patch: dict[str, Any] = field(default_factory=dict)
