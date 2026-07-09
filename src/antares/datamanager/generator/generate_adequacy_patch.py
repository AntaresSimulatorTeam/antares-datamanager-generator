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
import dataclasses

from typing import Any

from antares.craft import StudySettingsUpdate
from antares.craft.model.settings.adequacy_patch import (
    AdequacyPatchParametersUpdate,
    PriceTakingOrder,
)
from antares.craft.model.study import Study


def generate_adequacy_patch(study: Study, adequacy_patch_def: dict[str, Any]) -> None:
    """
    Generate adequacy patch settings for the study.
    """
    if not adequacy_patch_def:
        return

    # Filter adequacy_patch_def to only include fields present in AdequacyPatchParametersUpdate
    valid_fields = {f.name for f in dataclasses.fields(AdequacyPatchParametersUpdate)}
    filtered_def = {k: v for k, v in adequacy_patch_def.items() if k in valid_fields}

    # Handle price_taking_order mapping
    if "price_taking_order" in filtered_def:
        order = filtered_def["price_taking_order"]
        if isinstance(order, str):
            if order.upper() == "LOAD":
                filtered_def["price_taking_order"] = PriceTakingOrder.LOAD
            elif order.upper() == "DENS":
                filtered_def["price_taking_order"] = PriceTakingOrder.DENS

    # Filter out None values to avoid InvalidFieldForVersionError
    filtered_def = {k: v for k, v in filtered_def.items() if v is not None}

    if not filtered_def:
        return

    update_params = AdequacyPatchParametersUpdate(**filtered_def)

    study_settings = StudySettingsUpdate(adequacy_patch_parameters=update_params)
    study.update_settings(study_settings)
