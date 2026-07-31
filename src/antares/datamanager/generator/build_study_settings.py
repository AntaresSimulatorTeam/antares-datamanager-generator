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

from antares.craft import (
    AdequacyPatchParametersUpdate,
    AdvancedParametersUpdate,
    GeneralParametersUpdate,
    OptimizationParametersUpdate,
    SeedParametersUpdate,
    StudySettingsUpdate,
)
from antares.craft.model.settings.adequacy_patch import PriceTakingOrder
from antares.datamanager.models.study_data_json_model import StudyData


def normalize_enum_values(optimization_settings: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize enum values in optimization settings.

    Converts string representations of boolean values to actual booleans for ExportMPS enum.
    Handles cases where JSON contains "false"/"true" (strings) that should be converted to False/True.

    Args:
        optimization_settings: Dictionary of optimization parameters from JSON.

    Returns:
        The same dictionary with normalized include_exportmps value.
    """
    if "include_exportmps" in optimization_settings:
        value = optimization_settings["include_exportmps"]
        if isinstance(value, str):
            if value.lower() == "false":
                optimization_settings["include_exportmps"] = False
            elif value.lower() == "true":
                optimization_settings["include_exportmps"] = True
    return optimization_settings
    
def normalize_adequacy_enum_values(adequacy_settings: dict[str, Any]) -> dict[str, Any]:
    
    if "price_taking_order" in adequacy_settings:
        order = adequacy_settings["price_taking_order"]
        if isinstance(order, str):
            if order.upper() == "LOAD":
                adequacy_settings["price_taking_order"] = PriceTakingOrder.LOAD
            elif order.upper() == "DENS":
                adequacy_settings["price_taking_order"] = PriceTakingOrder.DENS  
    return adequacy_settings

def build_study_settings(settings_dict: dict[str, Any], study_data: StudyData) -> StudySettingsUpdate:
    """
    Build StudySettingsUpdate from settings dictionary using Pydantic validation.

    Pydantic handles missing fields with defaults, so we only pass non-None values.

    Args:
        settings_dict: Dictionary containing study settings (general, optimization, advanced, seed parameters).
        study_data: Study data object containing default values for general parameters.

    Returns:
        StudySettingsUpdate object with all parameter categories set.
    """
    general_params: GeneralParametersUpdate | None = None
    optimization_params: OptimizationParametersUpdate | None = None
    advanced_params: AdvancedParametersUpdate | None = None
    seed_params: SeedParametersUpdate | None = None
    adequacy_patch_params: AdequacyPatchParametersUpdate | None = None

    if settings_dict:
        # Extract and filter each parameter category (remove None values)
        general_settings = settings_dict.get("general_parameters", {})
        if general_settings:
            filtered_general = {k: v for k, v in general_settings.items() if v is not None}
            if filtered_general:
                general_params = GeneralParametersUpdate(**filtered_general)

        optimization_settings = settings_dict.get("optimization_parameters", {})
        if optimization_settings:
            filtered_optimization = {k: v for k, v in optimization_settings.items() if v is not None}
            if filtered_optimization:
                filtered_optimization = normalize_enum_values(filtered_optimization)
                optimization_params = OptimizationParametersUpdate(**filtered_optimization)

        advanced_settings = settings_dict.get("advanced_parameters", {})
        if advanced_settings:
            filtered_advanced = {k: v for k, v in advanced_settings.items() if v is not None}
            if filtered_advanced:
                advanced_params = AdvancedParametersUpdate(**filtered_advanced)

        seed_settings = settings_dict.get("seeds_parameters", {})
        if seed_settings:
            filtered_seeds = {k: v for k, v in seed_settings.items() if v is not None}
            if filtered_seeds:
                seed_params = SeedParametersUpdate(**filtered_seeds)

        adequacy_patch_settings = settings_dict.get("adequacy", {})
        if adequacy_patch_settings:
            valid_fields = {f.name for f in dataclasses.fields(AdequacyPatchParametersUpdate)}
            filtered_adequacy_patch = {k: v for k, v in adequacy_patch_settings.items() if k in valid_fields}
            if filtered_adequacy_patch:
                filtered_adequacy_patch = normalize_adequacy_enum_values(filtered_adequacy_patch)
                adequacy_patch_params = AdequacyPatchParametersUpdate(**filtered_adequacy_patch)        

    # Ensure required general parameters are set (only add if not already set)
    if general_params is None:
        general_params = GeneralParametersUpdate(
            nb_years=study_data.nb_years,
            first_month_in_year=study_data.first_month,
        )

    return StudySettingsUpdate(
        general_parameters=general_params,
        optimization_parameters=optimization_params,
        advanced_parameters=advanced_params,
        seed_parameters=seed_params,
        adequacy_patch_parameters=adequacy_patch_params,
    )
