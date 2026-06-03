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
from typing import Any, Optional, Set

import pandas as pd

from antares.craft import HydroAllocation, HydroPropertiesUpdate
from antares.datamanager.core.settings import settings
from antares.datamanager.logs.logging_setup import get_logger

logger = get_logger(__name__)

# Default value for maxpower series (columns 2 and 4 in the study file, but columns 1 and 3 here)
DEFAULT_MAXPOWER_VALUE = 24


def generate_hydro(area_obj: Any, hydro: dict[str, Any], used_files: Optional[Set[Path]] = None) -> None:
    if not hydro:
        return

    properties = hydro.get("properties", {})
    if isinstance(properties, list):
        properties = properties[0] if properties else {}

    series_list = hydro.get("series", [])

    # Update properties
    # Mapping intra_daily_modulation from input JSON's inter_daily_modulation
    properties_to_update = properties.copy()
    if "inter_daily_modulation" in properties:
        properties_to_update["intra_daily_modulation"] = properties_to_update.pop("inter_daily_modulation")

    # allocation and series are not fields in HydroPropertiesUpdate
    if "allocation" in properties_to_update:
        properties_to_update.pop("allocation")
    if "series" in properties_to_update:
        properties_to_update.pop("series")

    # HydroPropertiesUpdate can be instantiated with **properties_to_update
    hydro_props_update = HydroPropertiesUpdate(**properties_to_update)
    area_obj.hydro.update_properties(hydro_props_update)

    # Set allocation
    allocation_data = hydro.get("allocation")
    if not allocation_data:
        allocation_data = properties.get("allocation", {})

    if allocation_data:
        # Deduplicate allocation data by keeping the last occurrence of each area_id.
        # We lowercase the area_id to avoid duplicates due to casing differences.
        # We also exclude the current area itself because Antares automatically
        # includes it in the allocation list.
        current_area_id = area_obj.name.lower()
        unique_allocations = {}
        for target_area, coefficient in allocation_data.items():
            target_area_lower = target_area.lower()
            if target_area_lower != current_area_id:
                unique_allocations[target_area_lower] = HydroAllocation(
                    area_id=target_area_lower, coefficient=coefficient
                )

        area_obj.hydro.set_allocation(list(unique_allocations.values()))

    # Set series
    base_dir = _resolve_hydro_base_directory()
    for series_file in series_list:
        file_path = base_dir / series_file
        if used_files is not None:
            used_files.add(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"ERROR: file {file_path} doesn't exist")

        df = pd.read_feather(file_path)

        if "_mod" in series_file:
            area_obj.hydro.set_mod_series(df)
        elif "_ror" in series_file:
            area_obj.hydro.set_ror_series(df)
        elif "_mingen" in series_file:
            area_obj.hydro.set_mingen(df)
        elif "_reservoir" in series_file:
            area_obj.hydro.set_reservoir(df)
        elif "_maxpower" in series_file:
            # maxpower series should have 4 columns:
            # Col 0: Original data from arrow file
            # Col 1: DEFAULT_MAXPOWER_VALUE (24)
            # Col 2: 0 #TO MODIFY FOR PUMPING THE VALUE COMES from maxpower_df
            # Col 3: DEFAULT_MAXPOWER_VALUE (24)
            # We assume the input df has only 1 column. If it has more, we only use the first one.
            maxpower_df = pd.DataFrame()
            maxpower_df["0"] = df.iloc[:, 0]
            maxpower_df["1"] = DEFAULT_MAXPOWER_VALUE
            maxpower_df["2"] = 0
            maxpower_df["3"] = DEFAULT_MAXPOWER_VALUE
            area_obj.hydro.set_maxpower(maxpower_df)


def _resolve_hydro_base_directory() -> Path:
    return settings.hydro_ts_directory
