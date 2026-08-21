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
from typing import TYPE_CHECKING, Set

if TYPE_CHECKING:
    from antares.craft import ScenarioBuilder

import pandas as pd

from antares.craft.model.study import Study
from antares.datamanager.core.settings import settings
from antares.datamanager.logs.logging_setup import get_logger
from antares.datamanager.models.study_data_json_model import StudyData

logger = get_logger(__name__)


def generate_scenario_builder(study: Study, study_data: StudyData, used_files: Set[Path]) -> None:
    """
    Introduces scenario building configuration for the study.
    Executed as the last step of study generation.
    """
    modulo = study_data.scenario_builder_config.get("modulo", [])
    if not modulo:
        logger.info("No scenario builder configuration found in 'modulo'.")
        return

    sb = study.get_scenario_builder()

    scenarised_modulos = ["load", "hydro", "wind_onshore", "wind_offshore", "solar_pv", "solar_thermo"]
    if any(m in modulo for m in scenarised_modulos):
        _generate_scenarised_series(sb, study, study_data, modulo)

    # Save modification in study
    study.set_scenario_builder(sb)
    logger.info("Scenario builder configuration applied to the study.")


def _get_nb_ts(study_data: StudyData, category: str) -> int:
    """
    Helper to get nb_ts for a given category.
    """
    nb_ts = 0

    # Map category to StudyData field and directory
    mapping = {
        "load": (study_data.area_loads, settings.load_output_directory),
        "hydro": (study_data.area_hydro, settings.hydro_ts_directory),
        "wind_onshore": (study_data.area_res, settings.res_ts_directory),
        "wind_offshore": (study_data.area_res, settings.res_ts_directory),
        "solar_pv": (study_data.area_res, settings.res_ts_directory),
        "solar_thermo": (study_data.area_res, settings.res_ts_directory),
    }

    if category not in mapping:
        return 0

    data_dict, base_dir = mapping[category]
    if not data_dict:
        return 0

    # Determine areas to check: FR first, then others
    areas_to_check = list(data_dict.keys())
    if "FR" in areas_to_check:
        areas_to_check.remove("FR")
        areas_to_check.insert(0, "FR")

    for target_area in areas_to_check:
        area_data = data_dict.get(target_area)
        if area_data is None:
            continue

        files = []
        if category == "load":
            files = area_data if isinstance(area_data, list) else []
        elif category == "hydro":
            files = area_data.get("series", []) if isinstance(area_data, dict) else []
        elif category in ["wind_onshore", "wind_offshore", "solar_pv", "solar_thermo"]:
            # For RES, the structure can be either:
            # 1. Directly by technology: {"wind_onshore": {"series": [...]}, ...}
            # 2. Or grouped by cluster: {"clusters": {"c1": {"properties": {"group": "..."}, "series": [...]}}}

            # Case 1: Directly by technology
            if isinstance(area_data, dict) and category in area_data and isinstance(area_data[category], dict):
                files = area_data[category].get("series", [])

            # Case 2: Grouped by cluster (if Case 1 didn't find files)
            if not files:
                clusters = area_data.get("clusters", {}) if isinstance(area_data, dict) else {}
                target_group = category.replace("_", "").lower()
                for cluster_info in clusters.values():
                    group = (
                        cluster_info.get("properties", {}).get("group", "").replace("_", "").replace(" ", "").lower()
                    )
                    if group == target_group:
                        files = cluster_info.get("series", [])
                        if files:
                            break

        if files:
            file_path = base_dir / files[0]
            if file_path.exists():
                try:
                    df = pd.read_feather(file_path)
                    nb_ts = df.shape[1]
                    if nb_ts > 0:
                        return nb_ts
                except Exception as e:
                    logger.error(f"Failed to read file {file_path} for category {category}: {e}")

    return 0


def _generate_scenarised_series(sb: "ScenarioBuilder", study: Study, study_data: StudyData, modulo: list[str]) -> None:
    """
    Generate a scenario series and apply it to requested modulos after validation.
    """
    areas = study.get_areas()
    if not areas:
        logger.warning("No areas found in study to apply scenario.")
        return

    # area_id mapping to area objects
    area_mapping = dict(areas)

    expected_nb_ts = 0
    scenarised_modulos = ["load", "hydro", "wind_onshore", "wind_offshore", "solar_pv", "solar_thermo"]

    # 1. Determine expected_nb_ts from the first available modulo in the study
    # Prioritize 'load' if it exists.
    expected_nb_ts = _get_nb_ts(study_data, "load")
    if expected_nb_ts > 0:
        logger.info(f"Reference nb_ts determined from load: {expected_nb_ts}")
    else:
        for m in scenarised_modulos:
            if m == "load":
                continue
            nb_ts = _get_nb_ts(study_data, m)
            if nb_ts > 0:
                expected_nb_ts = nb_ts
                logger.info(f"Reference nb_ts determined from {m}: {expected_nb_ts}")
                break

    if expected_nb_ts == 0:
        logger.warning("Could not determine number of TS for any modulo. Using default value 1.")
        expected_nb_ts = 1

    # 2. Validate all other requested modulos
    for m in modulo:
        if m in scenarised_modulos:
            nb_ts = _get_nb_ts(study_data, m)
            if nb_ts > 0 and nb_ts != expected_nb_ts:
                msg = (
                    f"Timeseries must have the same number of columns for load, hydro, wind_onshore, "
                    f"wind_offshore, solar_pv, solar_thermo. Found {nb_ts} for {m} but expected {expected_nb_ts}. "
                    "Ensure all timeseries files have the same number of columns (number of climatic years)."
                )
                logger.error(msg)
                raise ValueError(msg)

    nb_years = study_data.nb_years

    # modulo calculation (1 to nb_ts repeated for nb_years)
    repeat_count = (nb_years // expected_nb_ts) + 1
    scenario_series: list[int | None] = [val for val in (list(range(1, expected_nb_ts + 1)) * repeat_count)[:nb_years]]

    logger.info(f"Applying scenario series of length {len(scenario_series)} (nb_ts={expected_nb_ts}) to all areas.")

    # Apply to all zones
    for area_id, area_obj in area_mapping.items():
        if "load" in modulo:
            sb.load.get_area(area_id).set_new_scenario(scenario_series)
        if "hydro" in modulo:
            sb.hydro.get_area(area_id).set_new_scenario(scenario_series)

        # Renewable clusters (wind_onshore, wind_offshore, solar_pv, solar_thermo)
        # mapped to sb.renewable which is a ScenarioCluster
        res_modulos = ["wind_onshore", "wind_offshore", "solar_pv", "solar_thermo"]
        requested_res = [m for m in modulo if m in res_modulos]

        if requested_res:
            renewables = area_obj.get_renewables()
            for cluster_id, cluster_obj in renewables.items():
                group = cluster_obj.properties.group.replace("_", "").replace(" ", "").lower()
                for res_m in requested_res:
                    target_group = res_m.replace("_", "").lower()
                    if group == target_group:
                        sb.renewable.get_cluster(area_id, cluster_id).set_new_scenario(scenario_series)
                        break
