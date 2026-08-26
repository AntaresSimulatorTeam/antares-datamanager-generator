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
from types import MappingProxyType
from typing import TYPE_CHECKING, Set

if TYPE_CHECKING:
    from antares.craft import ScenarioBuilder

import pandas as pd

from antares.craft.model.study import Study
from antares.craft.tools.contents_tool import transform_name_to_id
from antares.datamanager.core.settings import settings
from antares.datamanager.generator.generate_link_matrices import generate_link_capacity_df
from antares.datamanager.logs.logging_setup import get_logger
from antares.datamanager.models.study_data_json_model import StudyData

logger = get_logger(__name__)

_UNLINKED_SERIES_PLACEHOLDER = "matrix hash"


def generate_scenario_builder(study: Study, study_data: StudyData, used_files: Set[Path]) -> None:
    """
    Introduces scenario building configuration for the study.
    Executed as the last step of study generation.
    """
    climatic_data = study_data.scenario_builder_config.get("Climatic data", [])
    thermal_data = study_data.scenario_builder_config.get("Thermal", [])
    links_data = []
    for k, v in study_data.scenario_builder_config.items():
        if k.lower() == "links" and isinstance(v, list):
            links_data = v
            break

    if not climatic_data and not thermal_data and not links_data:
        logger.info("No scenario builder configuration found in 'Climatic data', 'Thermal', or 'Links'.")
        return

    sb = study.get_scenario_builder()

    scenarised_modulos = ["load", "hydro", "wind_onshore", "wind_offshore", "solar_pv", "solar_thermo"]
    if any(m in climatic_data for m in scenarised_modulos):
        _generate_scenarised_climatic_data_series(sb, study, study_data, climatic_data)

    if thermal_data:
        _generate_scenarised_thermal_series(sb, study, study_data, thermal_data)

    if links_data:
        _generate_scenarised_links_series(sb, study, study_data, links_data)

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


def _generate_scenarised_climatic_data_series(
    sb: "ScenarioBuilder", study: Study, study_data: StudyData, climatic_data: list[str]
) -> None:
    """
    Generate a scenario series and apply it to requested climatic data after validation.
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
    for m in climatic_data:
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
        if "load" in climatic_data:
            sb.load.get_area(area_id).set_new_scenario(scenario_series)
        if "hydro" in climatic_data:
            sb.hydro.get_area(area_id).set_new_scenario(scenario_series)

        # Renewable clusters (wind_onshore, wind_offshore, solar_pv, solar_thermo)
        # mapped to sb.renewable which is a ScenarioCluster
        res_modulos = ["wind_onshore", "wind_offshore", "solar_pv", "solar_thermo"]
        requested_res = [m for m in climatic_data if m in res_modulos]

        if requested_res:
            renewables = area_obj.get_renewables()
            for cluster_id, cluster_obj in renewables.items():
                group = cluster_obj.properties.group.replace("_", "").replace(" ", "").lower()
                for res_m in requested_res:
                    target_group = res_m.replace("_", "").lower()
                    if group == target_group:
                        sb.renewable.get_cluster(area_id, cluster_id).set_new_scenario(scenario_series)
                        break


def _generate_scenarised_thermal_series(
    sb: "ScenarioBuilder", study: Study, study_data: StudyData, thermal_data: list[str]
) -> None:
    """
    Generate scenario series for thermal configuration and apply to binding constraints / thermal clusters.
    """
    # 1. Verification of thermal items
    if "z_p2g_asservi" in thermal_data:
        # TODO: Handle z_p2g_asservi scenario generation
        logger.info("Handling for 'z_p2g_asservi' in Thermal scenario builder is not yet implemented.")

    if "nucleary_nuc_modulation" in thermal_data:
        # TODO: Handle nucleary_nuc_modulation scenario generation
        logger.info("Handling for 'nucleary_nuc_modulation' in Thermal scenario builder is not yet implemented.")

    if "nuclearfr" in thermal_data:
        _generate_nuclear_fr_scenario(sb, study, study_data)


def _generate_nuclear_fr_scenario(sb: "ScenarioBuilder", study: Study, study_data: StudyData) -> None:
    """
    Generate scenario for nuclear modulation binding constraints and nuclear thermal clusters for area FR.
    """
    # 1.Generate scenario for nuclear modulation binding constraints if defined
    _generate_nuclear_modulation_binding_constraints_scenario(sb, study_data)

    # 2.Generate scenario for thermal clusters belonging to group 'nuclear' for area FR
    _generate_nuclear_fr_thermal_clusters_scenario(sb, study, study_data)


def _generate_nuclear_modulation_binding_constraints_scenario(sb: "ScenarioBuilder", study_data: StudyData) -> None:
    """
    Scenarize nuclear modulation binding constraints (Nuc_modulation_limit, Nuc_modulation_daily, Nuc_modulation_weekly).
    """
    nuclear_modulation = study_data.nuclear_modulation_binding_constraints
    if not nuclear_modulation:
        logger.warning("No nuclear modulation binding constraints configuration found in study_data.")
        return

    constraints = nuclear_modulation.get("constraints", [])
    base_dir = settings.nuclear_modulation_ts_directory

    target_constraint_names = {"nuc_modulation_limit", "nuc_modulation_daily", "nuc_modulation_weekly"}
    expected_nb_ts = 0

    for constraint in constraints:
        name = constraint.get("name", "")
        if name.lower() not in target_constraint_names:
            continue

        series_file = constraint.get("series")
        if not series_file:
            continue

        file_path = base_dir / series_file
        if file_path.exists():
            try:
                df = pd.read_feather(file_path)
                nb_ts = df.shape[1]
                if expected_nb_ts == 0:
                    expected_nb_ts = nb_ts
                    logger.info(f"Reference nb_ts determined from {name}: {expected_nb_ts}")
                elif nb_ts != expected_nb_ts:
                    msg = (
                        f"Timeseries must have the same number of columns for nuclear modulation constraints. "
                        f"Found {nb_ts} for {name} but expected {expected_nb_ts}."
                    )
                    logger.error(msg)
                    raise ValueError(msg)
            except Exception as e:
                if isinstance(e, ValueError):
                    raise
                logger.error(f"Failed to read file {file_path} for constraint {name}: {e}")

    if expected_nb_ts == 0:
        expected_nb_ts = nuclear_modulation.get("nbTsColumns", 0)
        if expected_nb_ts == 0:
            logger.warning(
                "Could not determine number of TS for nuclear modulation constraints. Using default value 1."
            )
            expected_nb_ts = 1

    nb_years = study_data.nb_years
    repeat_count = (nb_years // expected_nb_ts) + 1
    scenario_series: list[int | None] = [val for val in (list(range(1, expected_nb_ts + 1)) * repeat_count)[:nb_years]]

    group = nuclear_modulation.get("group")
    if group:
        group_str = str(group)
        logger.info(
            f"Applying nuclear modulation scenario series of length {len(scenario_series)} "
            f"(nb_ts={expected_nb_ts}) to constraint group '{group_str}'."
        )
        sb.binding_constraint.get_group(group_str).set_new_scenario(scenario_series)
    else:
        logger.warning("No group defined in nuclear_modulation_binding_constraints.")


def _generate_nuclear_fr_thermal_clusters_scenario(sb: "ScenarioBuilder", study: Study, study_data: StudyData) -> None:
    """
    Generate scenario for all thermal clusters of group 'nuclear' for the area FR.
    The nb_ts for each cluster is the number of columns of its availability timeseries.
    """
    areas = study.get_areas()
    if not areas:
        logger.warning("No areas found in study to apply nuclear FR scenario.")
        return

    fr_area_id = None
    fr_area = None
    for area_id, area_obj in areas.items():
        if str(area_id).lower() == "fr" or getattr(area_obj, "name", "").lower() == "fr":
            fr_area_id = str(area_id)
            fr_area = area_obj
            break

    if not fr_area or not fr_area_id:
        logger.warning("FR area not found in study to apply nuclear FR scenario.")
        return

    if not hasattr(fr_area, "get_thermals"):
        return

    thermals = fr_area.get_thermals()
    if not thermals:
        return

    base_dir = settings.nuclear_availability_ts_directory
    fr_nuclear_data = {}
    for k, v in study_data.area_nuclear.items():
        if str(k).lower() == "fr":
            fr_nuclear_data = v
            break
    nuclear_clusters_data = fr_nuclear_data.get("clusters", {}) if isinstance(fr_nuclear_data, dict) else {}

    fr_thermals_data = {}
    for k, v in study_data.area_thermals.items():
        if str(k).lower() == "fr":
            fr_thermals_data = v
            break
    thermals_clusters_data = fr_thermals_data if isinstance(fr_thermals_data, dict) else {}

    for cluster_id, cluster_obj in thermals.items():
        group = ""
        props = getattr(cluster_obj, "properties", None)
        if props is not None and hasattr(props, "group") and props.group:
            group = str(props.group).replace("_", "").replace(" ", "").lower()

        if group != "nuclear":
            continue

        nb_ts = 0

        # 1. Look up cluster config in study_data.area_nuclear or study_data.area_thermals
        cluster_info = None
        if cluster_id in nuclear_clusters_data:
            cluster_info = nuclear_clusters_data[cluster_id]
        elif getattr(cluster_obj, "name", None) in nuclear_clusters_data:
            cluster_info = nuclear_clusters_data[cluster_obj.name]
        else:
            for name_key, val in nuclear_clusters_data.items():
                if transform_name_to_id(name_key) == cluster_id:
                    cluster_info = val
                    break

        if not cluster_info:
            if cluster_id in thermals_clusters_data:
                cluster_info = thermals_clusters_data[cluster_id]
            elif getattr(cluster_obj, "name", None) in thermals_clusters_data:
                cluster_info = thermals_clusters_data[cluster_obj.name]
            else:
                for name_key, val in thermals_clusters_data.items():
                    if transform_name_to_id(name_key) == cluster_id:
                        cluster_info = val
                        break

        if cluster_info and isinstance(cluster_info, dict):
            series_file = cluster_info.get("series")
            if series_file and series_file != _UNLINKED_SERIES_PLACEHOLDER:
                file_path = base_dir / series_file
                if file_path.exists():
                    try:
                        df = pd.read_feather(file_path)
                        nb_ts = df.shape[1]
                    except Exception as e:
                        logger.error(f"Failed to read file {file_path} for nuclear cluster {cluster_id}: {e}")

        # 2. Fallback to get_series_matrix from cluster_obj if available
        if nb_ts == 0 and hasattr(cluster_obj, "get_series_matrix"):
            try:
                matrix = cluster_obj.get_series_matrix()
                if matrix is not None and hasattr(matrix, "shape") and len(matrix.shape) > 1 and matrix.shape[1] > 0:
                    nb_ts = matrix.shape[1]
            except Exception as e:
                logger.debug(f"Could not get series matrix from cluster {cluster_id}: {e}")

        if nb_ts == 0:
            logger.warning(
                f"Could not determine number of TS for nuclear cluster '{cluster_id}'. Using default value 1."
            )
            nb_ts = 1

        nb_years = study_data.nb_years
        repeat_count = (nb_years // nb_ts) + 1
        scenario_series: list[int | None] = [val for val in (list(range(1, nb_ts + 1)) * repeat_count)[:nb_years]]

        logger.info(
            f"Applying nuclear thermal scenario series of length {len(scenario_series)} "
            f"(nb_ts={nb_ts}) to FR cluster '{cluster_id}'."
        )
        sb.thermal.get_cluster(fr_area_id, cluster_id).set_new_scenario(scenario_series)


def _normalize_link_id(link_str: str) -> tuple[str, str, str]:
    """
    Returns (normalized_link_id, area_from_id, area_to_id).
    Normalized link ID is in the format 'area_from / area_to' (areas sorted alphabetically).
    """
    if "/" in link_str:
        parts = [p.strip() for p in link_str.split("/", 1)]
        area_1 = transform_name_to_id(parts[0])
        area_2 = transform_name_to_id(parts[1])
        area_from, area_to = sorted([area_1, area_2])
        return f"{area_from} / {area_to}", area_from, area_to
    clean_id = transform_name_to_id(link_str.strip())
    return clean_id, clean_id, clean_id


def _generate_scenarised_links_series(
    sb: "ScenarioBuilder", study: Study, study_data: StudyData, links_data: list[str]
) -> None:
    """
    Generate scenario series for configured links based on their capacity timeseries.
    """
    study_links = study.get_links() if hasattr(study, "get_links") else {}

    for link_name in links_data:
        link_id, area_from, area_to = _normalize_link_id(link_name)

        # 1. Look up link object in study
        link_obj = None
        if isinstance(study_links, (dict, MappingProxyType)):
            link_obj = study_links.get(link_id)
            if not link_obj:
                for k, obj in study_links.items():
                    if _normalize_link_id(str(k))[0] == link_id:
                        link_obj = obj
                        break

        nb_ts = 0

        # Try to get capacity timeseries from study link object
        if link_obj is not None and hasattr(link_obj, "get_capacity_direct"):
            try:
                df = link_obj.get_capacity_direct()
                if df is not None and hasattr(df, "shape") and len(df.shape) > 1 and df.shape[1] > 0:
                    nb_ts = df.shape[1]
            except Exception as e:
                logger.debug(f"Could not get capacity direct from study link {link_id}: {e}")

        # 2. If not found, look up in study_data.links and generate capacity DataFrame
        if nb_ts == 0 and study_data.links:
            link_data = None
            for k, v in study_data.links.items():
                if _normalize_link_id(str(k))[0] == link_id:
                    link_data = v
                    break

            if link_data is not None and isinstance(link_data, dict):
                try:
                    df_cap = generate_link_capacity_df(
                        link_data,
                        "direct",
                        seed_tsgen_link=study_data.seed_tsgen_link,
                        link_name=f"{area_from}-{area_to}",
                    )
                    if (
                        df_cap is not None
                        and hasattr(df_cap, "shape")
                        and len(df_cap.shape) > 1
                        and df_cap.shape[1] > 0
                    ):
                        nb_ts = df_cap.shape[1]
                except Exception as e:
                    logger.error(f"Failed to generate link capacity for link {link_id}: {e}")

        # 3. Fallback to default
        if nb_ts == 0:
            logger.warning(f"Could not determine number of TS for link '{link_id}'. Using default value 1.")
            nb_ts = 1

        nb_years = study_data.nb_years
        repeat_count = (nb_years // nb_ts) + 1
        scenario_series: list[int | None] = [val for val in (list(range(1, nb_ts + 1)) * repeat_count)[:nb_years]]

        logger.info(
            f"Applying link scenario series of length {len(scenario_series)} (nb_ts={nb_ts}) to link '{link_id}'."
        )
        sb.link.get_link(link_id).set_new_scenario(scenario_series)
