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
from typing import TYPE_CHECKING, Any, Set

if TYPE_CHECKING:
    from antares.craft import Area, ScenarioBuilder

import pandas as pd

from antares.craft.model.study import Study
from antares.craft.tools.contents_tool import transform_name_to_id
from antares.datamanager.core.settings import settings
from antares.datamanager.generator.generate_link_matrices import generate_link_capacity_df
from antares.datamanager.logs.logging_setup import get_logger
from antares.datamanager.models.study_data_json_model import StudyData

logger = get_logger(__name__)

_UNLINKED_SERIES_PLACEHOLDER = "matrix hash"
EXCLUDED_LOAD_HYDRO_AREAS: Set[str] = {
    "y_nuc_modulation",
}


def _get_config_list(config: dict[str, Any], aliases: tuple[str, ...]) -> list[Any]:
    normalized_aliases = {a.lower().replace("_", " ").strip() for a in aliases}
    for k, v in config.items():
        if k.lower().replace("_", " ").strip() in normalized_aliases and isinstance(v, list):
            return v
    for a in aliases:
        raw_val = config.get(a)
        if isinstance(raw_val, list):
            return raw_val
    return []


def _build_scenario_series(nb_years: int, nb_ts: int) -> list[int | None]:
    if nb_ts <= 0:
        nb_ts = 1
    repeat_count = (nb_years // nb_ts) + 1
    return [val for val in (list(range(1, nb_ts + 1)) * repeat_count)[:nb_years]]


def _find_cluster_info(clusters_data: dict[str, Any], cluster_id: str, cluster_name: str | None = None) -> Any:
    if not isinstance(clusters_data, dict):
        return None
    if cluster_id in clusters_data:
        return clusters_data[cluster_id]
    if cluster_name and cluster_name in clusters_data:
        return clusters_data[cluster_name]
    clean_id = transform_name_to_id(cluster_id)
    for name_key, val in clusters_data.items():
        name_key_str = str(name_key)
        if (
            name_key_str == cluster_id
            or transform_name_to_id(name_key_str) == cluster_id
            or (clean_id and transform_name_to_id(name_key_str) == clean_id)
        ):
            return val
    return None


def _get_area_sts_data(study_data: StudyData, area_id_str: str, area_name_str: str) -> dict[str, Any]:
    for k, v in study_data.area_sts.items():
        if str(k).lower() in (area_id_str.lower(), area_name_str.lower()) or (
            transform_name_to_id(str(k)) in (transform_name_to_id(area_id_str), transform_name_to_id(area_name_str))
        ):
            if isinstance(v, dict):
                return v
    return {}


def _get_sts_cluster_group_and_info(
    storage_obj: Any,
    storage_id_str: str,
    storage_name_str: str,
    area_sts_data: dict[str, Any],
) -> tuple[Any, dict[str, Any] | None]:
    group_val = None
    props = getattr(storage_obj, "properties", None)
    if props is not None and hasattr(props, "group") and props.group:
        group_val = props.group.value if hasattr(props.group, "value") else props.group

    cluster_info = _find_cluster_info(area_sts_data, storage_id_str, storage_name_str)

    if not group_val and cluster_info and isinstance(cluster_info, dict):
        cluster_props = cluster_info.get("properties", {})
        if isinstance(cluster_props, dict) and cluster_props.get("group"):
            raw_grp = cluster_props["group"]
            group_val = raw_grp.value if hasattr(raw_grp, "value") else raw_grp

    return group_val, cluster_info


def _strip_zone_prefix_suffix(name_str: str, clean_zone: str) -> str:
    name_lower = name_str.lower()
    if name_lower.startswith(clean_zone + "_"):
        return name_str[len(clean_zone) + 1 :]
    if name_lower.startswith(clean_zone):
        return name_str[len(clean_zone) :]
    if name_lower.endswith("_" + clean_zone):
        return name_str[: -len(clean_zone) - 1]
    if name_lower.endswith(clean_zone):
        return name_str[: -len(clean_zone)]
    return name_str


def generate_scenario_builder(study: Study, study_data: StudyData, used_files: Set[Path]) -> None:
    """
    Introduces scenario building configuration for the study.
    Executed as the last step of study generation.
    """
    climatic_data_raw = _get_config_list(study_data.scenario_builder_config, ("climatic data", "Climatic data"))
    climatic_data = [
        str(item).replace("@", "").replace("*", "").strip() for item in climatic_data_raw if item is not None
    ]

    thermal_data_raw = _get_config_list(study_data.scenario_builder_config, ("thermal", "Thermal"))
    thermal_data = [str(item).strip() for item in thermal_data_raw if item is not None]

    links_data = _get_config_list(study_data.scenario_builder_config, ("links", "Links"))

    sts_inflows_data = _get_config_list(
        study_data.scenario_builder_config,
        ("sts inflows", "sts inflow", "stsinflows", "STS Inflows"),
    )

    sts_constraints_data = _get_config_list(
        study_data.scenario_builder_config,
        ("sts constraints", "sts constraint", "stsconstraints", "STS Constraints"),
    )

    if not climatic_data and not thermal_data and not links_data and not sts_inflows_data and not sts_constraints_data:
        logger.info(
            "No scenario builder configuration found in 'Climatic data', 'Thermal', 'Links', 'STS Inflows', or 'STS Constraints'."
        )
        return

    sb = study.get_scenario_builder()

    scenarised_modulos = ["load", "hydro", "wind_onshore", "wind_offshore", "solar_pv", "solar_thermo"]
    if any(m in climatic_data for m in scenarised_modulos):
        _generate_scenarised_climatic_data_series(sb, study, study_data, climatic_data)

    if thermal_data:
        _generate_scenarised_thermal_series(sb, study, study_data, thermal_data)

    if links_data:
        _generate_scenarised_links_series(sb, study, study_data, links_data)

    if sts_inflows_data:
        _generate_scenarised_sts_inflows_series(sb, study, study_data, sts_inflows_data)

    if sts_constraints_data:
        _generate_scenarised_sts_constraints_series(sb, study, study_data, sts_constraints_data)

    # Save modification in study
    study.set_scenario_builder(sb)
    logger.info("Scenario builder configuration applied to the study.")


def _normalize_res_group(group: str) -> str:
    """Normalize renewable group name for comparison."""
    normalized = str(group).replace("_", "").replace(" ", "").replace("-", "").lower()
    if normalized in ("solarthermal", "solarthermo"):
        return "solarthermo"
    return normalized


def _get_nb_ts(study: Study, study_data: StudyData, category: str) -> int:
    """
    Helper to get nb_ts for a given category.
    """
    supported_categories = [
        "load",
        "hydro",
        "wind_onshore",
        "wind_offshore",
        "solar_pv",
        "solar_thermo",
    ]
    if category not in supported_categories:
        return 0

    areas = study.get_areas()
    if not areas:
        return 0

    # Determine areas to check: FR first, then others
    areas_to_check: list[Area] = []
    for area_name, area_obj in areas.items():
        if str(area_name).lower() == "fr" or getattr(area_obj, "name", "").lower() == "fr":
            areas_to_check.insert(0, area_obj)
        else:
            areas_to_check.append(area_obj)

    for target_area in areas_to_check:
        if not target_area:
            continue
        try:
            if category == "load" and hasattr(target_area, "get_load_matrix"):
                matrix = target_area.get_load_matrix()
                if matrix is not None and hasattr(matrix, "shape") and len(matrix.shape) > 1 and matrix.shape[1] > 0:
                    return matrix.shape[1]

            elif category == "hydro" and getattr(target_area, "hydro", None) is not None:
                hydro_obj = target_area.hydro
                if hasattr(hydro_obj, "get_ror_series"):
                    matrix = hydro_obj.get_ror_series()
                    if (
                        matrix is not None
                        and hasattr(matrix, "shape")
                        and len(matrix.shape) > 1
                        and matrix.shape[1] > 0
                    ):
                        return matrix.shape[1]

            elif category in ["wind_onshore", "wind_offshore", "solar_pv", "solar_thermo"]:
                if hasattr(target_area, "get_renewables"):
                    renewables = target_area.get_renewables()
                    if renewables:
                        raw_area_id = (
                            getattr(target_area, "id", None) or getattr(target_area, "name", "") or str(area_name)
                        )
                        area_id = str(raw_area_id).lower()
                        for cluster_id, cluster_obj in renewables.items():
                            group = ""
                            if hasattr(cluster_obj, "properties") and cluster_obj.properties is not None:
                                raw_grp = getattr(cluster_obj.properties, "group", "")
                                group = str(raw_grp) if isinstance(raw_grp, str) else ""
                            elif hasattr(cluster_obj, "group"):
                                raw_grp = getattr(cluster_obj, "group", "")
                                group = str(raw_grp) if isinstance(raw_grp, str) else ""

                            if _normalize_res_group(group) == _normalize_res_group(category):
                                raw_cluster_id = getattr(cluster_obj, "id", None)
                                cluster_id_str = raw_cluster_id if isinstance(raw_cluster_id, str) else str(cluster_id)
                                raw_cluster_area = getattr(cluster_obj, "area_id", None)
                                cluster_area_id = raw_cluster_area if isinstance(raw_cluster_area, str) else area_id
                                matrix = None
                                renewable_service = getattr(cluster_obj, "_renewable_service", None) or getattr(
                                    target_area, "_renewable_service", None
                                )
                                if renewable_service is not None and hasattr(renewable_service, "get_renewable_matrix"):
                                    try:
                                        matrix = renewable_service.get_renewable_matrix(cluster_id_str, cluster_area_id)
                                    except Exception:
                                        pass

                                if matrix is None or not (
                                    hasattr(matrix, "shape") and len(matrix.shape) > 1 and matrix.shape[1] > 0
                                ):
                                    if hasattr(cluster_obj, "get_timeseries"):
                                        try:
                                            matrix = cluster_obj.get_timeseries()
                                        except Exception:
                                            pass

                                if matrix is None or not (
                                    hasattr(matrix, "shape") and len(matrix.shape) > 1 and matrix.shape[1] > 0
                                ):
                                    if hasattr(target_area, "get_renewable_matrix"):
                                        try:
                                            matrix = target_area.get_renewable_matrix(cluster_id_str, cluster_area_id)
                                        except Exception:
                                            pass
                                    elif hasattr(cluster_obj, "get_renewable_matrix"):
                                        try:
                                            matrix = cluster_obj.get_renewable_matrix(cluster_id_str, cluster_area_id)
                                        except Exception:
                                            pass

                                if (
                                    matrix is not None
                                    and hasattr(matrix, "shape")
                                    and len(matrix.shape) > 1
                                    and matrix.shape[1] > 0
                                ):
                                    return matrix.shape[1]
        except Exception as e:
            logger.error(f"Failed to get {category} matrix for area: {e}")

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

    scenarised_modulos = ["load", "hydro", "wind_onshore", "wind_offshore", "solar_pv", "solar_thermo"]

    # Pre-calculate / retrieve nb_ts for each scenarised modulo
    nb_ts_by_category: dict[str, int] = {m: _get_nb_ts(study, study_data, m) for m in scenarised_modulos}

    # 1. Determine expected_nb_ts from the first available modulo in the study
    # Prioritize 'load' if it exists.
    expected_nb_ts = nb_ts_by_category.get("load", 0)
    if expected_nb_ts > 1:
        logger.info(f"Reference nb_ts determined from load: {expected_nb_ts}")
    else:
        for m in scenarised_modulos:
            if m == "load":
                continue
            nb_ts = nb_ts_by_category.get(m, 0)
            if nb_ts > 1:
                expected_nb_ts = nb_ts
                logger.info(f"Reference nb_ts determined from {m}: {expected_nb_ts}")
                break

    if expected_nb_ts == 0:
        logger.warning("Could not determine number of TS for any modulo. Using default value 1.")
        expected_nb_ts = 1

    # 2. Validate all other requested modulos
    for m in climatic_data:
        if m in scenarised_modulos:
            nb_ts = nb_ts_by_category.get(m, 0)
            if nb_ts > 1 and nb_ts != expected_nb_ts:
                msg = (
                    f"Timeseries must have the same number of columns for load, hydro, wind_onshore, "
                    f"wind_offshore, solar_pv, solar_thermo. Found {nb_ts} for {m} but expected {expected_nb_ts}. "
                    "Ensure all timeseries files have the same number of columns (number of climatic years)."
                )
                logger.error(msg)
                raise ValueError(msg)

    # modulo calculation (1 to nb_ts repeated for nb_years)
    scenario_series = _build_scenario_series(study.get_settings().general_parameters.nb_years, expected_nb_ts)

    logger.info(f"Applying scenario series of length {len(scenario_series)} (nb_ts={expected_nb_ts}) to all areas.")

    # Apply to all zones (excluding specific zones for load and hydro)
    for area_id, area_obj in area_mapping.items():
        area_id_str = str(area_id).lower()
        area_name_val = getattr(area_obj, "name", None)
        area_name_str = area_name_val.lower() if isinstance(area_name_val, str) else ""
        is_excluded_load_hydro = (
            area_id_str in EXCLUDED_LOAD_HYDRO_AREAS
            or transform_name_to_id(area_id_str) in EXCLUDED_LOAD_HYDRO_AREAS
            or (
                bool(area_name_str)
                and (
                    area_name_str in EXCLUDED_LOAD_HYDRO_AREAS
                    or transform_name_to_id(area_name_str) in EXCLUDED_LOAD_HYDRO_AREAS
                )
            )
        )

        if not is_excluded_load_hydro:
            if "load" in climatic_data and nb_ts_by_category.get("load", 0) > 1:
                sb.load.get_area(area_id).set_new_scenario(scenario_series)
            if "hydro" in climatic_data and nb_ts_by_category.get("hydro", 0) > 1:
                sb.hydro.get_area(area_id).set_new_scenario(scenario_series)

        # Renewable clusters (wind_onshore, wind_offshore, solar_pv, solar_thermo)
        # mapped to sb.renewable which is a ScenarioCluster
        res_modulos = ["wind_onshore", "wind_offshore", "solar_pv", "solar_thermo"]
        requested_res = [m for m in climatic_data if m in res_modulos and nb_ts_by_category.get(m, 0) > 1]

        if requested_res:
            renewables = area_obj.get_renewables()
            for cluster_id, cluster_obj in renewables.items():
                raw_group = getattr(cluster_obj.properties, "group", "") if hasattr(cluster_obj, "properties") else ""
                for res_m in requested_res:
                    if _normalize_res_group(raw_group) == _normalize_res_group(res_m):
                        sb.renewable.get_cluster(area_id, cluster_id).set_new_scenario(scenario_series)
                        break


def _generate_scenarised_thermal_series(
    sb: "ScenarioBuilder", study: Study, study_data: StudyData, thermal_data: list[str]
) -> None:
    """
    Generate scenario series for thermal configuration and apply to binding constraints / thermal clusters.
    """
    normalized_thermal = set()
    for item in thermal_data:
        s = str(item).lower().strip()
        normalized_thermal.add(s)
        normalized_thermal.add(s.replace("@", "").replace("*", ""))
        normalized_thermal.add(s.replace("@", "_").replace("*", ""))
        normalized_thermal.add(s.replace("*", ""))

    # 1. Verification of thermal items
    if "z_p2g_asservi" in normalized_thermal or "*@z_p2g_asservi" in normalized_thermal:
        _generate_z_p2g_asservi_thermal_clusters_scenario(sb, study, study_data)

    if any(
        item
        in (
            "y_nuc_modulation",
            "nucleary_nuc_modulation",
            "nuclear_y_nuc_modulation",
            "nuclear@y_nuc_modulation",
            "ynucmodulation",
        )
        for item in normalized_thermal
    ):
        _generate_nuclear_thermal_clusters_scenario(sb, study, study_data, area_name="y_nuc_modulation")

    if any(item in ("nuclearfr", "nuclear_fr", "nuclear@fr") for item in normalized_thermal):
        _generate_nuclear_fr_scenario(sb, study, study_data)


def _generate_nuclear_fr_scenario(sb: "ScenarioBuilder", study: Study, study_data: StudyData) -> None:
    """
    Generate scenario for nuclear modulation binding constraints and nuclear thermal clusters for area FR.
    """
    # 1.Generate scenario for nuclear modulation binding constraints if defined
    _generate_nuclear_modulation_binding_constraints_scenario(
        sb, study_data, study.get_settings().general_parameters.nb_years
    )

    # 2.Generate scenario for thermal clusters belonging to group 'nuclear' for area FR
    _generate_nuclear_fr_thermal_clusters_scenario(sb, study, study_data)


def _generate_nuclear_modulation_binding_constraints_scenario(
    sb: "ScenarioBuilder", study_data: StudyData, nb_years: int
) -> None:
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

    scenario_series = _build_scenario_series(nb_years, expected_nb_ts)

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


def _generate_area_thermal_clusters_scenario(
    sb: "ScenarioBuilder",
    study: Study,
    study_data: StudyData,
    area_name: str,
    target_group: str | None = None,
) -> None:
    """
    Generate scenario for thermal clusters for the specified area.
    If target_group is specified, only clusters belonging to that group are processed.
    Otherwise, all thermal clusters in the area are processed.
    The nb_ts for each cluster is the number of columns of its availability timeseries.
    """
    group_label = f"{target_group} " if target_group else ""
    areas = study.get_areas()
    if not areas:
        logger.warning(f"No areas found in study to apply {group_label}{area_name} scenario.")
        return

    clean_target_name = transform_name_to_id(area_name)
    target_area_id = None
    target_area = None
    for area_id, area_obj in areas.items():
        area_id_str = str(area_id).lower()
        area_name_str = getattr(area_obj, "name", "").lower()
        if (
            area_id_str == clean_target_name
            or area_id_str == area_name.lower()
            or area_name_str == area_name.lower()
            or area_name_str == clean_target_name
        ):
            target_area_id = str(area_id)
            target_area = area_obj
            break

    if not target_area or not target_area_id:
        logger.warning(f"{area_name} area not found in study to apply {group_label}{area_name} scenario.")
        return

    if not hasattr(target_area, "get_thermals"):
        return

    thermals = target_area.get_thermals()
    if not thermals:
        return

    base_dir = settings.nuclear_availability_ts_directory
    area_nuclear_data = {}
    for k, v in study_data.area_nuclear.items():
        k_str = str(k).lower()
        if k_str == area_name.lower() or transform_name_to_id(k_str) == clean_target_name:
            area_nuclear_data = v
            break
    nuclear_clusters_data = area_nuclear_data.get("clusters", {}) if isinstance(area_nuclear_data, dict) else {}

    area_thermals_data = {}
    for k, v in study_data.area_thermals.items():
        k_str = str(k).lower()
        if k_str == area_name.lower() or transform_name_to_id(k_str) == clean_target_name:
            area_thermals_data = v
            break
    thermals_clusters_data = area_thermals_data if isinstance(area_thermals_data, dict) else {}

    for cluster_id, cluster_obj in thermals.items():
        if target_group is not None:
            group = ""
            props = getattr(cluster_obj, "properties", None)
            if props is not None and hasattr(props, "group") and props.group:
                group = str(props.group).replace("_", "").replace(" ", "").lower()

            if group != target_group.lower():
                continue

        nb_ts = 0

        # 1. Look up cluster config in study_data.area_nuclear or study_data.area_thermals
        cluster_name = getattr(cluster_obj, "name", None)
        cluster_info = _find_cluster_info(nuclear_clusters_data, cluster_id, cluster_name)
        if not cluster_info:
            cluster_info = _find_cluster_info(thermals_clusters_data, cluster_id, cluster_name)

        if cluster_info and isinstance(cluster_info, dict):
            series_file = cluster_info.get("series")
            if series_file and series_file != _UNLINKED_SERIES_PLACEHOLDER:
                file_path = base_dir / series_file
                if file_path.exists():
                    try:
                        df = pd.read_feather(file_path)
                        nb_ts = df.shape[1]
                    except Exception as e:
                        logger.error(f"Failed to read file {file_path} for thermal cluster {cluster_id}: {e}")

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
                f"Could not determine number of TS for thermal cluster '{cluster_id}'. Using default value 1."
            )
            nb_ts = 1

        scenario_series = _build_scenario_series(study.get_settings().general_parameters.nb_years, nb_ts)

        logger.info(
            f"Applying {group_label}thermal scenario series of length {len(scenario_series)} "
            f"(nb_ts={nb_ts}) to {area_name} cluster '{cluster_id}'."
        )
        sb.thermal.get_cluster(target_area_id, cluster_id).set_new_scenario(scenario_series)


def _generate_nuclear_thermal_clusters_scenario(
    sb: "ScenarioBuilder", study: Study, study_data: StudyData, area_name: str = "fr"
) -> None:
    """
    Generate scenario for all thermal clusters of group 'nuclear' for the specified area.
    The nb_ts for each cluster is the number of columns of its availability timeseries.
    """
    _generate_area_thermal_clusters_scenario(sb, study, study_data, area_name=area_name, target_group="nuclear")


def _generate_z_p2g_asservi_thermal_clusters_scenario(
    sb: "ScenarioBuilder", study: Study, study_data: StudyData, area_name: str = "z_p2g_asservi"
) -> None:
    """
    Generate scenario for all thermal clusters for the area z_p2g_asservi.
    The nb_ts for each cluster is the number of columns of its availability timeseries.
    """
    _generate_area_thermal_clusters_scenario(sb, study, study_data, area_name=area_name, target_group=None)


def _generate_nuclear_fr_thermal_clusters_scenario(sb: "ScenarioBuilder", study: Study, study_data: StudyData) -> None:
    """
    Generate scenario for all thermal clusters of group 'nuclear' for the area FR.
    The nb_ts for each cluster is the number of columns of its availability timeseries.
    """
    _generate_nuclear_thermal_clusters_scenario(sb, study, study_data, area_name="fr")


def _normalize_link_id(link_str: str) -> tuple[str, str, str]:
    """
    Returns (normalized_link_id, area_from_id, area_to_id).
    Normalized link ID is in the format 'area_from / area_to' (areas sorted alphabetically).
    """
    clean_str = link_str.replace("@", "").replace("*", "").strip()
    if "/" in clean_str:
        parts = [p.strip() for p in clean_str.split("/", 1)]
        area_1 = transform_name_to_id(parts[0])
        area_2 = transform_name_to_id(parts[1])
        area_from, area_to = sorted([area_1, area_2])
        return f"{area_from} / {area_to}", area_from, area_to
    clean_id = transform_name_to_id(clean_str)
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

        scenario_series = _build_scenario_series(study.get_settings().general_parameters.nb_years, nb_ts)

        logger.info(
            f"Applying link scenario series of length {len(scenario_series)} (nb_ts={nb_ts}) to link '{link_id}'."
        )
        sb.link.get_link(link_id).set_new_scenario(scenario_series)


def _match_sts_cluster_group(
    group_pattern: str,
    group_val: Any,
    storage_id_str: str,
    storage_name_str: str,
    clean_zone: str,
) -> bool:
    if group_pattern == "*":
        return True

    clean_target = group_pattern.replace("@", "").replace("*", "").replace("_", "").replace(" ", "").lower()
    trans_target = transform_name_to_id(group_pattern.lower())

    if not clean_target:
        return True

    # Representations of group_val
    clean_group_val = ""
    trans_group_val = ""
    if group_val is not None:
        raw_g = str(group_val.value if hasattr(group_val, "value") else group_val)
        clean_group_val = raw_g.replace("_", "").replace(" ", "").lower()
        trans_group_val = transform_name_to_id(raw_g.lower())

    # Representations of storage_id / name
    clean_sid = storage_id_str.replace("_", "").replace(" ", "").lower()
    trans_sid = transform_name_to_id(storage_id_str.lower())
    clean_sname = storage_name_str.replace("_", "").replace(" ", "").lower()
    trans_sname = transform_name_to_id(storage_name_str.lower())

    # Strip zone prefix / suffix
    sid_no_area = _strip_zone_prefix_suffix(storage_id_str, clean_zone)
    sname_no_area = _strip_zone_prefix_suffix(storage_name_str, clean_zone)

    clean_sid_no_area = sid_no_area.replace("_", "").replace(" ", "").lower()
    trans_sid_no_area = transform_name_to_id(sid_no_area.lower())
    clean_sname_no_area = sname_no_area.replace("_", "").replace(" ", "").lower()
    trans_sname_no_area = transform_name_to_id(sname_no_area.lower())

    valid_matches = {
        clean_sid,
        trans_sid,
        clean_sname,
        trans_sname,
        clean_sid_no_area,
        trans_sid_no_area,
        clean_sname_no_area,
        trans_sname_no_area,
    }
    if clean_group_val:
        valid_matches.add(clean_group_val)
        valid_matches.add(trans_group_val)

    # Direct match
    if clean_target in valid_matches or trans_target in valid_matches:
        return True

    # Prefix match (e.g. pondage matching pondage_2h, pondage_4h, pondage1, etc.)
    for candidate in valid_matches:
        if candidate.startswith(clean_target) or candidate.startswith(trans_target):
            return True

    # Specific PSP Open / PSP Closed checks when group is generic 'psp' or cluster is STEP
    # Target: psp_closed / pspclosed
    if clean_target in ("pspclosed", "psp_closed", "stepclosed", "step_closed", "ste_closed"):
        closed_indicators = ("closed", "ferme", "_c", "c_")
        is_psp = (
            clean_group_val in ("psp", "pspclosed", "step", "stepclosed")
            or "psp" in clean_sid
            or "step" in clean_sid
            or "psp" in clean_sname
            or "step" in clean_sname
        )
        has_closed = any(ind in clean_sid or ind in clean_sname or ind in trans_sid for ind in closed_indicators)
        has_open = any(
            ind in clean_sid or ind in clean_sname or ind in trans_sid for ind in ("open", "ouvert", "_o", "o_")
        )
        if is_psp and has_closed and not has_open:
            return True

    # Target: psp_open / pspopen
    if clean_target in ("pspopen", "psp_open", "stepopen", "step_open", "ste_open"):
        open_indicators = ("open", "ouvert", "_o", "o_")
        is_psp = (
            clean_group_val in ("psp", "pspopen", "step", "stepopen")
            or "psp" in clean_sid
            or "step" in clean_sid
            or "psp" in clean_sname
            or "step" in clean_sname
        )
        has_open = any(ind in clean_sid or ind in clean_sname or ind in trans_sid for ind in open_indicators)
        has_closed = any(
            ind in clean_sid or ind in clean_sname or ind in trans_sid for ind in ("closed", "ferme", "_c", "c_")
        )
        if is_psp and has_open and not has_closed:
            return True

    # Target: generic 'psp' or 'step'
    if clean_target in ("psp", "step"):
        if (
            clean_group_val in ("psp", "pspclosed", "pspopen", "step", "stepclosed", "stepopen")
            or "psp" in clean_sid
            or "step" in clean_sid
            or "psp" in clean_sname
            or "step" in clean_sname
        ):
            return True

    return False


def _generate_scenarised_sts_inflows_series(
    sb: "ScenarioBuilder", study: Study, study_data: StudyData, sts_inflows_data: list[str]
) -> None:
    """
    Generate scenario series for STS clusters matching configured groups based on their inflows timeseries.
    """
    storage_inflows = getattr(sb, "storage_inflows", None)
    if storage_inflows is None:
        logger.warning("ScenarioBuilder does not support storage_inflows (study version may be < 9.3).")
        return

    areas = study.get_areas()
    if not areas:
        logger.warning("No areas found in study to apply STS inflows scenario.")
        return

    # Parse inflow patterns
    parsed_patterns: list[tuple[str, str]] = []
    for item in sts_inflows_data:
        if not item:
            continue
        item_str = str(item.value if hasattr(item, "value") else item).strip()
        if not item_str:
            continue
        parts = [p.strip() for p in item_str.split("@") if p.strip()]
        if len(parts) == 1:
            parsed_patterns.append((parts[0], "*"))
        elif len(parts) >= 2:
            parsed_patterns.append((parts[0], parts[1]))

    if not parsed_patterns:
        return

    base_dir = settings.sts_ts_directory
    configured_clusters: set[tuple[str, str]] = set()

    for area_id, area_obj in areas.items():
        if not hasattr(area_obj, "get_st_storages"):
            continue

        storages = area_obj.get_st_storages()
        if not storages:
            continue

        area_id_str = str(getattr(area_obj, "id", area_id))
        area_name_str = str(getattr(area_obj, "name", area_id_str))

        # Look up area STS data in study_data
        area_sts_data = _get_area_sts_data(study_data, area_id_str, area_name_str)

        for storage_id, storage_obj in storages.items():
            storage_id_str = str(getattr(storage_obj, "id", storage_id))
            storage_name_str = str(getattr(storage_obj, "name", storage_id_str))

            if (area_id_str, storage_id_str) in configured_clusters:
                continue

            group_val, cluster_info = _get_sts_cluster_group_and_info(
                storage_obj, storage_id_str, storage_name_str, area_sts_data
            )

            # Check if this cluster matches any of the patterns
            matches = False
            for p1, p2 in parsed_patterns:
                for group_pattern, zone_pattern in [(p1, p2), (p2, p1)]:
                    if zone_pattern != "*":
                        clean_z = zone_pattern.lower()
                        trans_z = transform_name_to_id(zone_pattern.lower())
                        valid_areas = {
                            area_id_str.lower(),
                            area_name_str.lower(),
                            transform_name_to_id(area_id_str.lower()),
                            transform_name_to_id(area_name_str.lower()),
                        }
                        if clean_z not in valid_areas and trans_z not in valid_areas:
                            continue

                    if _match_sts_cluster_group(
                        group_pattern=group_pattern,
                        group_val=group_val,
                        storage_id_str=storage_id_str,
                        storage_name_str=storage_name_str,
                        clean_zone=area_id_str.lower(),
                    ):
                        matches = True
                        break
                if matches:
                    break

            if not matches:
                continue

            nb_ts = 0

            # 1. Try to get nb_ts directly from storage_obj inflows matrix in Antares study
            for method_name in ("get_storage_inflows", "get_inflows", "inflows"):
                if hasattr(storage_obj, method_name):
                    try:
                        attr_or_method = getattr(storage_obj, method_name)
                        matrix = attr_or_method() if callable(attr_or_method) else attr_or_method
                        if (
                            matrix is not None
                            and hasattr(matrix, "shape")
                            and len(matrix.shape) > 1
                            and matrix.shape[1] > 0
                        ):
                            nb_ts = matrix.shape[1]
                            break
                        elif (
                            matrix is not None
                            and hasattr(matrix, "shape")
                            and len(matrix.shape) == 1
                            and matrix.shape[0] > 0
                        ):
                            nb_ts = 1
                            break
                    except Exception as e:
                        logger.debug(
                            f"Could not get storage inflows from cluster {storage_id_str} via {method_name}: {e}"
                        )

            # 2. Fallback: try to get nb_ts from series file in study_data
            if nb_ts == 0 and cluster_info and isinstance(cluster_info, dict):
                raw_series = cluster_info.get("series", [])
                if isinstance(raw_series, dict):
                    raw_series = raw_series.get("series", [])
                series_list = (
                    [raw_series]
                    if isinstance(raw_series, str)
                    else (raw_series if isinstance(raw_series, list) else [])
                )

                inflows_filename = None
                for filename in series_list:
                    if isinstance(filename, str):
                        basename = Path(filename).name
                        if basename.lower().startswith("inflows.") or basename.split(".")[0].lower() == "inflows":
                            inflows_filename = filename
                            break

                if inflows_filename:
                    file_path = base_dir / inflows_filename
                    if file_path.exists():
                        try:
                            df = pd.read_feather(file_path)
                            nb_ts = df.shape[1]
                        except Exception as e:
                            logger.error(f"Failed to read file {file_path} for STS cluster {storage_id_str}: {e}")

            # 3. Fallback to default 1
            if nb_ts == 0:
                logger.warning(
                    f"Could not determine number of TS for STS cluster '{storage_id_str}' in area '{area_id_str}'. "
                    f"Using default value 1."
                )
                nb_ts = 1

            scenario_series = _build_scenario_series(study.get_settings().general_parameters.nb_years, nb_ts)

            logger.info(
                f"Applying STS inflows scenario series of length {len(scenario_series)} "
                f"(nb_ts={nb_ts}) to area '{area_id_str}' STS cluster '{storage_id_str}'."
            )
            storage_inflows.get_storage(area_id_str, storage_id_str).set_new_scenario(scenario_series)
            configured_clusters.add((area_id_str, storage_id_str))


def _generate_scenarised_sts_constraints_series(
    sb: "ScenarioBuilder", study: Study, study_data: StudyData, sts_constraints_data: list[str]
) -> None:
    """
    Generate scenario series for STS additional constraints matching configured pattern:
    e.g. 'zone@group@constraint_name' or 'AT@PSP@VE'.
    """
    storage_constraints = getattr(sb, "storage_constraints", None)
    if storage_constraints is None:
        logger.warning("ScenarioBuilder does not support storage_constraints (study version may be < 9.3).")
        return

    areas = study.get_areas()
    if not areas:
        logger.warning("No areas found in study to apply STS constraints scenario.")
        return

    base_dir = settings.sts_ts_directory

    for item in sts_constraints_data:
        if not item:
            continue
        item_str = str(item).strip()
        parts = [p.strip() for p in item_str.split("@") if p.strip()]
        if not parts:
            continue

        if len(parts) == 1:
            zone_pattern = "*"
            group_pattern = "*"
            constraint_pattern = parts[0]
        elif len(parts) == 2:
            zone_pattern = parts[0]
            group_pattern = "*"
            constraint_pattern = parts[1]
        else:
            zone_pattern = parts[0]
            group_pattern = parts[1]
            constraint_pattern = parts[2]

        clean_zone = zone_pattern.lower()
        clean_constraint = constraint_pattern.replace("_", "").replace(" ", "").lower()
        clean_constraint_id = transform_name_to_id(constraint_pattern.lower())

        for area_id, area_obj in areas.items():
            if not hasattr(area_obj, "get_st_storages"):
                continue

            area_id_str = str(getattr(area_obj, "id", area_id))
            area_name_str = str(getattr(area_obj, "name", area_id_str))

            # Match area
            if zone_pattern != "*":
                valid_area_names = {
                    area_id_str.lower(),
                    area_name_str.lower(),
                    transform_name_to_id(area_id_str.lower()),
                    transform_name_to_id(area_name_str.lower()),
                }
                if clean_zone not in valid_area_names and transform_name_to_id(clean_zone) not in valid_area_names:
                    continue

            storages = area_obj.get_st_storages()
            if not storages:
                continue

            area_sts_data = _get_area_sts_data(study_data, area_id_str, area_name_str)

            for storage_id, storage_obj in storages.items():
                storage_id_str = str(getattr(storage_obj, "id", storage_id))
                storage_name_str = str(getattr(storage_obj, "name", storage_id_str))

                group_val, cluster_info = _get_sts_cluster_group_and_info(
                    storage_obj, storage_id_str, storage_name_str, area_sts_data
                )

                # Match group
                if not _match_sts_cluster_group(
                    group_pattern=group_pattern,
                    group_val=group_val,
                    storage_id_str=storage_id_str,
                    storage_name_str=storage_name_str,
                    clean_zone=clean_zone,
                ):
                    continue

                # Find matching additional constraints on this storage
                matched_constraints: dict[str, str] = {}
                if hasattr(storage_obj, "get_constraints"):
                    for c_id, c_obj in storage_obj.get_constraints().items():
                        c_id_str = str(c_id)
                        c_name_str = str(getattr(c_obj, "name", c_id_str))
                        matched_constraints[c_id_str] = c_name_str

                # Also inspect constraintParameters in cluster_info
                if cluster_info and isinstance(cluster_info, dict):
                    raw_c_params = cluster_info.get("constraintParameters", {})
                    if isinstance(raw_c_params, dict):
                        for c_name in raw_c_params.keys():
                            c_name_str = str(c_name)
                            if c_name_str not in matched_constraints:
                                matched_constraints[c_name_str] = c_name_str

                for c_id_str, c_name_str in matched_constraints.items():
                    clean_cid = c_id_str.replace("_", "").replace(" ", "").lower()
                    clean_cname = c_name_str.replace("_", "").replace(" ", "").lower()
                    trans_cid = transform_name_to_id(c_id_str.lower())
                    trans_cname = transform_name_to_id(c_name_str.lower())

                    if constraint_pattern != "*":
                        constraint_matches = (
                            clean_constraint == clean_cid
                            or clean_constraint == clean_cname
                            or clean_constraint_id == trans_cid
                            or clean_constraint_id == trans_cname
                            or clean_constraint_id == clean_cid
                            or clean_constraint_id == clean_cname
                            or clean_constraint == trans_cid
                            or clean_constraint == trans_cname
                        )
                        if not constraint_matches:
                            continue

                    # Determine nb_ts from series file
                    nb_ts = 0
                    if cluster_info and isinstance(cluster_info, dict):
                        raw_constraints_series = cluster_info.get("stsConstraintsSeriesList", [])
                        if not isinstance(raw_constraints_series, list):
                            raw_constraints_series = []

                        target_filename = None
                        for filename in raw_constraints_series:
                            if not isinstance(filename, str):
                                continue
                            fn_lower = Path(filename).name.lower()
                            # Check if filename corresponds to this constraint name
                            if (
                                fn_lower.startswith(f"{c_name_str.lower()}.")
                                or fn_lower.startswith(f"{c_id_str.lower()}.")
                                or fn_lower.startswith(f"{c_name_str.lower()}_")
                                or fn_lower.startswith(f"{c_id_str.lower()}_")
                                or c_name_str.lower() in fn_lower
                                or c_id_str.lower() in fn_lower
                            ):
                                target_filename = filename
                                break

                        if target_filename:
                            file_path = base_dir / target_filename
                            if file_path.exists():
                                try:
                                    df = pd.read_feather(file_path)
                                    nb_ts = df.shape[1]
                                except Exception as e:
                                    logger.error(
                                        f"Failed to read file {file_path} for STS constraint '{c_id_str}' in cluster '{storage_id_str}': {e}"
                                    )

                    # Try to get nb_ts from existing matrix on storage if available
                    if nb_ts == 0 and hasattr(storage_obj, "get_constraint_term"):
                        try:
                            term_matrix = storage_obj.get_constraint_term(c_id_str)
                            if (
                                term_matrix is not None
                                and hasattr(term_matrix, "shape")
                                and len(term_matrix.shape) > 1
                                and term_matrix.shape[1] > 0
                            ):
                                nb_ts = term_matrix.shape[1]
                            elif (
                                term_matrix is not None
                                and hasattr(term_matrix, "shape")
                                and len(term_matrix.shape) == 1
                                and term_matrix.shape[0] > 0
                            ):
                                nb_ts = 1
                        except Exception as e:
                            logger.debug(f"Could not get constraint term from storage {storage_id_str}: {e}")

                    # Fallback to 1
                    if nb_ts == 0:
                        logger.warning(
                            f"Could not determine number of TS for STS constraint '{c_id_str}' in cluster '{storage_id_str}' (area '{area_id_str}'). "
                            f"Using default value 1."
                        )
                        nb_ts = 1

                    scenario_series = _build_scenario_series(study.get_settings().general_parameters.nb_years, nb_ts)

                    logger.info(
                        f"Applying STS constraints scenario series of length {len(scenario_series)} "
                        f"(nb_ts={nb_ts}) to area '{area_id_str}' STS cluster '{storage_id_str}' constraint '{c_id_str}'."
                    )
                    storage_constraints.get_constraint(area_id_str, storage_id_str, c_id_str).set_new_scenario(
                        scenario_series
                    )
