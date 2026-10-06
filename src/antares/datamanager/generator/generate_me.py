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
from __future__ import annotations

from pathlib import Path
from typing import Any, Set

import numpy as np
import pandas as pd

from antares.craft import (
    BindingConstraintFrequency,
    BindingConstraintOperator,
    BindingConstraintProperties,
    ClusterData,
    ConstraintTerm,
    HydroPropertiesUpdate,
    InflowStructureUpdate,
    LinkData,
    LinkProperties,
    LinkPropertiesUpdate,
    TransmissionCapacities,
)
from antares.craft.model.area import Area, AreaProperties, AreaUi
from antares.craft.model.study import Study
from antares.datamanager.core.settings import settings
from antares.datamanager.exceptions.exceptions import MEGenerationError
from antares.datamanager.generator.generate_hydro import set_hydro_allocation
from antares.datamanager.generator.generate_link_matrices import generate_constant_link_capacity_df
from antares.datamanager.generator.generate_sts_clusters import generate_sts_clusters
from antares.datamanager.generator.generate_thermal_clusters import generate_thermal_me_clusters
from antares.datamanager.logs.logging_setup import get_logger
from antares.datamanager.utils.area_ui_utils import generate_random_color, generate_random_coordinate

logger = get_logger(__name__)

EXPECTED_HOURS = 8760
EXPECTED_DAYS = 365

# Expected JSON (root level "ME" key)
#
# "ME": {
#   "area_me": {
#     "V_ME_H2_LONG_FR": {
#       "properties": {"energy_cost_unsupplied": 5376, "energy_cost_spilled": 0, "adequacy_patch_mode": "inside"},
#       "ui": "AreaUI class as JSON",   # placeholder (ignored)
#       "loads": ["load_v_me_h2_long_fr_2026-2027.csv.<UID>.arrow"]
#       # "No LOAD files for this area" if no load files
#       "sts_me": {
#         "V_ME_H2_SHORT_FR_st_storage": {
#           "properties": {"enabled": true, "group": "other1", "injection_nominal_capacity": 245, ...},
#           "series": ["lower_curve.xlsx.<UID>.arrow", "Pmax_injection.xlsx.<UID>.arrow"]
#           # not all 4 series (lower_curve, Pmax_injection, Pmax_soutirage, upper_curve) are required
#         }
#       },
#         "thermal_me": {
#             "V_ME_H2_LONG_EUEST_IMPORTS CANALISATION": {
#                 "properties": {
#                     "enabled": true,
#                     "must_run": false,
#                     "nb_unit": 1,
#                     "nominal_capacity": 19350.0,
#                     "marginal_cost": 95.0,
#                     "market_bid_cost": 95.0,
#                     "group": "Other"
#                 },
#                 "modulation": {
#                     "marginal_cost_modulation": 1,
#                     "market_bid_cost_modulation": 1,
#                     "must_run_modulation": 1,
#                     "capacity_modulation": 1
#                 },
#                  "series":
#                   ["<nom_ficher_modulation_checksum>.arrow"] // must_run_modulation_FE_prod_h2_central_v1.xlsx.e4a2a6a8-9c5c-4948-b20e-a24d2d6f3bf2.arrow
#             },
#         }
#     }
#   },
#   "links_me": {
#     "v_me_h2_long_euest/v_me_h2_long_iber": {
#       "directMw": 12000, "indirectMw": 12000, "hurdleCostDirect": 0.1, "hurdleCostIndirect": 0.1
#     },
#     "FR/z_p2g_long_fr": {"directMw": 6280, "indirectMw": 0, "hurdleCostDirect": 0, "hurdleCostIndirect": 0}
#   },
#   "hydro_me": {
#     "v_me_h2_long_fr": {
#       "properties": {"reservoir_capacity": 2315000},
#       "inflow_structure": {"intermonthly_correlation": 0.5},
#       "generating_series": "generating_v_me_h2_long_fr.arrow",
#       "pumping_pmax": {"pmax": 100, "hours": 24},
#       "reservoir_ts": "v_me_h2_long_fr_reservoir_levels_<checksum>.arrow"
#       # 365 x 3 (Minimum, Moyen, Maximum) -> input/hydro/common/capacity/reservoir_{area_id}.txt
#       "water_values_ts": "v_me_h2_long_fr_<checksum>.arrow",
#       # 365 x 101 -> input/hydro/common/capacity/waterValues_{area_id}.txt
#       "timeseries_ts": {
#         "ror": "v_me_h2_long_fr_ror.<checksum>.arrow",  # 8760 x N -> input/hydro/series/{area_id}/ror.txt
#         "mod": "v_me_h2_long_fr_mod.<checksum>.arrow"   # 365 x N  -> input/hydro/series/{area_id}/mod.txt
#       }
#     }
#   },
#   "binding_constraints_me": {
#     "constraints_P2G": [
#       {"node": "z_p2g_long_fr", "efficiency": 0.744}
#       # one constraint per z_p2g_XX node in links_me
#       # links to a v_me_* node get weight 1.0 (ME <-> ME),
#       # links to anything else get weight = efficiency (ELEC <-> ME)
#     ],
#     "constraints_G2P": [
#       {
#         "name": "g2p_euest", "enabled": true, "type": "hourly", "operator": "equal",
#         "node_1_left": "v_me_h2_long_euest", "node_2_left": "z_me_consoelec",
#         "node_right_area": {"AT": {"CCGT H2 pcomp": {"efficiency": 51}}}
#       }
#     ]
#   }
# }


def _build_me_area_properties(area_def: dict[str, Any]) -> AreaProperties | None:
    properties_json = area_def.get("properties")
    if not isinstance(properties_json, dict):
        return None

    props = {}
    for key in ["energy_cost_unsupplied", "energy_cost_spilled", "adequacy_patch_mode"]:
        val = properties_json.get(key)
        if val is not None:
            props[key] = val

    return AreaProperties(**props)


def _build_me_area_ui(area_def: dict[str, Any]) -> AreaUi:
    # random position and color
    ui_json = area_def.get("ui")
    if isinstance(ui_json, dict):
        try:
            return AreaUi(**ui_json)
        except Exception:
            pass

    x, y = generate_random_coordinate()
    color_rgb = generate_random_color()
    return AreaUi(x=x, y=y, color_rgb=color_rgb)


def _set_me_area_loads(area_obj: Area, loads: list[str], load_directory: Path, used_files: Set[Path]) -> None:
    for load_file in loads:
        load_path = load_directory / load_file
        df = pd.read_feather(load_path)
        area_obj.set_load(df)
        used_files.add(load_path)


def add_me_areas_to_study(study: Study, area_me: dict[str, Any], used_files: Set[Path]) -> dict[str, Area]:
    load_directory = settings.load_output_directory
    area_objs: dict[str, Area] = {}
    for area_name, area_def in area_me.items():
        properties = _build_me_area_properties(area_def)
        ui = _build_me_area_ui(area_def)
        loads = area_def.get("loads", [])
        loads = loads if isinstance(loads, list) else []
        thermals_me = area_def.get("thermals_me", {})
        try:
            area_obj = study.create_area(area_name=area_name, properties=properties, ui=ui)
            _set_me_area_loads(area_obj, loads, load_directory, used_files)
            if thermals_me:
                add_me_thermals_to_study(area_obj, thermals_me, used_files)
            area_objs[area_name] = area_obj
            logger.info(f"Created ME area {area_name}")
        except Exception as e:
            raise MEGenerationError(f"Could not create ME area {area_name}: {e}") from e
    return area_objs


def _constant_hurdle_cost_df(hurdle_cost_direct: float | None, hurdle_cost_indirect: float | None) -> pd.DataFrame:
    """
    8760 x 6 matrix:
    column 0 = direct hurdle cost,
    column 1 = indirect hurdle cost,
    columns 2-5 = 0
    """
    direct = float(hurdle_cost_direct) if hurdle_cost_direct is not None else 0.0
    indirect = float(hurdle_cost_indirect) if hurdle_cost_indirect is not None else 0.0

    data = np.zeros((EXPECTED_HOURS, 6), dtype=float)
    data[:, 0] = direct
    data[:, 1] = indirect
    return pd.DataFrame(data)


def add_me_links_to_study(study: Study, links_me: dict[str, Any]) -> None:
    for link_key, link_def in links_me.items():
        area_from, area_to = link_key.lower().split("/")
        direct_mw = link_def.get("directMw")
        indirect_mw = link_def.get("indirectMw")

        if (direct_mw is None) != (indirect_mw is None):
            raise MEGenerationError(
                f"ME link {area_from}/{area_to}: directMw and indirectMw must be both null (infinite) or both set"
            )
        is_infinite = direct_mw is None
        transmission_capacities = TransmissionCapacities.INFINITE if is_infinite else TransmissionCapacities.ENABLED

        try:
            link = study.create_link(
                area_from=area_from,
                area_to=area_to,
                properties=LinkProperties(transmission_capacities=transmission_capacities),
            )
            link.update_properties(LinkPropertiesUpdate(hurdles_cost=True))

            if not is_infinite:
                link.set_capacity_direct(generate_constant_link_capacity_df(direct_mw))
                link.set_capacity_indirect(generate_constant_link_capacity_df(indirect_mw))

            link.set_parameters(
                _constant_hurdle_cost_df(link_def.get("hurdleCostDirect"), link_def.get("hurdleCostIndirect"))
            )
            logger.info(f"Created ME link {area_from}/{area_to}")
        except MEGenerationError:
            raise
        except Exception as e:
            raise MEGenerationError(f"Could not create ME link {area_from}/{area_to}: {e}") from e


def add_me_sts_to_study(area_objs: dict[str, Area], area_me: dict[str, Any], used_files: Set[Path]) -> None:
    for area_name, area_def in area_me.items():
        sts_me = area_def.get("sts_me")
        if not isinstance(sts_me, dict) or not sts_me:
            continue

        area_obj = area_objs.get(area_name)
        if area_obj is None:
            continue

        try:
            generate_sts_clusters(area_obj, sts_me, used_files)
            logger.info(f"Created ME short-term storage clusters for area {area_name}")
        except Exception as e:
            raise MEGenerationError(f"Could not create ME short-term storage for area {area_name}: {e}") from e


def add_me_thermals_to_study(area_obj: Area, thermals_me: dict[str, Any], used_files: Set[Path]) -> None:
    try:
        generate_thermal_me_clusters(area_obj, thermals_me, used_files)
        logger.info(f"Created ME Thermal clusters for area {area_obj.name}")
    except Exception as e:
        raise MEGenerationError(f"Could not create ME Thermal for area {area_obj.name}: {e}") from e


def _me_maxpower_side(
    area_name: str, hydro_def: dict[str, Any], side: str, used_files: Set[Path]
) -> tuple[pd.Series, float]:
    capacity = hydro_def.get(f"{side}_pmax") or {}
    if not isinstance(capacity, dict):
        raise MEGenerationError(f"ME hydro {area_name}: {side}_pmax must be an object")
    hours = capacity.get("hours", 24)
    series_file = hydro_def.get(f"{side}_series")
    if series_file is not None:
        if not isinstance(series_file, str) or not series_file:
            raise MEGenerationError(f"ME hydro {area_name}: {side}_series must be a file name")
        file_path = settings.hydro_me_directory / series_file
        if not file_path.is_file():
            raise MEGenerationError(f"ME hydro {area_name}: missing {side} series file {file_path}")
        used_files.add(file_path)
        df = pd.read_feather(file_path)
        columns = {str(column).lower(): column for column in df.columns}
        if area_name.lower() not in columns:
            raise MEGenerationError(f"ME hydro {area_name}: node not found in {side} series file {file_path}")
        if len(df) != EXPECTED_DAYS:
            raise MEGenerationError(f"ME hydro {area_name}: {side} series must contain {EXPECTED_DAYS} days")
        return df[columns[area_name.lower()]].reset_index(drop=True), hours

    return pd.Series([capacity.get("pmax", 0)] * EXPECTED_DAYS), hours


def _read_me_hydro_matrix(
    area_name: str, label: str, file_name: Any, rows: int, columns: int | None, used_files: Set[Path]
) -> pd.DataFrame:
    """Reads a HYDRO_ME Arrow file and returns its values only (headers dropped).
    `columns=None` accepts any number of columns (at least one)."""
    if not isinstance(file_name, str) or not file_name:
        raise MEGenerationError(f"ME hydro {area_name}: {label} must be a file name")
    file_path = settings.hydro_me_directory / file_name
    if not file_path.is_file():
        raise MEGenerationError(f"ME hydro {area_name}: missing {label} file {file_path}")
    df = pd.read_feather(file_path)
    valid_columns = df.shape[1] >= 1 if columns is None else df.shape[1] == columns
    if len(df) != rows or not valid_columns:
        expected_columns = "at least 1" if columns is None else str(columns)
        raise MEGenerationError(
            f"ME hydro {area_name}: {label} must contain {rows} rows and {expected_columns} columns, got {df.shape}"
        )
    used_files.add(file_path)
    return pd.DataFrame(df.to_numpy(dtype=float))


# key in timeseries_ts -> (expected number of rows, Area.hydro setter)
ME_HYDRO_TIMESERIES = {"ror": (EXPECTED_HOURS, "set_ror_series"), "mod": (EXPECTED_DAYS, "set_mod_series")}
WATER_VALUES_COLUMNS = 101


def _set_me_hydro_timeseries(area_obj: Area, area_name: str, timeseries_ts: Any, used_files: Set[Path]) -> None:
    if not isinstance(timeseries_ts, dict):
        raise MEGenerationError(f"ME hydro {area_name}: timeseries_ts must be an object")
    unknown = set(timeseries_ts) - set(ME_HYDRO_TIMESERIES)
    if unknown:
        raise MEGenerationError(f"ME hydro {area_name}: unknown timeseries_ts keys {sorted(unknown)}")

    for key, file_name in timeseries_ts.items():
        expected_rows, setter = ME_HYDRO_TIMESERIES[key]
        df = _read_me_hydro_matrix(area_name, f"timeseries_ts.{key}", file_name, expected_rows, None, used_files)
        getattr(area_obj.hydro, setter)(df)


def add_me_hydro_to_study(area_objs: dict[str, Area], hydro_me: dict[str, Any], used_files: Set[Path]) -> None:
    areas_by_id = {name.lower(): area for name, area in area_objs.items()}
    for area_name, hydro_def in hydro_me.items():
        area_obj = areas_by_id.get(area_name.lower())
        if area_obj is None:
            raise MEGenerationError(f"ME hydro area {area_name} is not present in area_me")
        if not isinstance(hydro_def, dict):
            raise MEGenerationError(f"ME hydro {area_name} must be an object")

        properties = hydro_def.get("properties")
        if properties is not None:
            area_obj.hydro.update_properties(HydroPropertiesUpdate(**properties))
        inflow_structure = hydro_def.get("inflow_structure")
        if inflow_structure is not None:
            area_obj.hydro.update_inflow_structure(InflowStructureUpdate(**inflow_structure))
        allocation = hydro_def.get("allocation")
        if allocation:
            set_hydro_allocation(area_obj, allocation)

        generating, generating_hours = _me_maxpower_side(area_name, hydro_def, "generating", used_files)
        pumping, pumping_hours = _me_maxpower_side(area_name, hydro_def, "pumping", used_files)
        area_obj.hydro.set_maxpower(
            pd.DataFrame({"0": generating, "1": generating_hours, "2": pumping, "3": pumping_hours})
        )

        reservoir_ts = hydro_def.get("reservoir_ts")
        if reservoir_ts is not None:
            area_obj.hydro.set_reservoir(
                _read_me_hydro_matrix(area_name, "reservoir_ts", reservoir_ts, EXPECTED_DAYS, 3, used_files)
            )

        water_values_ts = hydro_def.get("water_values_ts")
        if water_values_ts is not None:
            area_obj.hydro.set_water_values(
                _read_me_hydro_matrix(
                    area_name, "water_values_ts", water_values_ts, EXPECTED_DAYS, WATER_VALUES_COLUMNS, used_files
                )
            )

        timeseries_ts = hydro_def.get("timeseries_ts")
        if timeseries_ts is not None:
            _set_me_hydro_timeseries(area_obj, area_name, timeseries_ts, used_files)


P2G_NODE_PREFIX = "z_p2g_"
ME_NODE_PREFIX = "v_me_"


def _collect_p2g_links(links_me: dict[str, Any]) -> dict[str, list[str]]:
    """For each z_p2g_XX node, list the other area on every link it appears in"""
    links_by_p2g_node: dict[str, list[str]] = {}
    for link_key in links_me:
        area_from, area_to = (part.lower() for part in link_key.split("/"))
        for node, other_side in ((area_from, area_to), (area_to, area_from)):
            if node.startswith(P2G_NODE_PREFIX):
                links_by_p2g_node.setdefault(node, []).append(other_side)
    return links_by_p2g_node


def _build_p2g_terms(node: str, other_sides: list[str], efficiency_by_node: dict[str, float]) -> list[ConstraintTerm]:
    # efficiency value is required, even if all links are ME <-> ME (efficiency not used)
    if node not in efficiency_by_node:
        raise MEGenerationError(f"Efficiency missing for P2G node {node}")

    terms = []
    for other_side in other_sides:
        weight = 1.0 if other_side.startswith(ME_NODE_PREFIX) else efficiency_by_node[node]
        terms.append(ConstraintTerm(data=LinkData(area1=node, area2=other_side), weight=weight))
    return terms


def add_me_p2g_binding_constraints(study: Study, links_me: dict[str, Any], constraints_p2g: list[Any]) -> None:
    try:
        efficiency_by_node = {str(entry["node"]).lower(): float(entry["efficiency"]) for entry in constraints_p2g}
    except (KeyError, TypeError, ValueError) as e:
        raise MEGenerationError(f"Invalid P2G efficiency entry in binding_constraints_me: {e}") from e

    links_by_p2g_node = _collect_p2g_links(links_me)
    properties = BindingConstraintProperties(
        enabled=True, time_step=BindingConstraintFrequency.HOURLY, operator=BindingConstraintOperator.EQUAL
    )

    for node, other_sides in links_by_p2g_node.items():
        try:
            terms = _build_p2g_terms(node, other_sides, efficiency_by_node)
            study.create_binding_constraint(name=f"efficiency_{node}", properties=properties, terms=terms)
            logger.info(f"Created ME P2G binding constraint efficiency_{node}")
        except MEGenerationError:
            raise
        except Exception as e:
            raise MEGenerationError(f"Could not create ME P2G binding constraint for node {node}: {e}") from e


def _build_g2p_terms(constraint: dict[str, Any]) -> list[ConstraintTerm]:
    node_1_left = str(constraint["node_1_left"]).lower()
    node_2_left = str(constraint["node_2_left"]).lower()
    node_right_area = constraint["node_right_area"]
    if not node_1_left or not node_2_left or not isinstance(node_right_area, dict):
        raise ValueError("node_1_left and node_2_left must be set and node_right_area must be an object")

    terms = [ConstraintTerm(data=LinkData(area1=node_1_left, area2=node_2_left), weight=1.0)]
    for area_name, clusters in node_right_area.items():
        if not isinstance(clusters, dict):
            raise ValueError(f"clusters for area {area_name} must be an object")
        for cluster_name, cluster_data in clusters.items():
            if not isinstance(cluster_data, dict):
                raise ValueError(f"data for cluster {cluster_name} in area {area_name} must be an object")
            efficiency = float(cluster_data["efficiency"])
            if efficiency == 0:
                raise ValueError(f"efficiency for cluster {cluster_name} in area {area_name} must not be zero")
            normalized_area_name = str(area_name).lower()
            normalized_cluster_name = f"{area_name}_{cluster_name}".lower()
            terms.append(
                ConstraintTerm(
                    data=ClusterData(area=normalized_area_name, cluster=normalized_cluster_name),
                    weight=-1.0 / efficiency,
                )
            )
    return terms


def add_me_g2p_binding_constraints(study: Study, constraints_g2p: list[Any]) -> None:
    for constraint in constraints_g2p:
        constraint_name = constraint.get("name") if isinstance(constraint, dict) else None
        try:
            if not isinstance(constraint, dict) or not constraint_name:
                raise ValueError("each constraint must be an object with a name")
            properties = BindingConstraintProperties(
                enabled=constraint["enabled"],
                time_step=BindingConstraintFrequency(constraint["type"]),
                operator=BindingConstraintOperator(constraint["operator"]),
            )
            terms = _build_g2p_terms(constraint)
            study.create_binding_constraint(name=str(constraint_name), properties=properties, terms=terms)
            logger.info(f"Created ME G2P binding constraint {constraint_name}")
        except (KeyError, TypeError, ValueError) as e:
            raise MEGenerationError(f"Invalid G2P binding constraint {constraint_name or '<unnamed>'}: {e}") from e
        except Exception as e:
            raise MEGenerationError(f"Could not create ME G2P binding constraint {constraint_name}: {e}") from e


def generate_me(study: Study, me_data: dict[str, Any], used_files: Set[Path]) -> None:
    area_me = me_data.get("area_me") or {}
    links_me = me_data.get("links_me") or {}
    area_objs = add_me_areas_to_study(study, area_me, used_files)
    add_me_links_to_study(study, links_me)
    add_me_sts_to_study(area_objs, area_me, used_files)
    add_me_hydro_to_study(area_objs, me_data.get("hydro_me") or {}, used_files)

    binding_constraints_me = me_data.get("binding_constraints_me") or {}
    add_me_p2g_binding_constraints(study, links_me, binding_constraints_me.get("constraints_P2G") or [])
    add_me_g2p_binding_constraints(study, binding_constraints_me.get("constraints_G2P") or [])
