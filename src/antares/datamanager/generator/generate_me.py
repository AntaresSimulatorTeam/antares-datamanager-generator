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
    ConstraintTerm,
    LinkData,
    LinkProperties,
    LinkPropertiesUpdate,
    TransmissionCapacities,
)
from antares.craft.model.area import Area, AreaProperties, AreaUi
from antares.craft.model.study import Study
from antares.datamanager.core.settings import settings
from antares.datamanager.exceptions.exceptions import MEGenerationError
from antares.datamanager.generator.generate_link_matrices import generate_constant_link_capacity_df
from antares.datamanager.generator.generate_sts_clusters import generate_sts_clusters
from antares.datamanager.logs.logging_setup import get_logger
from antares.datamanager.utils.area_ui_utils import generate_random_color, generate_random_coordinate

logger = get_logger(__name__)

EXPECTED_HOURS = 8760

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
#       }
#     }
#   },
#   "links_me": {
#     "v_me_h2_long_euest/v_me_h2_long_iber": {
#       "directMw": 12000, "indirectMw": 12000, "hurdleCostDirect": 0.1, "hurdleCostIndirect": 0.1
#     },
#     "FR/z_p2g_long_fr": {"directMw": 6280, "indirectMw": 0, "hurdleCostDirect": 0, "hurdleCostIndirect": 0}
#   },
#   "binding_constraints_me": {
#     "constraints_P2G": [
#       {"node": "z_p2g_long_fr", "efficiency": 0.744}
#       # one constraint per z_p2g_XX node in links_me
#       # links to a v_me_* node get weight 1.0 (ME <-> ME),
#       # links to anything else get weight = efficiency (ELEC <-> ME)
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
        try:
            area_obj = study.create_area(area_name=area_name, properties=properties, ui=ui)
            _set_me_area_loads(area_obj, loads, load_directory, used_files)
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


def generate_me(study: Study, me_data: dict[str, Any], used_files: Set[Path]) -> None:
    area_me = me_data.get("area_me") or {}
    links_me = me_data.get("links_me") or {}
    area_objs = add_me_areas_to_study(study, area_me, used_files)
    add_me_links_to_study(study, links_me)
    add_me_sts_to_study(area_objs, area_me, used_files)

    binding_constraints_me = me_data.get("binding_constraints_me") or {}
    add_me_p2g_binding_constraints(study, links_me, binding_constraints_me.get("constraints_P2G") or [])
