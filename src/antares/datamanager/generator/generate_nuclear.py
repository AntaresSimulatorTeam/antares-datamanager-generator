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
from typing import Any, Optional, Set

import numpy as np
import pandas as pd

from antares.craft import (
    BindingConstraintFrequency,
    BindingConstraintOperator,
    BindingConstraintProperties,
    ClusterData,
    ConstraintTerm,
    LocalTSGenerationBehavior,
    ThermalClusterPropertiesUpdate,
)
from antares.craft.model.area import Area
from antares.craft.model.commons import FilterOption
from antares.craft.model.study import Study
from antares.craft.tools.contents_tool import transform_name_to_id
from antares.datamanager.core.settings import settings
from antares.datamanager.exceptions.exceptions import NuclearGenerationError
from antares.datamanager.generator.generate_misc_timeseries import EXPECTED_HOURS, MISC_COLUMNS
from antares.datamanager.logs.logging_setup import get_logger
from antares.datamanager.utils.seed_factory import SeedFactory

logger = get_logger(__name__)

# Expected JSON layout :
#
# Nuclear thermal clusters are under "nuclear.clusters" instead of "thermals",
#
# "fr": {
#   "thermals": { "FR_Gas_ccgt": {...} },
#   "nuclear": { "clusters": {
#     "FR_Nuclear_cp0_cp1_cp2": { "series": "<arrow_file>", ... },   # LT: no transforamtion, same thing for cp0_cp1_cp2/n4/p4
#     "FR_Nuclear_epr": { "series": "<arrow_file>", ... },           # EPR: no transformation
#     "FR_Nuclear_smr": {
#       "series": "<arrow_file_shared_pool>",                       # SMR: shared raw pool, needs mixing
#       "smr_mixage": { "unit_count": 3, "seed": "frnuclear_smrseed-tsgen-thermal" },
#       ...
#     }
#   } }
# },
# "y_nuc_modulation": {
#   "nuclear": { "clusters": { "y_nuc_modulation_nuclear_cp0_cp1_cp2": {...}, ... } }  # same shape, mirrored
# }
#
# "series" is optional per cluster,
# "smr_mixage" is present if and only if "series" points to
# a shared SMR pool that still needs the seeded "draw and sum" below.
#
# Top level "binding_constraints" (same level as area or links) contains talon and modulation
# both are optional
#
# "binding_constraints": {
#   "nuclear_modulation": {
#     "group": "scenarised200",             # groups the constraint TS + encodes nbTsColumns
#     "nbTsColumns": 200,
#     "frStandardClusters": ["fr_nuclear_cp0_cp1_cp2", "fr_nuclear_epr", ...],   # weight = 1
#     "frPeakClusters": ["fr_nuclear_peak1", ...],                              # weight = 1, hourly only
#     "yNucModulationClusters": ["y_nuc_modulation_nuclear_cp0_cp1_cp2", ...],  # weight = -coeff
#     "constraints": [
#       {
#         "name": "nuc_modulation_limit",   # -> binding constraint name
#         "type": "hourly",                 # -> hourly | daily | weekly
#         "coeff": 1.00,                    # -> y_nuc_modulation side weight (negated)
#         "includesPeak": true,             # -> also include frPeakClusters on the FR side
#         "series": "<arrow_file>"          # -> RHS (less_term_matrix), without any transformation
#       },
#       ...
#     ]
#   },
#   "nuclear_talon": {
#     "group": "scenarised200",
#     "nbTsColumns": 200,
#     "frStandardClusters": ["fr_nuclear_cp0_cp1_cp2", "fr_nuclear_epr", ...],   # weight = 1, fixed
#     "series": "<arrow_file>"            # -> RHS (greater_term_matrix), without any transformation
#   }
# }

FR_AREA_ID = "fr"
Y_NUC_MODULATION_AREA_NAME = "y_nuc_modulation"
Y_NUC_MODULATION_PSP_DEFAULT = -999999
NUCLEAR_TALON_CONSTRAINT_NAME = "talon_nuc"

_TIME_STEP_BY_TYPE = {
    "hourly": BindingConstraintFrequency.HOURLY,
    "daily": BindingConstraintFrequency.DAILY,
    "weekly": BindingConstraintFrequency.WEEKLY,
}

_ALL_FILTERS = {
    FilterOption.HOURLY,
    FilterOption.DAILY,
    FilterOption.WEEKLY,
    FilterOption.MONTHLY,
    FilterOption.ANNUAL,
}

_REQUIRED_MODULATION_KEYS = ("group", "frStandardClusters", "frPeakClusters", "yNucModulationClusters", "constraints")
_REQUIRED_MODULATION_CONSTRAINT_KEYS = ("name", "type", "coeff", "includesPeak", "series")
_REQUIRED_TALON_KEYS = ("group", "frStandardClusters", "series")


def _require_keys(payload: dict[str, Any], required_keys: tuple[str, ...], context: str) -> None:
    missing = [key for key in required_keys if key not in payload]
    if missing:
        raise NuclearGenerationError(f"{context} is missing required key(s): {', '.join(missing)}")


def generate_y_nuc_modulation_misc(area_obj: Area) -> None:
    """
    The y_nuc_modulation area has no real MISC generation. It only has a
    hardcoded value on the PSP column (all other columns are 0)
    """
    matrix = pd.DataFrame(np.zeros((EXPECTED_HOURS, len(MISC_COLUMNS)), dtype=np.float64), columns=MISC_COLUMNS)
    matrix["PSP"] = Y_NUC_MODULATION_PSP_DEFAULT
    area_obj.set_misc_gen(matrix)


def generate_nuclear_modulation_binding_constraints(
    study: Study,
    nuclear_modulation_binding_constraints: dict[str, Any],
    used_files: Optional[Set[Path]] = None,
) -> None:
    """
    Creates the nuclear modulation binding constraints (hourly/daily/weekly) with
    the FR nuclear clusters to their y_nuc_modulation.
    """
    _require_keys(nuclear_modulation_binding_constraints, _REQUIRED_MODULATION_KEYS, "nuclear_modulation")

    group = nuclear_modulation_binding_constraints["group"]
    fr_standard_clusters = nuclear_modulation_binding_constraints["frStandardClusters"]
    fr_peak_clusters = nuclear_modulation_binding_constraints["frPeakClusters"]
    y_nuc_modulation_clusters = nuclear_modulation_binding_constraints["yNucModulationClusters"]
    constraints = nuclear_modulation_binding_constraints["constraints"]

    base_dir = settings.nuclear_modulation_ts_directory

    for constraint in constraints:
        _create_nuclear_modulation_constraint(
            study,
            constraint,
            group=group,
            fr_standard_clusters=fr_standard_clusters,
            fr_peak_clusters=fr_peak_clusters,
            y_nuc_modulation_clusters=y_nuc_modulation_clusters,
            base_dir=base_dir,
            used_files=used_files,
        )


def _create_nuclear_modulation_constraint(
    study: Study,
    constraint: dict[str, Any],
    group: str,
    fr_standard_clusters: list[str],
    fr_peak_clusters: list[str],
    y_nuc_modulation_clusters: list[str],
    base_dir: Path,
    used_files: Optional[Set[Path]] = None,
) -> None:
    _require_keys(constraint, _REQUIRED_MODULATION_CONSTRAINT_KEYS, "nuclear_modulation constraint entry")

    name = constraint["name"]
    coeff = constraint["coeff"]
    includes_peak = constraint["includesPeak"]

    properties = BindingConstraintProperties(
        enabled=True,
        time_step=_TIME_STEP_BY_TYPE[constraint["type"]],
        operator=BindingConstraintOperator.LESS,
        group=group,
        filter_year_by_year=set(_ALL_FILTERS),
        filter_synthesis=set(_ALL_FILTERS),
    )

    fr_clusters = list(fr_standard_clusters) + (list(fr_peak_clusters) if includes_peak else [])
    terms = [
        ConstraintTerm(data=ClusterData(area=FR_AREA_ID, cluster=cluster_id), weight=1) for cluster_id in fr_clusters
    ]
    terms += [
        ConstraintTerm(data=ClusterData(area=Y_NUC_MODULATION_AREA_NAME, cluster=cluster_id), weight=-coeff)
        for cluster_id in y_nuc_modulation_clusters
    ]

    series_path = base_dir / constraint["series"]
    if used_files is not None:
        used_files.add(series_path)
    less_term_matrix = pd.read_feather(series_path)

    study.create_binding_constraint(
        name=name,
        properties=properties,
        terms=terms,
        less_term_matrix=less_term_matrix,
    )
    logger.info(f"Created nuclear modulation binding constraint {name}")


def generate_nuclear_talon_binding_constraint(
    study: Study,
    nuclear_talon_binding_constraint: dict[str, Any],
    used_files: Optional[Set[Path]] = None,
) -> None:
    """
    Creates the nuclear talon binding constraint
    FR physical nuclear production (standard clusters only, no peak, no virtual).
    """
    _require_keys(nuclear_talon_binding_constraint, _REQUIRED_TALON_KEYS, "nuclear_talon")

    group = nuclear_talon_binding_constraint["group"]
    fr_standard_clusters = nuclear_talon_binding_constraint["frStandardClusters"]

    properties = BindingConstraintProperties(
        enabled=True,
        time_step=BindingConstraintFrequency.HOURLY,
        operator=BindingConstraintOperator.GREATER,
        group=group,
        filter_year_by_year=set(_ALL_FILTERS),
        filter_synthesis=set(_ALL_FILTERS),
    )

    terms = [
        ConstraintTerm(data=ClusterData(area=FR_AREA_ID, cluster=cluster_id), weight=1)
        for cluster_id in fr_standard_clusters
    ]

    base_dir = settings.nuclear_talon_ts_directory
    series_path = base_dir / nuclear_talon_binding_constraint["series"]
    if used_files is not None:
        used_files.add(series_path)
    greater_term_matrix = pd.read_feather(series_path)

    study.create_binding_constraint(
        name=NUCLEAR_TALON_CONSTRAINT_NAME,
        properties=properties,
        terms=terms,
        greater_term_matrix=greater_term_matrix,
    )
    logger.info(f"Created nuclear talon binding constraint {NUCLEAR_TALON_CONSTRAINT_NAME}")


_REQUIRED_SMR_MIXAGE_KEYS = ("unit_count", "seed")

# Placeholder value used by the Java backend when no trajectory of a given family
# is linked to the study - the "series" key stays present but keeps its pre-existing
# placeholder rather than being omitted, same placeholder used for fuel_cost/co2_cost.
_UNLINKED_SERIES_PLACEHOLDER = "matrix hash"


def generate_nuclear_availability(
    area_obj: Area,
    nuclear_clusters: dict[str, Any],
    used_files: Optional[Set[Path]] = None,
) -> None:
    """
    Applies LT/EPR/SMR availability series on created nuclear clusters
    (see create_thermal_cluster_with_prepro, called before).

    Clusters with no "series" entry (no trajectory linked) are left not touched
    """
    base_dir: Optional[Path] = None
    pool_cache: dict[Path, pd.DataFrame] = {}

    for cluster_name, cluster_values in nuclear_clusters.items():
        series_filename = cluster_values.get("series")
        if not series_filename or series_filename == _UNLINKED_SERIES_PLACEHOLDER:
            continue

        if base_dir is None:
            base_dir = settings.nuclear_availability_ts_directory

        series_path = base_dir / series_filename
        if used_files is not None:
            used_files.add(series_path)
        if series_path not in pool_cache:
            pool_cache[series_path] = pd.read_feather(series_path)
        pool = pool_cache[series_path]

        smr_mixage = cluster_values.get("smr_mixage")
        final_series = _mix_smr_series(pool, smr_mixage, cluster_name) if smr_mixage is not None else pool

        thermal_id = transform_name_to_id(cluster_name)
        try:
            thermal_cluster = area_obj.get_thermals()[thermal_id]
        except KeyError as exc:
            raise NuclearGenerationError(
                f"Nuclear cluster '{cluster_name}' not found on area before applying availability series"
            ) from exc
            
        thermal_cluster.update_properties(ThermalClusterPropertiesUpdate(gen_ts=LocalTSGenerationBehavior.FORCE_NO_GENERATION))
        thermal_cluster.set_series(final_series)
        logger.info(f"Applied nuclear availability series to cluster {cluster_name}")


def _mix_smr_series(pool: pd.DataFrame, smr_mixage: dict[str, Any], cluster_name: str) -> pd.DataFrame:
    """
    "Mixage des chroniques" draw: the first active SMR unit maps
    output column k directly to pool column k, each additional unit independently draws
    nb_series pool column indices with replacement (seeded), and output column k is the
    cell by cell sum across all active units' contributions for that column
    """
    _require_keys(smr_mixage, _REQUIRED_SMR_MIXAGE_KEYS, f"smr_mixage for cluster '{cluster_name}'")

    unit_count = smr_mixage["unit_count"]
    if not isinstance(unit_count, int) or isinstance(unit_count, bool) or unit_count < 1:
        raise NuclearGenerationError(f"Invalid smr_mixage.unit_count for cluster '{cluster_name}': {unit_count!r}")

    if unit_count == 1:
        return pool.copy()

    rng = np.random.default_rng(SeedFactory.from_string(str(smr_mixage["seed"])))
    pool_values = pool.to_numpy()
    nb_series = pool_values.shape[1]

    total = pool_values.copy()
    for _ in range(unit_count - 1):
        draw = rng.integers(0, nb_series, size=nb_series)
        total = total + pool_values[:, draw]

    return pd.DataFrame(total, columns=pool.columns)
