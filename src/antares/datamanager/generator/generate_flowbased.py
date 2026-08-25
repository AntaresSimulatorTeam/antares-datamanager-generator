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

from collections import Counter
from pathlib import Path
from typing import Any, Optional, Set, cast

import numpy as np
import pandas as pd

from antares.craft import (
    BindingConstraintFrequency,
    BindingConstraintOperator,
    BindingConstraintProperties,
    ClusterData,
    ConstraintTerm,
    LinkData,
    LinkProperties,
    Month,
    ThermalClusterProperties,
    TransmissionCapacities,
)
from antares.craft.model.area import Area
from antares.craft.model.study import Study
from antares.datamanager.core.settings import settings
from antares.datamanager.exceptions.exceptions import FlowbasedGenerationError
from antares.datamanager.generator.generate_link_matrices import generate_link_capacity_df
from antares.datamanager.generator.generate_res_clusters import map_res_group_to_aw
from antares.datamanager.logs.logging_setup import get_logger
from antares.datamanager.utils.random_forest_reader import load_forest_model, predict_clusters_batch
from antares.datamanager.utils.season_utils import SeasonManager

logger = get_logger(__name__)

# Expected JSON layout (top-level "flowbased" key)
#
# "flowbased": {
#   "recalculate_ts": true,             # false = Possibilite 1 (lecture directe) (#TODO)
#   "ts_path": "flowbased/name_2022/2021",  # dir with weight.txt/second_member.txt/ts.txt/pmml
#   "virtual_nodes": ["zz_flowbased", "model_description_fb", "alegro1", "alegro2", "alegro3"],
#   "links": [
#     {
#       "name": "alegro1 - alegro2",    # full link name "area1 - area2"
#       "transmission_capacities": "ENABLED",   # ENABLED -> capacity fields below are read; INFINITE -> ignored
#       "winter_HP_direct_MW": 1000, "winter_HP_indirect_MW": 1000,
#       "winter_HC_direct_MW": 1000, "winter_HC_indirect_MW": 1000,
#       "summer_HP_direct_MW": 1000, "summer_HP_indirect_MW": 1000,
#       "summer_HC_direct_MW": 1000, "summer_HC_indirect_MW": 1000
#     },
#     {"name": "fr - zz_flowbased", "transmission_capacities": "INFINITE"}
#   ],
#   "type_days": [                      # only present when recalculate_ts is true
#     {"clustering": "winter1", "id_type_day": 1, "class_day": "winterWd"}, ...
#   ]
# }

EXPECTED_HOURS = 8760
HUB_AREAS = ("at", "be", "de", "fr", "nl")
WIND_GROUPS = {"Wind Onshore", "Wind Offshore"}
SOLAR_GROUPS = {"Solar PV", "Solar Thermal"}
SCENARIO_BUILDER_GROUP_PREFIX = "flowbased_fb"

# Required file names
SUMMER_MODEL_FILENAME = "random_forest_summer.pmml"
WINTER_MODEL_FILENAME = "random_forest_winter.pmml"
WEIGHT_FILENAME = "weight.txt"
SECOND_MEMBER_FILENAME = "second_member.txt"
TS_FILENAME = "ts.txt"

_REQUIRED_LINK_CAPACITY_KEYS = (
    "winter_HC_direct_MW",
    "winter_HP_direct_MW",
    "summer_HC_direct_MW",
    "summer_HP_direct_MW",
    "winter_HC_indirect_MW",
    "winter_HP_indirect_MW",
    "summer_HC_indirect_MW",
    "summer_HP_indirect_MW",
)


class FlowbasedFileReader:
    """
    Parses the flowbased raw reference files (`weight.txt`, `second_member.txt`).
    """

    SECOND_MEMBER_COLUMNS = ("id_day", "id_hour", "vect_b", "name")

    @staticmethod
    def read_weight_file(weight_path: Path) -> pd.DataFrame:
        """Read the PTDF coefficient table (`weight.txt`).

        One row per constraint (`FBxxx`), one column per link, named after the pair of
        zones it connects (ex: `fr.zz_flowbased`).

        Args:
            weight_path: Path to `weight.txt`

        Returns:
            A DataFrame indexed by constraint name (`Name` column), one column per link.

        Raises:
            FlowbasedGenerationError: If the file is missing or cannot be parsed.
        """
        try:
            weight_df = pd.read_csv(weight_path, sep=r"\s+")
        except (OSError, pd.errors.ParserError) as exc:
            raise FlowbasedGenerationError(f"Could not read weight file {weight_path}: {exc}") from exc

        if "Name" not in weight_df.columns:
            raise FlowbasedGenerationError(f"Weight file {weight_path} is missing the expected 'Name' column")

        logger.info("Loaded flowbased weight file", extra={"weight_path": str(weight_path), "rows": len(weight_df)})
        return weight_df.set_index("Name")

    @staticmethod
    def read_second_member_file(second_member_path: Path) -> pd.DataFrame:
        """Read the RHS lookup table (`second_member.txt`).

        Expected columns: `Id_day`, `Id_hour`, `vect_b`, `Name`.

        Args:
            second_member_path: Path to `second_member.txt`

        Returns:
            A DataFrame with lowercase columns `id_day`, `id_hour`, `vect_b`, `name`.

        Raises:
            FlowbasedGenerationError: If the file is missing, cannot be parsed, or is
                missing one of the expected columns.
        """
        try:
            second_member_df = pd.read_csv(second_member_path, sep=r"\s+")
        except (OSError, pd.errors.ParserError) as exc:
            raise FlowbasedGenerationError(f"Could not read second member file {second_member_path}: {exc}") from exc

        second_member_df.columns = pd.Index([str(column).lower() for column in second_member_df.columns])
        missing_columns = set(FlowbasedFileReader.SECOND_MEMBER_COLUMNS) - set(second_member_df.columns)
        if missing_columns:
            raise FlowbasedGenerationError(
                f"Second member file {second_member_path} is missing column(s): {sorted(missing_columns)}"
            )

        logger.info(
            "Loaded flowbased second member file",
            extra={"second_member_path": str(second_member_path), "rows": len(second_member_df)},
        )
        return second_member_df

    @staticmethod
    def read_ts_file(ts_path: Path) -> pd.DataFrame:
        """Read the day type / domain time series (`ts.txt`).

        Expected format: daily rows (365 days) or hourly rows (8760 hours).
        Columns: metadata columns ('Date', 'Id_day') + one column per climatic year (containing Id_domaine).

        Args:
            ts_path: Path to `ts.txt`

        Returns:
            A DataFrame with 8760 hourly rows, columns 0..N-1 containing the domain IDs.
        """
        try:
            ts_df = pd.read_csv(ts_path, sep=r"\s+", quotechar='"')
        except (OSError, pd.errors.ParserError) as exc:
            raise FlowbasedGenerationError(f"Could not read ts file {ts_path}: {exc}") from exc

        # Suppression des colonnes de métadonnées (insensibles à la casse)
        cols_to_drop = [col for col in ts_df.columns if str(col).lower() in {"date", "id_day"}]
        data_df = ts_df.drop(columns=cols_to_drop)

        if data_df.empty:
            raise FlowbasedGenerationError(f"No domain columns found in {ts_path} after dropping metadata columns")

        # Si le fichier est au pas journalier (365 ou 366 jours), étendre à 8760 heures (24h par jour)
        if len(data_df) in (365, 366):
            # On prend les 365 premiers jours pour obtenir 365 * 24 = 8760 heures
            daily_values = data_df.iloc[:365].to_numpy()
            hourly_values = np.repeat(daily_values, 24, axis=0)
            result_df = pd.DataFrame(hourly_values)
        elif len(data_df) == EXPECTED_HOURS:
            result_df = pd.DataFrame(data_df.to_numpy())
        else:
            raise FlowbasedGenerationError(
                f"Unexpected number of rows in {ts_path}: {len(data_df)} (expected 365 daily rows or {EXPECTED_HOURS} hourly rows)"
            )

        logger.info(
            "Loaded flowbased ts file",
            extra={"ts_path": str(ts_path), "rows": len(result_df), "columns": result_df.shape[1]},
        )

        return result_df


def generate_flowbased_binding_constraints(
    study: Study, flowbased_data: dict[str, Any], first_month: Month, nb_years: Any, used_files: Set[Path]
) -> None:
    """Create the 100 FBxxx flowbased binding constraints (Possibilite 2 / recalculate).

    Assumes the flowbased hub links (ex: `fr%zz_flowbased`) already exist in `study`
    (call `create_flowbased_areas_and_links` first)

    Args:
        study: The Antares study being generated.
        study_data: The full parsed study data.
        used_files: Set of `.arrow`/raw files opened, tracked for post generation cleanup.

    Raises:
        FlowbasedGenerationError: On any inconsistency in the flowbased input data.
    """
    trajectory_directory = _resolve_trajectory_directory(flowbased_data)

    weight_df = _read_weight_file(trajectory_directory, used_files)
    second_member_df = _read_second_member_file(trajectory_directory, used_files)
    vect_b_lookup = build_vect_b_lookup_table(second_member_df)

    if flowbased_data.get("recalculate_ts"):
        type_days = flowbased_data.get("type_days") or []
        if not type_days:
            raise FlowbasedGenerationError("flowbased.type_days is required for the recalculate path")

        summer_model, winter_model = _load_models(trajectory_directory, used_files)
        hub_features = _build_hub_features(study)
        id_day_types = compute_id_day_types(summer_model, winter_model, hub_features, type_days, first_month)
    else:
        id_day_types = _read_ts_file(trajectory_directory, used_files)

    n_columns = id_day_types.shape[1]

    group_name = f"{SCENARIO_BUILDER_GROUP_PREFIX}{n_columns}"
    properties = BindingConstraintProperties(
        enabled=True,
        time_step=BindingConstraintFrequency.HOURLY,
        operator=BindingConstraintOperator.LESS,
        group=group_name,
    )

    for constraint_name, weight_row in weight_df.iterrows():
        terms = _build_constraint_terms(weight_row, flowbased_data, study.get_areas())
        rhs = build_rhs_matrix(id_day_types, vect_b_lookup, str(constraint_name))
        rhs = _pad_to_binding_constraint_hourly_rows(rhs)
        study.create_binding_constraint(
            name=str(constraint_name), properties=properties, terms=terms, less_term_matrix=rhs
        )
        logger.info(f"Created flowbased binding constraint {constraint_name}")

    _wire_scenario_builder(study, group_name, n_columns, nb_years)


# virtual zones + hub/alegro links


def create_flowbased_areas_and_links(
    study: Study, flowbased_data: dict[str, Any], first_month: Optional[Month]
) -> None:
    """Create the flowbased virtual zones and their links.

    Needed by both RHS possibilities (recalcul and lecture directe)

    Args:
        study: The Antares study being generated.
        flowbased_data: The JSON `flowbased` block (`virtual_nodes`, `links`).

    Raises:
        FlowbasedGenerationError: On any inconsistency in the flowbased input data.
        :param study:
        :param flowbased_data:
        :param first_month:
    """
    for area_name in flowbased_data.get("virtual_nodes") or []:
        study.create_area(area_name=area_name)
        logger.info(f"Created flowbased virtual area {area_name}")

    for entry in flowbased_data.get("links") or []:
        area1, area2 = _parse_link_name(entry.get("name"))
        _create_flowbased_link(study, area1, area2, entry, first_month)

    # Création du cluster et de la contrainte restriction_ahc si model_description_fb est présent
    if "model_description_fb" in (flowbased_data.get("virtual_nodes") or []):
        create_restriction_ahc(study)


def _parse_link_name(name: Any) -> tuple[str, str]:
    """flowbased.links entries name the full link, ex: "fr - zz_flowbased"."""
    parts = [part.strip().lower() for part in str(name).split("-")]
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise FlowbasedGenerationError(f"Unexpected flowbased link name {name!r}, expected 'area1 - area2'")
    return parts[0], parts[1]


def _create_flowbased_link(
    study: Study, area1: str, area2: str, link_entry: dict[str, Any], first_month: Optional[Month]
) -> None:
    """`transmission_capacities` is always present, ENABLED or INFINITE. Capacity fields
    (winter_HP_direct_MW, ...) are only read when ENABLED;"""
    transmission_capacities = _parse_transmission_capacities(link_entry.get("transmission_capacities", "ENABLED"))
    link_name = f"{area1}-{area2}"
    link = study.create_link(
        area_from=area1, area_to=area2, properties=LinkProperties(transmission_capacities=transmission_capacities)
    )

    if transmission_capacities != TransmissionCapacities.ENABLED:
        logger.info(f"Created flowbased link {link_name} (transmission_capacities={transmission_capacities})")
        return

    link_data = _to_link_capacity_data(link_entry, area1, area2)
    link.set_capacity_direct(
        generate_link_capacity_df(link_data, "direct", 0, link_name=link_name, first_month=first_month)
    )
    link.set_capacity_indirect(
        generate_link_capacity_df(link_data, "indirect", 0, link_name=link_name, first_month=first_month)
    )
    logger.info(f"Created flowbased link {link_name}")


def _parse_transmission_capacities(value: Any) -> TransmissionCapacities:
    try:
        return TransmissionCapacities[str(value).strip().upper()]
    except KeyError as exc:
        raise FlowbasedGenerationError(f"Unknown transmission_capacities value: {value!r}") from exc


def _to_link_capacity_data(link_entry: dict[str, Any], area1: str, area2: str) -> dict[str, int]:
    missing = [key for key in _REQUIRED_LINK_CAPACITY_KEYS if key not in link_entry]
    if missing:
        raise FlowbasedGenerationError(f"Missing capacity field(s) {missing} for flowbased link {area1}-{area2}")
    # generate_link_capacity_df matches keys without underscores
    # (ex: "winterhcdirectmw")
    return {key.replace("_", ""): link_entry[key] for key in _REQUIRED_LINK_CAPACITY_KEYS}


_TS_PATH_PREFIX = "flowbased/"


def _resolve_trajectory_directory(flowbased_data: dict[str, Any]) -> Path:
    ts_path = flowbased_data.get("ts_path")
    if not ts_path:
        raise FlowbasedGenerationError("flowbased.ts_path is required for the recalculate path")
    relative_path = str(ts_path).removeprefix(_TS_PATH_PREFIX)
    return settings.flowbased_directory / relative_path


def _load_models(trajectory_directory: Path, used_files: Set[Path]) -> tuple[Any, Any]:
    summer_path = trajectory_directory / SUMMER_MODEL_FILENAME
    winter_path = trajectory_directory / WINTER_MODEL_FILENAME
    used_files.add(summer_path)
    used_files.add(winter_path)
    return load_forest_model(summer_path), load_forest_model(winter_path)


def _read_weight_file(trajectory_directory: Path, used_files: Set[Path]) -> pd.DataFrame:
    weight_path = trajectory_directory / WEIGHT_FILENAME
    used_files.add(weight_path)
    return FlowbasedFileReader.read_weight_file(weight_path)


def _read_second_member_file(trajectory_directory: Path, used_files: Set[Path]) -> pd.DataFrame:
    second_member_path = trajectory_directory / SECOND_MEMBER_FILENAME
    used_files.add(second_member_path)
    return FlowbasedFileReader.read_second_member_file(second_member_path)


def _read_ts_file(trajectory_directory: Path, used_files: Set[Path]) -> pd.DataFrame:
    ts_path = trajectory_directory / TS_FILENAME
    used_files.add(ts_path)
    return FlowbasedFileReader.read_ts_file(ts_path)


# feature extraction (Load / Wind / Solar / RoR for the 5 hub countries)


def _read_load_series(area: Area) -> pd.DataFrame:
    return area.get_load_matrix()


def _read_combined_res_series(area: Area, groups: set[str]) -> pd.DataFrame:
    """
    Sum matching RES clusters' production (load_factor x enabled capacity), in MW.
    """
    combined: pd.DataFrame | None = None
    for cluster in area.get_renewables().values():
        if map_res_group_to_aw(cluster.properties.group) not in groups:
            continue
        weighted = cluster.get_timeseries() * cluster.properties.enabled_capacity
        combined = weighted if combined is None else combined.add(weighted, fill_value=0.0)

    if combined is None:
        raise FlowbasedGenerationError(f"No matching RES clusters found for hub area '{area.id}', groups={groups}")
    return combined


def _read_ror_series(area: Area) -> pd.DataFrame:
    return area.hydro.get_ror_series()


def _validate_row_count(area: str, series_by_variable: dict[str, pd.DataFrame]) -> None:
    for variable, df in series_by_variable.items():
        if len(df) != EXPECTED_HOURS:
            raise FlowbasedGenerationError(
                f"Expected {EXPECTED_HOURS} hourly rows for hub area '{area}' variable '{variable}', got {len(df)}"
            )


def _validate_column_counts(hub_features: dict[str, dict[str, pd.DataFrame]]) -> None:
    """
    Throws a clear message naming exactly which series don't match, if columns are not
    consistent between all series. (Assumed correct before flowbased)
    """
    column_counts = {
        f"{area}.{variable}": df.shape[1]
        for area, by_variable in hub_features.items()
        for variable, df in by_variable.items()
    }
    if len(set(column_counts.values())) <= 1:
        return

    expected = Counter(column_counts.values()).most_common(1)[0][0]
    outliers = {key: count for key, count in column_counts.items() if count != expected}
    raise FlowbasedGenerationError(
        f"Mismatched scenario column counts: expected {expected}, but these series don't match: {outliers}"
    )


def _build_hub_features(study: Study) -> dict[str, dict[str, pd.DataFrame]]:
    areas = study.get_areas()
    features: dict[str, dict[str, pd.DataFrame]] = {}
    for area_id in HUB_AREAS:
        if area_id not in areas:
            raise FlowbasedGenerationError(f"Hub area '{area_id}' not found in study areas")
        area = areas[area_id]
        series_by_variable = {
            "load": _read_load_series(area),
            "wind": _read_combined_res_series(area, WIND_GROUPS),
            "solar": _read_combined_res_series(area, SOLAR_GROUPS),
            "h_ror": _read_ror_series(area),
        }
        _validate_row_count(area_id, series_by_variable)
        features[area_id] = series_by_variable
    _validate_column_counts(features)
    return features


# rf prediction -> idDayType, per hour and per reference year column. Confirmed against the


def _hourly_season_mask(first_month: Month) -> np.ndarray[Any, np.dtype[np.bool_]]:
    daily_mask = SeasonManager(first_month).is_winter()
    return np.repeat(daily_mask, 24)


def _zscore_pooled(hourly: pd.DataFrame) -> pd.DataFrame:
    """Z-score an (8760, n_columns) hourly df using one mean/std pooled over every value
    (all hours x all columns), matching R's `scale()` in the old generator (sample std, ddof=1).

    A constant series (std == 0) is returned as an all zero series because its z-score is a
    zero division
    """
    values = hourly.to_numpy(dtype=float)
    mean = float(values.mean())
    std = float(values.std(ddof=1))
    if std < 1e-9:  # == 0.0
        return pd.DataFrame(0.0, index=hourly.index, columns=hourly.columns)
    return (hourly - mean) / std


def _build_feature_frame(normalized_features: dict[str, dict[str, pd.DataFrame]], column_index: int) -> pd.DataFrame:
    columns = {
        f"{area}_{variable}_normalized": series.iloc[:, column_index]
        for area, series_by_variable in normalized_features.items()
        for variable, series in series_by_variable.items()
    }
    return pd.DataFrame(columns)


def _predict_column_clusters(
    summer_model: Any, winter_model: Any, features: pd.DataFrame, is_winter: np.ndarray[Any, np.dtype[np.bool_]]
) -> list[str]:
    clusters: pd.Series[Any] = pd.Series(index=features.index, dtype=object)
    if is_winter.any():
        clusters.loc[is_winter] = predict_clusters_batch(winter_model, features.loc[is_winter])
    if (~is_winter).any():
        clusters.loc[~is_winter] = predict_clusters_batch(summer_model, features.loc[~is_winter])
    return list(clusters)


def _map_cluster_to_id_day_type(cluster: str, cluster_to_id_day_type: dict[str, int]) -> int:
    if cluster not in cluster_to_id_day_type:
        raise FlowbasedGenerationError(f"Predicted cluster '{cluster}' has no entry in flowbased.type_days")
    return cluster_to_id_day_type[cluster]


def compute_id_day_types(
    summer_model: Any,
    winter_model: Any,
    hub_features: dict[str, dict[str, pd.DataFrame]],
    type_days: list[dict[str, Any]],
    first_month: Month,
) -> pd.DataFrame:
    """Predict `idDayType` for every hour, for every reference climatic year column.

    Computed once, shared by all 100 FBxxx constraints.

    Returns shape (8760, n_columns), columns 0..n_columns-1.
    """
    is_winter = _hourly_season_mask(first_month)
    cluster_to_id_day_type = {str(entry["clustering"]): int(entry["id_type_day"]) for entry in type_days}
    # Every series shares the same column count. The load series
    # is just the reference point used to read it.
    n_columns = hub_features[HUB_AREAS[0]]["load"].shape[1]

    normalized_features = {
        area: {variable: _zscore_pooled(series) for variable, series in by_variable.items()}
        for area, by_variable in hub_features.items()
    }

    id_day_type_columns = {}
    for column_index in range(n_columns):
        column_features = _build_feature_frame(normalized_features, column_index)
        clusters = _predict_column_clusters(summer_model, winter_model, column_features, is_winter)
        id_day_type_columns[column_index] = [
            _map_cluster_to_id_day_type(cluster, cluster_to_id_day_type) for cluster in clusters
        ]

    return pd.DataFrame(id_day_type_columns)


# idDayType -> RHS

FILTER_ID_HOUR = 16


def build_vect_b_lookup_table(second_member: pd.DataFrame) -> dict[str, dict[int, float]]:
    """Build the full `{constraint_name: {id_day: vect_b}}` lookup for the whole file.

    The generator filters on `Id_hour == 16` by default
    """
    representative_rows = second_member[second_member["id_hour"] == FILTER_ID_HOUR]
    lookup: dict[str, dict[int, float]] = {}
    for (name, id_day), group in representative_rows.groupby(["name", "id_day"]):
        if len(group) > 1:
            raise FlowbasedGenerationError(
                f"Multiple Id_hour=={FILTER_ID_HOUR} rows for constraint='{name}', id_day={id_day}"
            )
        lookup.setdefault(cast(str, name), {})[cast(int, id_day)] = float(group["vect_b"].iloc[0])
    return lookup


def build_rhs_matrix(
    id_day_types: pd.DataFrame, vect_b_lookup: dict[str, dict[int, float]], constraint_name: str
) -> pd.DataFrame:
    vect_b_by_id_day = vect_b_lookup.get(constraint_name)
    if not vect_b_by_id_day:
        raise FlowbasedGenerationError(f"No second_member rows found for constraint '{constraint_name}'")

    rhs: pd.DataFrame = id_day_types.apply(lambda column: column.map(vect_b_by_id_day))
    if rhs.isna().any(axis=None):
        raise FlowbasedGenerationError(f"idDayType value has no matching vect_b for constraint '{constraint_name}'")
    return rhs


# Extra rows must be zero padded before being handed to create_binding_constraint
BINDING_CONSTRAINT_HOURLY_ROWS = 8784


def _pad_to_binding_constraint_hourly_rows(matrix: pd.DataFrame) -> pd.DataFrame:
    if len(matrix) != EXPECTED_HOURS:
        raise FlowbasedGenerationError(f"Expected {EXPECTED_HOURS} rows before padding, got {len(matrix)}")
    padding = pd.DataFrame(0.0, index=range(BINDING_CONSTRAINT_HOURLY_ROWS - EXPECTED_HOURS), columns=matrix.columns)
    return pd.concat([matrix, padding], ignore_index=True)


# weight.txt -> binding constraint terms


def _build_constraint_terms(
    weight_row: pd.Series[Any], flowbased_data: dict[str, Any], areas: Any
) -> list[ConstraintTerm]:
    terms = []
    virtual_nodes = flowbased_data.get("virtual_nodes")
    valid_nodes = set(virtual_nodes or ()) | set(areas or ())

    for link_column, coefficient in weight_row.items():
        area1, separator, area2 = str(link_column).partition(".")

        # 1. Vérification du format en premier
        if not separator or not area1 or not area2:
            raise FlowbasedGenerationError(f"Unexpected weight.txt column name '{link_column}', expected 'area1.area2'")

        # 2. Filtrage des nœuds ignorés / non présents
        if area1 not in valid_nodes or area2 not in valid_nodes:
            continue

        terms.append(
            ConstraintTerm(
                data=LinkData(area1=area1.lower(), area2=area2.lower()),
                weight=float(coefficient),
            )
        )
    return terms


# Scenario builder part


def _wire_scenario_builder(study: Study, group_name: str, n_columns: int, nb_years: int) -> None:
    scenario_builder = study.get_scenario_builder()
    group_matrix = scenario_builder.binding_constraint.get_group(group_name)
    group_matrix.set_new_scenario([year % n_columns for year in range(nb_years)])
    study.set_scenario_builder(scenario_builder)
    logger.info(f"Wired flowbased scenario builder group={group_name} nb_years={nb_years} n_columns={n_columns}")


def create_restriction_ahc(study: Study, limitation_mw: float = 10000.0) -> None:
    """Crée le cluster thermique virtuel et la contrainte couplante restriction_ahc.

    Équation :
        1 * (ch%fr) - 1 * (fr%itn) - 1 * (fr%zz_flowbased) - 1 * (model_description_fb.restriction_ahc) <= 0
    """
    area_name = "model_description_fb"
    cluster_name = "restriction_ahc"

    # 1. Récupération de la zone et création du cluster thermique virtuel
    area_obj = study.get_areas()[area_name]

    # Création du cluster thermique avec sa capacité nominale (ex: 10000 MW)
    area_obj.create_thermal_cluster(
        thermal_name=cluster_name,
        properties=ThermalClusterProperties(
            nominal_capacity=limitation_mw,
            unit_count=1,
            enabled=True,
        ),
    )
    logger.info(f"Created virtual thermal cluster {cluster_name} in area {area_name}")

    # 2. Définition des propriétés de la contrainte couplante
    properties = BindingConstraintProperties(
        enabled=True,
        time_step=BindingConstraintFrequency.HOURLY,
        operator=BindingConstraintOperator.LESS,
    )

    # 3. Définition des termes (coefficients PTDF et cluster)
    terms = [
        ConstraintTerm(data=LinkData(area1="CH", area2="FR"), weight=1.0),
        ConstraintTerm(data=LinkData(area1="FR", area2="ITN"), weight=-1.0),
        ConstraintTerm(data=LinkData(area1="FR", area2="zz_flowbased"), weight=-1.0),
        ConstraintTerm(
            data=ClusterData(area=area_name, cluster=cluster_name),
            weight=-1.0,
        ),
    ]

    # 4. Matrice second membre (RHS) : 0 pour 8760 heures
    # Le cluster virtuel avec coefficient -1 porte la limitation
    less_term_matrix = pd.DataFrame(np.zeros((8784, 1)))

    # 5. Création de la contrainte couplante dans l'étude
    study.create_binding_constraint(
        name="restriction_ahc",
        properties=properties,
        terms=terms,
        less_term_matrix=less_term_matrix,
    )
    logger.info("Created restriction_ahc binding constraint")
