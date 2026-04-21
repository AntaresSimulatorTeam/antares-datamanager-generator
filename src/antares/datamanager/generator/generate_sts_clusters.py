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
from typing import Any, Dict

import pandas as pd

from antares.craft import (
    AdditionalConstraintOperator,
    AdditionalConstraintVariable,
    Area,
    Occurrence,
    STStorageAdditionalConstraint,
    STStorageProperties,
)
from antares.datamanager.core.settings import settings
from antares.datamanager.logs.logging_setup import configure_ecs_logger, get_logger

# Configurer le logger au démarrage du module (ou appeler configure_ecs_logger() dans le main)
configure_ecs_logger()
logger = get_logger(__name__)


STS_CSV_MARKER = ".csv"
STS_ENABLED_BY_VALUE = {"true": True, "false": False}
STS_OPERATOR_BY_VALUE = {operator.value: operator for operator in AdditionalConstraintOperator}
STS_VARIABLE_BY_VALUE = {variable.value: variable for variable in AdditionalConstraintVariable}


def _resolve_sts_file_path(base_dir: Path, filename: str, cluster_name: str, file_kind: str) -> Path:
    if not isinstance(filename, str) or not filename.strip():
        raise ValueError(f"Invalid {file_kind} filename for cluster '{cluster_name}': {filename!r}")

    file_name_path = Path(filename)
    if file_name_path.is_absolute() or ".." in file_name_path.parts:
        raise ValueError(f"Unsafe {file_kind} filename for cluster '{cluster_name}': {filename!r}")

    file_path = (base_dir / file_name_path).resolve(strict=False)
    base_dir_resolved = base_dir.resolve(strict=False)
    if base_dir_resolved not in file_path.parents and file_path != base_dir_resolved:
        raise ValueError(f"Unsafe {file_kind} path for cluster '{cluster_name}': {file_path}")

    if not file_path.exists():
        raise FileNotFoundError(f"STS {file_kind} file not found for cluster '{cluster_name}': {file_path}")

    return file_path


def _extract_sts_series(values: Dict[str, Any], cluster_name: str) -> list[str]:
    raw_series = values.get("series", [])
    if isinstance(raw_series, dict):
        raw_series = raw_series.get("series", [])

    if raw_series is None:
        return []
    if not isinstance(raw_series, list):
        raise ValueError(f"Invalid STS series payload for cluster '{cluster_name}': expected list")

    return raw_series


def _extract_matrix(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if df.shape[1] > 1:
        return df.iloc[:, [1]]
    return df.iloc[:, [0]]


def _parse_enabled(value: Any, cluster_name: str, constraint_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in STS_ENABLED_BY_VALUE:
            return STS_ENABLED_BY_VALUE[normalized]
    raise ValueError(
        f"Invalid enabled value for STS constraint '{constraint_name}' in cluster '{cluster_name}': {value!r}"
    )


def _parse_operator(value: Any, cluster_name: str, constraint_name: str) -> AdditionalConstraintOperator:
    if not isinstance(value, str):
        raise ValueError(
            f"Invalid operator for STS constraint '{constraint_name}' in cluster '{cluster_name}': {value!r}"
        )

    normalized = value.strip().lower()
    if normalized not in STS_OPERATOR_BY_VALUE:
        raise ValueError(
            f"Unsupported operator for STS constraint '{constraint_name}' in cluster '{cluster_name}': {value!r}"
        )
    return STS_OPERATOR_BY_VALUE[normalized]


def _parse_variable(value: Any, cluster_name: str, constraint_name: str) -> AdditionalConstraintVariable:
    if not isinstance(value, str):
        raise ValueError(
            f"Invalid variable for STS constraint '{constraint_name}' in cluster '{cluster_name}': {value!r}"
        )

    normalized = value.strip().lower()
    if normalized not in STS_VARIABLE_BY_VALUE:
        raise ValueError(
            f"Unsupported variable for STS constraint '{constraint_name}' in cluster '{cluster_name}': {value!r}"
        )
    return STS_VARIABLE_BY_VALUE[normalized]


def _parse_occurrences(raw_hours: Any, cluster_name: str, constraint_name: str) -> list[Occurrence]:
    if raw_hours is None:
        return []
    if not isinstance(raw_hours, list):
        raise ValueError(
            f"Invalid hours payload for STS constraint '{constraint_name}' in cluster '{cluster_name}': expected list"
        )

    occurrences: list[Occurrence] = []
    for index, occurrence_hours in enumerate(raw_hours, start=1):
        if not isinstance(occurrence_hours, list):
            raise ValueError(
                f"Invalid hours block #{index} for STS constraint '{constraint_name}' in cluster '{cluster_name}'"
            )

        sanitized_hours: list[int] = []
        for hour in occurrence_hours:
            if not isinstance(hour, int) or hour <= 0:
                raise ValueError(
                    f"Invalid hour value in STS constraint '{constraint_name}' for cluster '{cluster_name}': {hour!r}"
                )
            sanitized_hours.append(hour)

        occurrences.append(Occurrence(hours=sanitized_hours))

    return occurrences


def _extract_constraint_name_from_series_file(filename: str) -> str | None:
    basename = Path(filename).name
    lower_basename = basename.lower()
    marker_index = lower_basename.find(STS_CSV_MARKER)
    if marker_index <= 0:
        return None
    return basename[:marker_index].lower()


def _create_sts_additional_constraints(storage: Any, values: Dict[str, Any], base_dir: Path, cluster_name: str) -> None:
    raw_constraints = values.get("constraintParameters")
    if raw_constraints is None:
        return
    if not isinstance(raw_constraints, dict):
        raise ValueError(f"Invalid constraintParameters for cluster '{cluster_name}': expected object")

    raw_series_list = values.get("stsConstraintsSeriesList", [])
    if not isinstance(raw_series_list, list):
        raise ValueError(f"Invalid stsConstraintsSeriesList for cluster '{cluster_name}': expected list")

    series_by_constraint_name: dict[str, str] = {}
    for filename in raw_series_list:
        if not isinstance(filename, str):
            raise ValueError(
                f"Invalid RHS filename in stsConstraintsSeriesList for cluster '{cluster_name}': {filename!r}"
            )

        constraint_name = _extract_constraint_name_from_series_file(filename)
        if constraint_name is None:
            continue

        if constraint_name in series_by_constraint_name:
            raise ValueError(f"Duplicate RHS series for STS constraint '{constraint_name}' in cluster '{cluster_name}'")
        series_by_constraint_name[constraint_name] = filename

    for constraint_name, constraint_data in raw_constraints.items():
        if not isinstance(constraint_name, str) or not constraint_name.strip():
            raise ValueError(f"Invalid STS constraint name for cluster '{cluster_name}': {constraint_name!r}")
        if not isinstance(constraint_data, dict):
            raise ValueError(
                f"Invalid STS constraint payload for '{constraint_name}' in cluster '{cluster_name}': expected object"
            )

        constraint = STStorageAdditionalConstraint(
            name=constraint_name,
            variable=_parse_variable(constraint_data.get("variable"), cluster_name, constraint_name),
            operator=_parse_operator(constraint_data.get("operator"), cluster_name, constraint_name),
            occurrences=_parse_occurrences(constraint_data.get("hours"), cluster_name, constraint_name),
            enabled=_parse_enabled(constraint_data.get("enabled", True), cluster_name, constraint_name),
        )
        storage.create_constraints([constraint])

        rhs_filename = series_by_constraint_name.get(constraint_name.lower())
        if rhs_filename is None:
            raise FileNotFoundError(
                f"No RHS series found for STS constraint '{constraint_name}' in cluster '{cluster_name}'"
            )

        rhs_path = _resolve_sts_file_path(base_dir, rhs_filename, cluster_name, "constraint RHS matrix")
        rhs_df = pd.read_feather(rhs_path)
        storage.set_constraint_term(constraint_name, _extract_matrix(rhs_df))


def generate_sts_clusters(area_obj: Area, sts: Dict[str, Any]) -> None:
    # Short-term storage clusters
    for cluster_name, values in sts.items():
        logger.info("Creating sts cluster : ", cluster_name)
        properties = values.get("properties", {})
        st_storage_properties = STStorageProperties(**properties)

        sts_series = _extract_sts_series(values, cluster_name)

        storage = area_obj.create_st_storage(cluster_name, st_storage_properties)
        matrix_setter_map = {
            "inflows": storage.set_storage_inflows,
            "lower_curve": storage.set_lower_rule_curve,
            "Pmax_injection": storage.update_pmax_injection,
            "Pmax_soutirage": storage.set_pmax_withdrawal,
            "upper_curve": storage.set_upper_rule_curve,
        }

        base_dir = settings.sts_ts_directory

        for filename in sts_series:
            prefix = filename.split(".")[0]
            setter = matrix_setter_map.get(prefix)
            if not setter:
                continue

            file_path = _resolve_sts_file_path(base_dir, filename, cluster_name, "matrix")

            df = pd.read_feather(file_path)
            matrix = _extract_matrix(df)
            setter(matrix)

        _create_sts_additional_constraints(storage, values, base_dir, cluster_name)
