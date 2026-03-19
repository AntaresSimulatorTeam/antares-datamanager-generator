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
from typing import Any

import numpy as np
import pandas as pd

from antares.craft.model.area import Area
from antares.datamanager.core.settings import settings
from antares.datamanager.exceptions.exceptions import MiscGenerationError
from antares.datamanager.logs.logging_setup import get_logger

logger = get_logger(__name__)

# AW expects 8 columns in this order :
# CHP/PSP/Row balance are ignord for now (0)
MISC_COLUMNS = ["CHP", "BioMass", "Bi Gas", "Waste", "GeoThermal", "Other", "PSP", "Row balance"]
EXPECTED_HOURS = 8760

# Misc load factor scaling (for a value between 0 and 1)
MISC_LOAD_FACTOR_CONVERSION_FACT = 1000.0

# Source misc group -> Antares misc-gen column
GROUP_TO_COLUMN = {
    "biomass": "BioMass",
    "biogas": "Bi Gas",
    "hydrokinetic": "Other",
    "waste": "Waste",
    "other": "Other",
    "geothermal": "GeoThermal",
    "wave": "Other",
}


def generate_misc_timeseries(area_obj: Area, area_name: str, misc: dict[str, Any]) -> None:
    matrix = build_misc_timeseries_matrix(area_name, misc)
    if matrix.shape[1] != len(MISC_COLUMNS):
        raise ValueError(
            f"Invalid MISC matrix width for area='{area_name}': expected {len(MISC_COLUMNS)}, got {matrix.shape[1]}"
        )
    area_obj.set_misc_gen(matrix)


def build_misc_timeseries_matrix(area_name: str, misc: dict[str, Any]) -> pd.DataFrame:
    matrix = pd.DataFrame(np.zeros((EXPECTED_HOURS, len(MISC_COLUMNS)), dtype=np.float64), columns=MISC_COLUMNS)

    if not misc:
        return matrix

    unmapped_groups_found: set[str] = set()

    base_dir = settings.misc_ts_directory
    for group_name, group_values in misc.items():
        validated_group_values = _validate_misc_group_values(group_values, area_name, group_name)

        normalized_group = _normalize_group_name(group_name)
        target_column = GROUP_TO_COLUMN.get(normalized_group)
        if target_column is None:
            # CHP/PSP/Row ignored here
            unmapped_groups_found.add(normalized_group)
            continue

        load_factor = _read_load_factor_series(base_dir, area_name, group_name, validated_group_values)
        if load_factor is None:
            continue

        capacity = _read_capacity(validated_group_values, area_name, group_name)
        normalized_load_factor = load_factor / MISC_LOAD_FACTOR_CONVERSION_FACT
        # TODO: uncomment to validate load factor values are between 0 and 1 (disabled foer now)
        # _validate_normalized_load_factor(normalized_load_factor, area_name, group_name)

        # ts_values = load_factor_mean X puissance total (0 < load_factor < 1)
        matrix[target_column] += normalized_load_factor * capacity

    if unmapped_groups_found:
        logger.debug(
            "Ignored MISC groups for area=%s groups=%s",
            area_name,
            sorted(unmapped_groups_found),
        )

    return matrix


def _validate_misc_group_values(group_values: Any, area_name: str, group_name: str) -> dict[str, Any]:
    if not isinstance(group_values, dict):
        raise MiscGenerationError(
            f"Invalid MISC group data for area='{area_name}', group='{group_name}': expected object, got {type(group_values).__name__}"
        )

    return group_values


def _read_load_factor_series(
    base_dir: Path,
    area_name: str,
    group_name: str,
    group_values: dict[str, Any],
) -> pd.Series[Any] | None:
    raw_series_files = group_values.get("series", [])
    if isinstance(raw_series_files, str):
        series_files = [raw_series_files]
    elif isinstance(raw_series_files, list):
        if any(not isinstance(file_name, str) for file_name in raw_series_files):
            raise MiscGenerationError(
                f"Invalid MISC series list for area='{area_name}', group='{group_name}': expected string[]"
            )
        series_files = raw_series_files
    else:
        raise MiscGenerationError(f"Invalid MISC series list for area='{area_name}', group='{group_name}'")

    if not series_files:
        return None

    if len(series_files) > 1:
        raise MiscGenerationError(
            f"Expected one MISC series file for area='{area_name}', group='{group_name}', got {len(series_files)}"
        )

    filename = series_files[0]
    file_path = _resolve_and_validate_misc_path(base_dir, filename)
    df = pd.read_feather(file_path)
    return _extract_hourly_series(df, area_name, group_name, filename)


def _extract_hourly_series(
    df: pd.DataFrame,
    area_name: str,
    group_name: str,
    filename: str,
) -> pd.Series[Any]:
    if df.empty or df.shape[1] < 1:
        raise MiscGenerationError(
            f"MISC .arrow file has no time series column for area='{area_name}', group='{group_name}', file='{filename}'"
        )

    if len(df.index) != EXPECTED_HOURS:
        raise MiscGenerationError(
            f"MISC .arrow file must contain {EXPECTED_HOURS} rows for area='{area_name}', "
            f"group='{group_name}', file='{filename}', got {len(df.index)}"
        )

    first_column = df.iloc[:, 0]
    try:
        return pd.to_numeric(first_column, errors="raise")
    except (TypeError, ValueError) as exc:
        raise MiscGenerationError(
            f"MISC .arrow file contains invalid values for area='{area_name}', group='{group_name}', file='{filename}'"
        ) from exc


def _read_capacity(group_values: dict[str, Any], area_name: str, group_name: str) -> float:
    raw_capacity = group_values.get("properties", {}).get("capacity", 0)
    try:
        capacity = float(raw_capacity)
    except (TypeError, ValueError) as exc:
        raise MiscGenerationError(
            f"Invalid MISC capacity for area='{area_name}', group='{group_name}': {raw_capacity}"
        ) from exc

    return capacity


def _validate_normalized_load_factor(load_factor: pd.Series[Any], area_name: str, group_name: str) -> None:
    invalid_mask = ~load_factor.between(0.0, 1.0)
    if not invalid_mask.any():
        return

    raise MiscGenerationError(
        "Invalid MISC load factor for "
        f"area='{area_name}', group='{group_name}': value must be betwween [0, 1], "
        f"got min={float(load_factor.min())}, max={float(load_factor.max())}"
    )


def _resolve_and_validate_misc_path(base_dir: Path, filename: str) -> Path:
    if not filename.endswith(".arrow"):
        raise MiscGenerationError(f"Unexpected MISC file extension for '{filename}'")

    base_resolved = base_dir.resolve()
    file_path = (base_resolved / filename).resolve()

    if base_resolved != file_path and base_resolved not in file_path.parents:
        raise MiscGenerationError(f"MISC series path outside allowed directory: '{filename}'")

    if not file_path.exists():
        raise FileNotFoundError(f"MISC series file not found: {file_path}")

    return file_path


def _normalize_group_name(group_name: str) -> str:
    return str(group_name).strip().lower()
