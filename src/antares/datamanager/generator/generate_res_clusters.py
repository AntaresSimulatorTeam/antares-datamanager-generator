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
from typing import Any, Mapping

import numpy as np
import pandas as pd

from antares.craft.model.area import Area
from antares.craft.model.renewable import RenewableClusterProperties, TimeSeriesInterpretation
from antares.datamanager.core.settings import settings
from antares.datamanager.exceptions.exceptions import RESGenerationError
from antares.datamanager.logs.logging_setup import get_logger

logger = get_logger(__name__)

EXPECTED_HOURS = 8760
PRODUCTION_FACTOR = "production-factor"

# Input group -> AW renewable group
RES_GROUP_TO_AW = {
    "wind_onshore": "Wind Onshore",
    "wind_offshore": "Wind Offshore",
    "solar_pv": "Solar PV",
    "solar_thermo": "Solar Thermal",
}

RES_VALUE_COLUMN_INDEX = 0


def map_res_group_to_aw(group: str) -> str:
    normalized = str(group).strip().lower()
    if normalized in RES_GROUP_TO_AW:
        return RES_GROUP_TO_AW[normalized]

    # Accept already-normalized AW labels too.
    for label in RES_GROUP_TO_AW.values():
        if normalized == label.lower():
            return label

    raise RESGenerationError(f"Unsupported RES group '{group}'")


def resolve_res_capacity_and_enabled(
    *,
    installed_power: Any,
) -> tuple[float, bool]:
    capacity = 0.0
    if installed_power is None:
        return 0.0, False
    else:
        capacity = _to_float_capacity(installed_power)

    if capacity < 0:
        raise RESGenerationError(f"Negative installed power: {capacity}")

    enabled = capacity > 0.0
    return capacity, enabled


def read_res_hourly_series(
    *,
    base_dir: Path,
    filename: str,
    expected_rows: int = EXPECTED_HOURS,
    value_column_index: int = 0,
) -> pd.DataFrame:
    """
    Read an .arrow file and return a DataFrame containing all TS
    columns.
    """
    file_path = resolve_and_validate_res_arrow_path(base_dir, filename)
    df = pd.read_feather(file_path)

    if df.empty or df.shape[1] < 1:
        raise RESGenerationError(f"RES .arrow file has no time series columns for file='{filename}'")

    if len(df.index) != expected_rows:
        raise RESGenerationError(
            f"RES .arrow file must contain {expected_rows} rows for file='{filename}', got {len(df.index)}"
        )

    # If the first column is not TS and all remaining columns are
    # drop the first column.
    numeric_mask = df.apply(lambda col: pd.to_numeric(col, errors="coerce").notna().all())
    if df.shape[1] > 1 and not bool(numeric_mask.iloc[0]) and bool(numeric_mask.iloc[1:].all()):
        ts_df = df.iloc[:, 1:].copy()
    else:
        ts_df = df.loc[:, numeric_mask].copy()

    if ts_df.shape[1] == 0:
        raise RESGenerationError(f"RES .arrow file contains no numeric time series for file='{filename}'")

    ts_df = ts_df.apply(lambda col: pd.to_numeric(col, errors="coerce")).astype(float)
    if ts_df.isna().any(axis=None):
        raise RESGenerationError(f"RES .arrow file contains non-numeric values for file='{filename}'")

    out_of_bounds_mask = (ts_df < 0.0) | (ts_df > 1.0)
    if out_of_bounds_mask.any(axis=None):
        min_v = float(ts_df.min().min())
        max_v = float(ts_df.max().max())
        raise RESGenerationError(
            f"RES .arrow values out of bounds [0,1] for file='{filename}' (min={min_v}, max={max_v})"
        )

    return ts_df


def resolve_and_validate_res_arrow_path(
    base_dir: Path,
    filename: str,
    allowed_extensions: tuple[str, ...] = (".arrow",),
) -> Path:
    if not isinstance(filename, str) or not filename:
        raise RESGenerationError("RES series filename must be a non-empty string")

    if not filename.endswith(allowed_extensions):
        raise RESGenerationError(
            f"Unexpected RES file extension for '{filename}', expected one of {allowed_extensions}"
        )

    base_resolved = base_dir.resolve()
    file_path = (base_resolved / filename).resolve()

    if base_resolved != file_path and base_resolved not in file_path.parents:
        raise RESGenerationError(f"RES series path outside allowed directory: '{filename}'")

    if not file_path.exists():
        raise FileNotFoundError(f"RES series file not found: {file_path}")

    return file_path


def compute_fr_weighted_load_factor(
    *,
    techno_series_by_zone: Mapping[str, Mapping[str, pd.DataFrame]],
    techno_weights_by_zone: Mapping[str, Mapping[str, float]],
    zonal_weights: Mapping[str, float],
) -> pd.DataFrame:
    if not zonal_weights:
        raise RESGenerationError("zonal_weights is empty")

    zone_averages = _compute_zone_averages(
        techno_series_by_zone=techno_series_by_zone,
        techno_weights_by_zone=techno_weights_by_zone,
        zonal_weights=zonal_weights,
    )

    if not zone_averages:
        raise RESGenerationError("No usable zone averages for FR weighted load factor")

    return _compute_global_weighted_series(zone_averages=zone_averages, zonal_weights=zonal_weights)


def build_res_cluster_payload(
    *,
    area_name: str,
    cluster_name: str,
    aw_group: str,
    capacity_mw: float,
    enabled: bool,
    ts_interpretation: str = PRODUCTION_FACTOR,
    unit_count: int = 1,
) -> dict[str, Any]:
    nominal = float(capacity_mw)
    return {
        "area": area_name,
        "cluster": cluster_name,
        "group": aw_group,
        "enabled": bool(enabled),
        "ts_interpretation": ts_interpretation,
        "unit_count": unit_count,
        "nominal_capacity": nominal,
    }


def generate_res_clusters(area_obj: Area, area_name: str, res: dict[str, Any]) -> None:
    """
    Expected `res` json :
    {
      "wind_onshore": {
        "properties": {
          "capacity": 5000,
          "group": "wind_onshore"
        },
        "series": ["NAME.arrow"]
      }
    }
    for FR only:
    {
      "wind_onshore": {
        "properties": {
          "capacity": 5000,
          "group": "wind_onshore"
        },
        "series": []
        "fr_aggregation": {
            "zone_weights": {
              "FR01": 0.2,
              "FRO2": 0.0,
            },
            "tech_weights_by_zone": {
              "FR01": {
                "offshore_tech1": 0.6,
                "offshore_tech2": 0.4
              },
            },
            "series_by_zone_and_tech": {
              "FR01": {
                "offshore_tech1": "fr01_offshore_tech1.arrow",
                "offshore_tech2": "fr01_offshore_tech2.arrow"
              },
            }
          }
      }
    }
    """
    base_ts_directory = _resolve_res_base_directory()

    if not res:
        return

    if not isinstance(res, Mapping):
        raise RESGenerationError(f"Invalid RES payload for area='{area_name}': expected object")

    normalized_area_name = str(area_name).strip().upper()

    for group_key, group_values in res.items():
        payload, validated_series = _process_res_entry(
            area_name=area_name,
            normalized_area_name=normalized_area_name,
            group_key=group_key,
            group_values=group_values,
            base_ts_directory=base_ts_directory,
        )

        logger.info("Prepared RES cluster payload area=%s group=%s payload=%s", area_name, group_key, payload)
        _register_res_outputs(
            area_obj=area_obj, group_key=group_key, payload=payload, validated_series=validated_series
        )


def _resolve_res_base_directory() -> Path:
    base_ts_directory = settings.res_ts_directory
    if not isinstance(base_ts_directory, Path):
        raise RESGenerationError("res_ts_directory must be a Path")
    return base_ts_directory


def _validate_series_list(*, area_name: str, group_key: str, raw_series: Any) -> list[str]:
    if not isinstance(raw_series, list) or any(not isinstance(item, str) for item in raw_series):
        raise RESGenerationError(
            f"Invalid RES series list for area='{area_name}', group='{group_key}': expected string[]"
        )

    return raw_series


def _extract_res_properties(*, area_name: str, group_key: str, group_values: Mapping[str, Any]) -> tuple[str, Any]:
    properties = group_values.get("properties")
    if not isinstance(properties, Mapping):
        raise RESGenerationError(f"Invalid RES properties for area='{area_name}', group='{group_key}': expected object")

    if "group" not in properties:
        raise RESGenerationError(f"Missing RES properties.group for area='{area_name}', group='{group_key}'")
    if "capacity" not in properties:
        raise RESGenerationError(f"Missing RES properties.capacity for area='{area_name}', group='{group_key}'")

    return str(properties.get("group", "")), properties.get("capacity")


def _validate_group_key_matches_group(*, area_name: str, group_key: str, group_name: str) -> None:
    if str(group_key).strip().lower() != str(group_name).strip().lower():
        raise RESGenerationError(
            f"RES entry key must match properties.group; area='{area_name}', key='{group_key}', group='{group_name}'"
        )


def _build_fr_weighted_series_from_aggregation(
    *,
    area_name: str,
    group_key: str,
    raw_fr_aggregation: Any,
    base_ts_directory: Path,
) -> pd.DataFrame:
    if not isinstance(raw_fr_aggregation, Mapping):
        raise RESGenerationError(f"Missing or invalid fr_aggregation for FR area='{area_name}', group='{group_key}'")

    required_keys = {"zone_weights", "tech_weights_by_zone", "series_by_zone_and_tech"}
    missing_keys = sorted(key for key in required_keys if key not in raw_fr_aggregation)
    if missing_keys:
        raise RESGenerationError(
            f"Missing FR aggregation keys {missing_keys} for area='{area_name}', group='{group_key}'"
        )

    zone_weights = _parse_zone_weights(
        area_name=area_name,
        group_key=group_key,
        raw_zone_weights=raw_fr_aggregation.get("zone_weights"),
    )

    tech_weights_by_zone = _parse_tech_weights_by_zone(
        area_name=area_name,
        group_key=group_key,
        raw_tech_weights_by_zone=raw_fr_aggregation.get("tech_weights_by_zone", {}),
        expected_zones=set(zone_weights.keys()),
    )

    techno_series_by_zone = _load_tech_series_by_zone(
        area_name=area_name,
        group_key=group_key,
        raw_series_by_zone_and_tech=raw_fr_aggregation.get("series_by_zone_and_tech", {}),
        expected_zones=set(zone_weights.keys()),
        expected_techs_by_zone={zone: set(techs.keys()) for zone, techs in tech_weights_by_zone.items()},
        base_ts_directory=base_ts_directory,
    )

    return compute_fr_weighted_load_factor(
        techno_series_by_zone=techno_series_by_zone,
        techno_weights_by_zone=tech_weights_by_zone,
        zonal_weights=zone_weights,
    )


def _parse_zone_weights(*, area_name: str, group_key: str, raw_zone_weights: Any) -> dict[str, float]:
    if not isinstance(raw_zone_weights, Mapping) or not raw_zone_weights:
        raise RESGenerationError(f"Invalid zone_weights for area='{area_name}', group='{group_key}'")

    zone_weights: dict[str, float] = {}
    for raw_zone, raw_weight in raw_zone_weights.items():
        zone = str(raw_zone).strip().upper()
        if not _is_valid_fr_zone(zone):
            raise RESGenerationError(f"Invalid FR zone key '{raw_zone}' for area='{area_name}', group='{group_key}'")
        weight = _to_float_capacity(raw_weight)
        if weight < 0:
            raise RESGenerationError(f"Negative zone weight for zone='{zone}', area='{area_name}', group='{group_key}'")
        zone_weights[zone] = weight

    if sum(zone_weights.values()) <= 0:
        raise RESGenerationError(f"Sum of zone_weights must be > 0 for area='{area_name}', group='{group_key}'")

    return zone_weights


def _parse_tech_weights_by_zone(
    *,
    area_name: str,
    group_key: str,
    raw_tech_weights_by_zone: Any,
    expected_zones: set[str],
) -> dict[str, dict[str, float]]:
    if not isinstance(raw_tech_weights_by_zone, Mapping):
        raise RESGenerationError(f"Invalid tech_weights_by_zone for area='{area_name}', group='{group_key}'")

    incoming_zones = {str(zone).strip().upper() for zone in raw_tech_weights_by_zone.keys()}
    for zone in incoming_zones:
        if zone not in expected_zones:
            raise RESGenerationError(
                f"Zone '{zone}' in tech_weights_by_zone not found in zone_weights for "
                f"area='{area_name}', group='{group_key}'"
            )

    parsed: dict[str, dict[str, float]] = {}
    for raw_zone, raw_tech_weights in raw_tech_weights_by_zone.items():
        zone = str(raw_zone).strip().upper()
        parsed[zone] = _parse_single_zone_tech_weights(
            zone=zone,
            raw_tech_weights=raw_tech_weights,
            area_name=area_name,
            group_key=group_key,
        )

    return parsed


def _load_tech_series_by_zone(
    *,
    area_name: str,
    group_key: str,
    raw_series_by_zone_and_tech: Any,
    expected_zones: set[str],
    expected_techs_by_zone: Mapping[str, set[str]],
    base_ts_directory: Path,
) -> dict[str, dict[str, Any]]:
    if not isinstance(raw_series_by_zone_and_tech, Mapping):
        raise RESGenerationError(f"Invalid series_by_zone_and_tech for area='{area_name}', group='{group_key}'")

    incoming_zones = {str(zone).strip().upper() for zone in raw_series_by_zone_and_tech.keys()}
    for zone in incoming_zones:
        if zone not in expected_zones:
            raise RESGenerationError(
                f"Zone '{zone}' in series_by_zone_and_tech not found in zone_weights for "
                f"area='{area_name}', group='{group_key}'"
            )

    series_by_zone: dict[str, dict[str, pd.DataFrame]] = {}
    for raw_zone, raw_series_by_tech in raw_series_by_zone_and_tech.items():
        zone = str(raw_zone).strip().upper()
        if not isinstance(raw_series_by_tech, Mapping):
            raise RESGenerationError(
                f"Invalid technology series map for zone='{zone}', area='{area_name}', group='{group_key}'"
            )

        tech_keys = {str(key).strip() for key in raw_series_by_tech.keys()}
        expected_techs = expected_techs_by_zone.get(zone, set())

        # Verify that all series tech keys are a subset of expected techs
        if not tech_keys.issubset(expected_techs):
            unexpected_techs = tech_keys - expected_techs
            raise RESGenerationError(
                f"Unexpected technology keys {unexpected_techs} in series_by_zone_and_tech for zone='{zone}', "
                f"area='{area_name}', group='{group_key}'"
            )

        loaded_by_tech: dict[str, Any] = {}
        for tech, filename in raw_series_by_tech.items():
            loaded_by_tech[str(tech).strip()] = read_res_hourly_series(
                base_dir=base_ts_directory,
                filename=str(filename),
                expected_rows=EXPECTED_HOURS,
                value_column_index=RES_VALUE_COLUMN_INDEX,
            )
        series_by_zone[zone] = loaded_by_tech

    return series_by_zone


def _is_valid_fr_zone(zone: str) -> bool:
    if not zone.startswith("FR"):
        return False
    suffix = zone[2:]
    if not suffix.isdigit():
        return False
    zone_index = int(suffix)
    return 1 <= zone_index <= 26


def _coerce_numeric_df(*, df: pd.DataFrame | pd.Series, zone: str, tech: str) -> pd.DataFrame:
    if isinstance(df, pd.Series):
        series = pd.to_numeric(df, errors="coerce")
        if series.isna().any():
            raise RESGenerationError(f"Non numeric values for zone='{zone}', tech='{tech}'")
        return series.astype(float).to_frame(name=(series.name or "value"))

    numeric = df.apply(lambda col: pd.to_numeric(col, errors="coerce"))
    if numeric.isna().any(axis=None):
        raise RESGenerationError(f"Non numeric values for zone='{zone}', tech='{tech}'")
    return numeric.astype(float)


def _compute_zone_average(
    *,
    zone: str,
    tech_weights: Mapping[str, float],
    series_by_tech: Mapping[str, Any],
) -> pd.DataFrame:
    if not tech_weights:
        raise RESGenerationError(f"No technology weights for zone='{zone}'")

    weighted_sum: pd.DataFrame | None = None
    weight_sum = 0.0
    has_positive_weight = False

    for tech, tech_weight in tech_weights.items():
        if tech_weight < 0:
            raise RESGenerationError(f"Negative technology weight for zone='{zone}', tech='{tech}'")

        # Skip techs with weight 0
        if tech_weight == 0:
            continue

        has_positive_weight = True
        series = series_by_tech.get(tech)
        if series is None:
            raise RESGenerationError(f"Missing technology series for zone='{zone}', tech='{tech}'")
        numeric_series = _coerce_numeric_df(df=series, zone=zone, tech=tech)
        if weighted_sum is None:
            weighted_sum = pd.DataFrame(
                np.zeros((len(numeric_series), len(numeric_series.columns)), dtype=np.float64),
                columns=numeric_series.columns,
            )
        else:
            if len(numeric_series) != len(weighted_sum):
                raise RESGenerationError(f"Inconsistent series length in zone='{zone}', tech='{tech}'")
            if list(numeric_series.columns) != list(weighted_sum.columns):
                raise RESGenerationError(f"Inconsistent series columns in zone='{zone}', tech='{tech}'")

        weighted_sum = weighted_sum + (numeric_series * float(tech_weight))
        weight_sum += float(tech_weight)

    if not has_positive_weight or weighted_sum is None or weight_sum <= 0:
        raise RESGenerationError(f"No usable technology series for zone='{zone}'")

    return weighted_sum / weight_sum


def _compute_zone_averages(
    *,
    techno_series_by_zone: Mapping[str, Mapping[str, pd.DataFrame]],
    techno_weights_by_zone: Mapping[str, Mapping[str, float]],
    zonal_weights: Mapping[str, float],
) -> dict[str, pd.DataFrame]:
    zone_averages: dict[str, pd.DataFrame] = {}
    for zone, zone_weight in zonal_weights.items():
        if zone_weight < 0:
            raise RESGenerationError(f"Negative zonal weight for zone='{zone}': {zone_weight}")

        # Skip zones with zero weight
        if zone_weight == 0:
            continue

        tech_weights = techno_weights_by_zone.get(zone)

        # active zones (> 0) MUST have technology rows
        if not tech_weights:
            raise RESGenerationError(f"Active zone '{zone}' is missing from tech_weights_by_zone")

        series_by_tech = techno_series_by_zone.get(zone, {})

        zone_averages[zone] = _compute_zone_average(
            zone=zone,
            tech_weights=tech_weights,
            series_by_tech=series_by_tech,
        )

    return zone_averages


def _compute_global_weighted_series(
    *,
    zone_averages: Mapping[str, pd.DataFrame],
    zonal_weights: Mapping[str, float],
) -> pd.DataFrame:
    global_series_sum: pd.DataFrame | None = None
    global_weight_sum = 0.0

    for zone, zone_weight in zonal_weights.items():
        # Skip zones with zero weight
        if zone_weight == 0:
            continue

        zone_series = zone_averages.get(zone)
        if zone_series is None:
            continue

        if global_series_sum is None:
            global_series_sum = pd.DataFrame(
                np.zeros((len(zone_series), len(zone_series.columns)), dtype=np.float64), columns=zone_series.columns
            )
        else:
            if len(zone_series) != len(global_series_sum):
                raise RESGenerationError(f"Inconsistent zone series length for zone='{zone}'")
            if list(zone_series.columns) != list(global_series_sum.columns):
                raise RESGenerationError(f"Inconsistent zone series columns for zone='{zone}'")

        global_series_sum = global_series_sum + (zone_series * float(zone_weight))
        global_weight_sum += float(zone_weight)

    if global_series_sum is None or global_weight_sum <= 0:
        raise RESGenerationError("No usable global weighted load factor for FR")

    return global_series_sum / global_weight_sum


def _parse_single_zone_tech_weights(
    *,
    zone: str,
    raw_tech_weights: Any,
    area_name: str,
    group_key: str,
) -> dict[str, float]:
    if not isinstance(raw_tech_weights, Mapping) or not raw_tech_weights:
        raise RESGenerationError(
            f"Invalid technology weights for zone='{zone}', area='{area_name}', group='{group_key}'"
        )

    parsed_zone: dict[str, float] = {}
    for raw_tech, raw_weight in raw_tech_weights.items():
        tech = str(raw_tech).strip()
        if not tech:
            raise RESGenerationError(f"Empty technology key for zone='{zone}', area='{area_name}', group='{group_key}'")
        weight = _to_float_capacity(raw_weight)
        if weight < 0:
            raise RESGenerationError(f"Negative technology weight for zone='{zone}', tech='{tech}'")
        parsed_zone[tech] = weight

    return parsed_zone


def _process_res_entry(
    *,
    area_name: str,
    normalized_area_name: str,
    group_key: str,
    group_values: Any,
    base_ts_directory: Path,
) -> tuple[dict[str, Any], pd.DataFrame | None]:
    if not isinstance(group_values, Mapping):
        raise RESGenerationError(f"Invalid RES group payload for area='{area_name}', group='{group_key}'")

    group_raw, installed_power = _extract_res_properties(
        area_name=area_name,
        group_key=group_key,
        group_values=group_values,
    )
    _validate_group_key_matches_group(area_name=area_name, group_key=group_key, group_name=group_raw)

    aw_group = map_res_group_to_aw(group_raw)
    capacity, enabled = resolve_res_capacity_and_enabled(installed_power=installed_power)

    payload = build_res_cluster_payload(
        area_name=area_name,
        cluster_name=group_key,
        aw_group=aw_group,
        capacity_mw=capacity,
        enabled=enabled,
    )
    if not enabled:
        return payload, None

    series_files = _validate_series_list(
        area_name=area_name,
        group_key=group_key,
        raw_series=group_values.get("series", []),
    )
    fr_aggregation = group_values.get("fr_aggregation")
    validated_series = _compute_cluster_series(
        normalized_area_name=normalized_area_name,
        area_name=area_name,
        group_key=group_key,
        series_files=series_files,
        fr_aggregation=fr_aggregation,
        base_ts_directory=base_ts_directory,
    )
    return payload, validated_series


def _compute_cluster_series(
    *,
    normalized_area_name: str,
    area_name: str,
    group_key: str,
    series_files: list[str],
    fr_aggregation: Any,
    base_ts_directory: Path,
) -> pd.DataFrame:
    if normalized_area_name == "FR":
        if series_files:
            raise RESGenerationError(
                f"FR RES computed mode expects empty series for area='{area_name}', group='{group_key}'"
            )
        return _build_fr_weighted_series_from_aggregation(
            area_name=area_name,
            group_key=group_key,
            raw_fr_aggregation=fr_aggregation,
            base_ts_directory=base_ts_directory,
        )

    if fr_aggregation is not None:
        raise RESGenerationError(
            f"fr_aggregation is only supported for FR area; area='{area_name}', group='{group_key}'"
        )
    if len(series_files) != 1:
        raise RESGenerationError(
            f"Expected exactly one RES series file for area='{area_name}', group='{group_key}', got {len(series_files)}"
        )
    return read_res_hourly_series(
        base_dir=base_ts_directory,
        filename=series_files[0],
        expected_rows=EXPECTED_HOURS,
        value_column_index=RES_VALUE_COLUMN_INDEX,
    )


def _register_res_outputs(
    *,
    area_obj: Area,
    group_key: str,
    payload: dict[str, Any],
    validated_series: pd.DataFrame | None,
) -> None:
    properties = RenewableClusterProperties(
        enabled=bool(payload["enabled"]),
        unit_count=int(payload["unit_count"]),
        nominal_capacity=float(payload["nominal_capacity"]),
        group=str(payload["group"]),
        ts_interpretation=_parse_ts_interpretation(str(payload["ts_interpretation"])),
    )
    renewable_cluster = area_obj.create_renewable_cluster(group_key, properties)
    if validated_series is not None:
        if isinstance(validated_series, pd.DataFrame):
            renewable_cluster.set_series(validated_series)
        else:
            renewable_cluster.set_series(pd.DataFrame({"value": validated_series}))


def _parse_ts_interpretation(value: str) -> TimeSeriesInterpretation:
    try:
        return TimeSeriesInterpretation(value)
    except ValueError as exc:
        raise RESGenerationError(f"Unsupported RES ts_interpretation '{value}'") from exc


def _to_float_capacity(raw_capacity: Any) -> float:
    try:
        return float(raw_capacity)
    except (TypeError, ValueError) as exc:
        raise RESGenerationError(f"Invalid installed power value: {raw_capacity}") from exc
