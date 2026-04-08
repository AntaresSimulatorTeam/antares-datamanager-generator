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
) -> pd.Series[Any]:
    file_path = resolve_and_validate_res_arrow_path(base_dir, filename)
    df = pd.read_feather(file_path)

    if df.empty or df.shape[1] <= value_column_index:
        raise RESGenerationError(
            f"RES .arrow file has no value column index={value_column_index} for file='{filename}'"
        )

    if len(df.index) != expected_rows:
        raise RESGenerationError(
            f"RES .arrow file must contain {expected_rows} rows for file='{filename}', got {len(df.index)}"
        )

    series = pd.to_numeric(df.iloc[:, value_column_index], errors="coerce")
    if series.isna().any():
        raise RESGenerationError(f"RES .arrow file contains non-numeric values for file='{filename}'")

    out_of_bounds = (series < 0.0) | (series > 1.0)
    if bool(out_of_bounds.any()):
        raise RESGenerationError(
            f"RES .arrow values out of bounds [0,1] for file='{filename}' "
            f"(min={float(series.min())}, max={float(series.max())})"
        )

    return series


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
    techno_series_by_zone: Mapping[str, Mapping[str, pd.Series[Any]]],
    techno_weights_by_zone: Mapping[str, Mapping[str, float]],
    zonal_weights: Mapping[str, float],
) -> pd.Series[Any]:
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
    return {
        "area": area_name,
        "cluster": cluster_name,
        "group": aw_group,
        "enabled": bool(enabled),
        "ts_interpretation": ts_interpretation,
        "unit_count": unit_count,
        "nominal_capacity_mw": float(capacity_mw),
    }


def generate_res_clusters(area_obj: Any, area_name: str, res: dict[str, Any]) -> None:
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

    for cluster_name, cluster_values in res.items():
        payload, validated_series = _process_res_entry(
            area_name=area_name,
            normalized_area_name=normalized_area_name,
            cluster_name=cluster_name,
            cluster_values=cluster_values,
            base_ts_directory=base_ts_directory,
        )

        logger.info("Prepared RES cluster payload area=%s cluster=%s payload=%s", area_name, cluster_name, payload)
        _register_res_outputs(
            area_obj=area_obj, cluster_name=cluster_name, payload=payload, validated_series=validated_series
        )


def _resolve_res_base_directory() -> Path:
    base_ts_directory = settings.res_ts_directory
    if not isinstance(base_ts_directory, Path):
        raise RESGenerationError("res_ts_directory must be a Path")
    return base_ts_directory


def _validate_series_list(*, area_name: str, cluster_name: str, raw_series: Any) -> list[str]:
    if not isinstance(raw_series, list) or any(not isinstance(item, str) for item in raw_series):
        raise RESGenerationError(
            f"Invalid RES series list for area='{area_name}', cluster='{cluster_name}': expected string[]"
        )

    return raw_series


def _extract_res_properties(*, area_name: str, cluster_name: str, cluster_values: Mapping[str, Any]) -> tuple[str, Any]:
    properties = cluster_values.get("properties")
    if not isinstance(properties, Mapping):
        raise RESGenerationError(
            f"Invalid RES properties for area='{area_name}', cluster='{cluster_name}': expected object"
        )

    if "group" not in properties:
        raise RESGenerationError(f"Missing RES properties.group for area='{area_name}', cluster='{cluster_name}'")
    if "capacity" not in properties:
        raise RESGenerationError(f"Missing RES properties.capacity for area='{area_name}', cluster='{cluster_name}'")

    return str(properties.get("group", "")), properties.get("capacity")


def _validate_cluster_key_matches_group(*, area_name: str, cluster_name: str, group_name: str) -> None:
    if str(cluster_name).strip().lower() != str(group_name).strip().lower():
        raise RESGenerationError(
            f"RES entry key must match properties.group; area='{area_name}', key='{cluster_name}', group='{group_name}'"
        )


def _build_fr_weighted_series_from_aggregation(
    *,
    area_name: str,
    cluster_name: str,
    raw_fr_aggregation: Any,
    base_ts_directory: Path,
) -> pd.Series[Any]:
    if not isinstance(raw_fr_aggregation, Mapping):
        raise RESGenerationError(
            f"Missing or invalid fr_aggregation for FR area='{area_name}', cluster='{cluster_name}'"
        )

    required_keys = {"zone_weights", "tech_weights_by_zone", "series_by_zone_and_tech"}
    missing_keys = sorted(key for key in required_keys if key not in raw_fr_aggregation)
    if missing_keys:
        raise RESGenerationError(
            f"Missing FR aggregation keys {missing_keys} for area='{area_name}', cluster='{cluster_name}'"
        )

    zone_weights = _parse_zone_weights(
        area_name=area_name,
        cluster_name=cluster_name,
        raw_zone_weights=raw_fr_aggregation.get("zone_weights"),
    )
    tech_weights_by_zone = _parse_tech_weights_by_zone(
        area_name=area_name,
        cluster_name=cluster_name,
        raw_tech_weights_by_zone=raw_fr_aggregation.get("tech_weights_by_zone"),
        expected_zones=set(zone_weights.keys()),
    )
    techno_series_by_zone = _load_tech_series_by_zone(
        area_name=area_name,
        cluster_name=cluster_name,
        raw_series_by_zone_and_tech=raw_fr_aggregation.get("series_by_zone_and_tech"),
        expected_zones=set(zone_weights.keys()),
        expected_techs_by_zone={zone: set(techs.keys()) for zone, techs in tech_weights_by_zone.items()},
        base_ts_directory=base_ts_directory,
    )

    return compute_fr_weighted_load_factor(
        techno_series_by_zone=techno_series_by_zone,
        techno_weights_by_zone=tech_weights_by_zone,
        zonal_weights=zone_weights,
    )


def _parse_zone_weights(*, area_name: str, cluster_name: str, raw_zone_weights: Any) -> dict[str, float]:
    if not isinstance(raw_zone_weights, Mapping) or not raw_zone_weights:
        raise RESGenerationError(f"Invalid zone_weights for area='{area_name}', cluster='{cluster_name}'")

    zone_weights: dict[str, float] = {}
    for raw_zone, raw_weight in raw_zone_weights.items():
        zone = str(raw_zone).strip().upper()
        if not _is_valid_fr_zone(zone):
            raise RESGenerationError(
                f"Invalid FR zone key '{raw_zone}' for area='{area_name}', cluster='{cluster_name}'"
            )
        weight = _to_float_capacity(raw_weight)
        if weight < 0:
            raise RESGenerationError(
                f"Negative zone weight for zone='{zone}', area='{area_name}', cluster='{cluster_name}'"
            )
        zone_weights[zone] = weight

    if sum(zone_weights.values()) <= 0:
        raise RESGenerationError(f"Sum of zone_weights must be > 0 for area='{area_name}', cluster='{cluster_name}'")

    return zone_weights


def _parse_tech_weights_by_zone(
    *,
    area_name: str,
    cluster_name: str,
    raw_tech_weights_by_zone: Any,
    expected_zones: set[str],
) -> dict[str, dict[str, float]]:
    if not isinstance(raw_tech_weights_by_zone, Mapping) or not raw_tech_weights_by_zone:
        raise RESGenerationError(f"Invalid tech_weights_by_zone for area='{area_name}', cluster='{cluster_name}'")

    _validate_expected_zones(
        source_name="tech_weights_by_zone",
        raw_zone_map=raw_tech_weights_by_zone,
        expected_zones=expected_zones,
        area_name=area_name,
        cluster_name=cluster_name,
    )

    parsed: dict[str, dict[str, float]] = {}
    for raw_zone, raw_tech_weights in raw_tech_weights_by_zone.items():
        zone = str(raw_zone).strip().upper()
        parsed[zone] = _parse_single_zone_tech_weights(
            zone=zone,
            raw_tech_weights=raw_tech_weights,
            area_name=area_name,
            cluster_name=cluster_name,
        )

    return parsed


def _load_tech_series_by_zone(
    *,
    area_name: str,
    cluster_name: str,
    raw_series_by_zone_and_tech: Any,
    expected_zones: set[str],
    expected_techs_by_zone: Mapping[str, set[str]],
    base_ts_directory: Path,
) -> dict[str, dict[str, pd.Series[Any]]]:
    if not isinstance(raw_series_by_zone_and_tech, Mapping) or not raw_series_by_zone_and_tech:
        raise RESGenerationError(f"Invalid series_by_zone_and_tech for area='{area_name}', cluster='{cluster_name}'")

    incoming_zones = {str(zone).strip().upper() for zone in raw_series_by_zone_and_tech.keys()}
    if incoming_zones != expected_zones:
        raise RESGenerationError(
            "Mismatched zones between zone_weights and series_by_zone_and_tech for "
            f"area='{area_name}', cluster='{cluster_name}'"
        )

    series_by_zone: dict[str, dict[str, pd.Series[Any]]] = {}
    for raw_zone, raw_series_by_tech in raw_series_by_zone_and_tech.items():
        zone = str(raw_zone).strip().upper()
        if not isinstance(raw_series_by_tech, Mapping) or not raw_series_by_tech:
            raise RESGenerationError(
                f"Invalid technology series map for zone='{zone}', area='{area_name}', cluster='{cluster_name}'"
            )

        tech_keys = {str(key).strip() for key in raw_series_by_tech.keys()}
        expected_techs = expected_techs_by_zone.get(zone, set())
        if tech_keys != expected_techs:
            raise RESGenerationError(
                "Technology keys mismatch between tech_weights_by_zone and series_by_zone_and_tech for "
                f"zone='{zone}', area='{area_name}', cluster='{cluster_name}'"
            )

        loaded_by_tech: dict[str, pd.Series[Any]] = {}
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
    if len(zone) != 4 or not zone.startswith("FR"):
        return False
    suffix = zone[2:]
    if not suffix.isdigit():
        return False
    zone_index = int(suffix)
    return 1 <= zone_index <= 26


def _coerce_numeric_series(*, series: pd.Series[Any], zone: str, tech: str) -> pd.Series[Any]:
    numeric_series = pd.to_numeric(series, errors="coerce")
    if numeric_series.isna().any():
        raise RESGenerationError(f"Non numeric values for zone='{zone}', tech='{tech}'")
    return numeric_series


def _compute_zone_average(
    *,
    zone: str,
    tech_weights: Mapping[str, float],
    series_by_tech: Mapping[str, pd.Series[Any]],
) -> pd.Series[Any]:
    if not tech_weights:
        raise RESGenerationError(f"No technology weights for zone='{zone}'")

    weighted_sum: pd.Series[Any] | None = None
    weight_sum = 0.0
    for tech, tech_weight in tech_weights.items():
        if tech_weight < 0:
            raise RESGenerationError(f"Negative technology weight for zone='{zone}', tech='{tech}'")

        series = series_by_tech.get(tech)
        if series is None:
            raise RESGenerationError(f"Missing technology series for zone='{zone}', tech='{tech}'")

        numeric_series = _coerce_numeric_series(series=series, zone=zone, tech=tech)
        if weighted_sum is None:
            weighted_sum = pd.Series(np.zeros(len(numeric_series), dtype=np.float64))
        elif len(numeric_series) != len(weighted_sum):
            raise RESGenerationError(f"Inconsistent series length in zone='{zone}', tech='{tech}'")

        weighted_sum = weighted_sum + (numeric_series * float(tech_weight))
        weight_sum += float(tech_weight)

    if weighted_sum is None or weight_sum <= 0:
        raise RESGenerationError(f"No usable technology series for zone='{zone}'")

    return weighted_sum / weight_sum


def _compute_zone_averages(
    *,
    techno_series_by_zone: Mapping[str, Mapping[str, pd.Series[Any]]],
    techno_weights_by_zone: Mapping[str, Mapping[str, float]],
    zonal_weights: Mapping[str, float],
) -> dict[str, pd.Series[Any]]:
    zone_averages: dict[str, pd.Series[Any]] = {}
    for zone, zone_weight in zonal_weights.items():
        if zone_weight < 0:
            raise RESGenerationError(f"Negative zonal weight for zone='{zone}': {zone_weight}")

        zone_averages[zone] = _compute_zone_average(
            zone=zone,
            tech_weights=techno_weights_by_zone.get(zone, {}),
            series_by_tech=techno_series_by_zone.get(zone, {}),
        )

    return zone_averages


def _compute_global_weighted_series(
    *,
    zone_averages: Mapping[str, pd.Series[Any]],
    zonal_weights: Mapping[str, float],
) -> pd.Series[Any]:
    global_series_sum: pd.Series[Any] | None = None
    global_weight_sum = 0.0

    for zone, zone_weight in zonal_weights.items():
        zone_series = zone_averages.get(zone)
        if zone_series is None:
            raise RESGenerationError(f"Missing zone average for zone='{zone}'")

        if global_series_sum is None:
            global_series_sum = pd.Series(np.zeros(len(zone_series), dtype=np.float64))
        elif len(zone_series) != len(global_series_sum):
            raise RESGenerationError(f"Inconsistent zone series length for zone='{zone}'")

        global_series_sum = global_series_sum + (zone_series * float(zone_weight))
        global_weight_sum += float(zone_weight)

    if global_series_sum is None or global_weight_sum <= 0:
        raise RESGenerationError("No usable global weighted load factor for FR")

    return global_series_sum / global_weight_sum


def _validate_expected_zones(
    *,
    source_name: str,
    raw_zone_map: Mapping[str, Any],
    expected_zones: set[str],
    area_name: str,
    cluster_name: str,
) -> None:
    incoming_zones = {str(zone).strip().upper() for zone in raw_zone_map.keys()}
    if incoming_zones != expected_zones:
        raise RESGenerationError(
            f"Mismatched zones between zone_weights and {source_name} for area='{area_name}', cluster='{cluster_name}'"
        )


def _parse_single_zone_tech_weights(
    *,
    zone: str,
    raw_tech_weights: Any,
    area_name: str,
    cluster_name: str,
) -> dict[str, float]:
    if not isinstance(raw_tech_weights, Mapping) or not raw_tech_weights:
        raise RESGenerationError(
            f"Invalid technology weights for zone='{zone}', area='{area_name}', cluster='{cluster_name}'"
        )

    parsed_zone: dict[str, float] = {}
    for raw_tech, raw_weight in raw_tech_weights.items():
        tech = str(raw_tech).strip()
        if not tech:
            raise RESGenerationError(
                f"Empty technology key for zone='{zone}', area='{area_name}', cluster='{cluster_name}'"
            )
        weight = _to_float_capacity(raw_weight)
        if weight < 0:
            raise RESGenerationError(f"Negative technology weight for zone='{zone}', tech='{tech}'")
        parsed_zone[tech] = weight

    if sum(parsed_zone.values()) <= 0:
        raise RESGenerationError(
            f"Sum of technology weights must be > 0 for zone='{zone}', area='{area_name}', cluster='{cluster_name}'"
        )

    return parsed_zone


def _process_res_entry(
    *,
    area_name: str,
    normalized_area_name: str,
    cluster_name: str,
    cluster_values: Any,
    base_ts_directory: Path,
) -> tuple[dict[str, Any], pd.Series[Any] | None]:
    if not isinstance(cluster_values, Mapping):
        raise RESGenerationError(f"Invalid RES cluster payload for area='{area_name}', cluster='{cluster_name}'")

    group_raw, installed_power = _extract_res_properties(
        area_name=area_name,
        cluster_name=cluster_name,
        cluster_values=cluster_values,
    )
    _validate_cluster_key_matches_group(area_name=area_name, cluster_name=cluster_name, group_name=group_raw)

    aw_group = map_res_group_to_aw(group_raw)
    capacity, enabled = resolve_res_capacity_and_enabled(installed_power=installed_power)
    payload = build_res_cluster_payload(
        area_name=area_name,
        cluster_name=cluster_name,
        aw_group=aw_group,
        capacity_mw=capacity,
        enabled=enabled,
    )
    if not enabled:
        return payload, None

    series_files = _validate_series_list(
        area_name=area_name,
        cluster_name=cluster_name,
        raw_series=cluster_values.get("series", []),
    )
    fr_aggregation = cluster_values.get("fr_aggregation")
    validated_series = _compute_cluster_series(
        normalized_area_name=normalized_area_name,
        area_name=area_name,
        cluster_name=cluster_name,
        series_files=series_files,
        fr_aggregation=fr_aggregation,
        base_ts_directory=base_ts_directory,
    )
    return payload, validated_series


def _compute_cluster_series(
    *,
    normalized_area_name: str,
    area_name: str,
    cluster_name: str,
    series_files: list[str],
    fr_aggregation: Any,
    base_ts_directory: Path,
) -> pd.Series[Any]:
    if normalized_area_name == "FR":
        if series_files:
            raise RESGenerationError(
                f"FR RES computed mode expects empty series for area='{area_name}', cluster='{cluster_name}'"
            )
        return _build_fr_weighted_series_from_aggregation(
            area_name=area_name,
            cluster_name=cluster_name,
            raw_fr_aggregation=fr_aggregation,
            base_ts_directory=base_ts_directory,
        )

    if fr_aggregation is not None:
        raise RESGenerationError(
            f"fr_aggregation is only supported for FR area; area='{area_name}', cluster='{cluster_name}'"
        )
    if len(series_files) != 1:
        raise RESGenerationError(
            f"Expected exactly one RES series file for area='{area_name}', cluster='{cluster_name}', got {len(series_files)}"
        )
    return read_res_hourly_series(
        base_dir=base_ts_directory,
        filename=series_files[0],
        expected_rows=EXPECTED_HOURS,
        value_column_index=RES_VALUE_COLUMN_INDEX,
    )


def _register_res_outputs(
    *,
    area_obj: Any,
    cluster_name: str,
    payload: dict[str, Any],
    validated_series: pd.Series[Any] | None,
) -> None:
    if hasattr(area_obj, "register_res_payload"):
        area_obj.register_res_payload(payload)
    if validated_series is not None and hasattr(area_obj, "register_res_timeseries"):
        area_obj.register_res_timeseries(cluster_name, validated_series)


def _to_float_capacity(raw_capacity: Any) -> float:
    try:
        return float(raw_capacity)
    except (TypeError, ValueError) as exc:
        raise RESGenerationError(f"Invalid installed power value: {raw_capacity}") from exc
