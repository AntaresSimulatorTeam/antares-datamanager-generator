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
import pytest

from typing import Any, cast

import pandas as pd

from antares.datamanager.exceptions.exceptions import RESGenerationError
from antares.datamanager.generator.generate_res_clusters import (
    _compute_zone_average,
    _resolve_res_base_directory,
    generate_res_clusters,
    map_res_group_to_aw,
    read_res_hourly_series,
    resolve_and_validate_res_arrow_path,
    resolve_res_capacity_and_enabled,
)


def test_map_res_group_to_aw_accepts_legacy_and_aw_values():
    assert map_res_group_to_aw("wind_onshore") == "Wind Onshore"
    assert map_res_group_to_aw("Solar PV") == "Solar PV"


def test_map_res_group_to_aw_rejects_unknown_group():
    with pytest.raises(RESGenerationError, match="Unsupported RES group"):
        map_res_group_to_aw("tidal")


def test_resolve_capacity_and_enabled_missing_capacity_disables_cluster():
    capacity, enabled = resolve_res_capacity_and_enabled(installed_power=None)

    assert capacity == 0.0
    assert enabled is False


def test_resolve_capacity_and_enabled_rejects_mapping_value():
    with pytest.raises(RESGenerationError, match="Invalid installed power value"):
        resolve_res_capacity_and_enabled(installed_power={"2031": 1000})


def test_resolve_capacity_and_enabled_rejects_negative_value():
    with pytest.raises(RESGenerationError, match="Negative installed power"):
        resolve_res_capacity_and_enabled(installed_power=-1)


def test_resolve_and_validate_res_arrow_path_rejects_unsafe_path(tmp_path):
    with pytest.raises(RESGenerationError, match="outside allowed directory"):
        resolve_and_validate_res_arrow_path(tmp_path, "../escape.arrow")


def test_resolve_and_validate_res_arrow_path_empty_filename(tmp_path):
    with pytest.raises(RESGenerationError, match="must be a non-empty string"):
        resolve_and_validate_res_arrow_path(tmp_path, "")


def test_resolve_and_validate_res_arrow_path_invalid_extension(tmp_path):
    with pytest.raises(RESGenerationError, match="Unexpected RES file extension"):
        resolve_and_validate_res_arrow_path(tmp_path, "test.txt")


def test_resolve_and_validate_res_arrow_path_not_found(tmp_path):
    with pytest.raises(FileNotFoundError, match="RES series file not found"):
        resolve_and_validate_res_arrow_path(tmp_path, "missing.arrow")


def test_read_res_hourly_series_validates_row_count(tmp_path):
    file_path = tmp_path / "ts.arrow"
    pd.DataFrame({"v": [0.1] * 10}).to_feather(file_path)

    with pytest.raises(RESGenerationError, match="must contain 8760 rows"):
        read_res_hourly_series(base_dir=tmp_path, filename="ts.arrow")


def test_read_res_hourly_series_empty_dataframe(tmp_path):
    file_path = tmp_path / "empty.arrow"
    pd.DataFrame().to_feather(file_path)
    with pytest.raises(RESGenerationError, match="has no time series columns"):
        read_res_hourly_series(base_dir=tmp_path, filename="empty.arrow")


def test_read_res_hourly_series_non_numeric(tmp_path):
    file_path = tmp_path / "str.arrow"
    pd.DataFrame({"v": ["a"] * 8760}).to_feather(file_path)
    # Legacy behaviour raised a different message; current implementation
    # rejects files that contain no numeric TS columns.
    with pytest.raises(RESGenerationError, match="contains no numeric time series"):
        read_res_hourly_series(base_dir=tmp_path, filename="str.arrow")


def test_read_res_hourly_series_out_of_bounds(tmp_path):
    file_path = tmp_path / "high.arrow"
    pd.DataFrame({"v": [1.5] * 8760}).to_feather(file_path)
    with pytest.raises(RESGenerationError, match="out of bounds"):
        read_res_hourly_series(base_dir=tmp_path, filename="high.arrow")


def test_read_res_hourly_series_uses_first_real_ts_column_when_date_column_present(tmp_path):
    file_path = tmp_path / "multi.arrow"
    pd.DataFrame({"date": [0.0] * 8760, "TS1": [0.42] * 8760, "TS2": [0.99] * 8760}).to_feather(file_path)

    ts_df = read_res_hourly_series(base_dir=tmp_path, filename="multi.arrow")

    assert len(ts_df) == 8760
    # Ensure the first real TS column ('TS1') was preserved and contains expected values
    assert float(ts_df["TS1"].iloc[0]) == pytest.approx(0.42)
    assert float(ts_df["TS1"].mean()) == pytest.approx(0.42)


class _RenewableClusterFake:
    name: str
    properties: Any
    series: pd.DataFrame | None
    _owner: "_AreaForStarter"

    def __init__(self, name: str, props: Any, owner: "_AreaForStarter"):
        self.name = name
        self.properties = props
        self.series: pd.DataFrame | None = None
        self._owner = owner

    def set_series(self, matrix: pd.DataFrame) -> None:
        self.series = matrix
        self._owner.timeseries[self.name] = matrix


class _AreaForStarter:
    payloads: list[dict[str, Any]]
    timeseries: dict[str, Any]
    created: list[Any]

    def __init__(self):
        self.payloads: list[dict[str, Any]] = []
        self.timeseries: dict[str, Any] = {}
        self.created: list[Any] = []

    def create_renewable_cluster(self, renewable_name: str, properties: Any = None) -> Any:
        cluster: Any = _RenewableClusterFake(renewable_name, properties, self)
        self.created.append(cluster)
        self.payloads.append(
            {
                "cluster": renewable_name,
                "group": properties.group if properties is not None else None,
                "enabled": properties.enabled if properties is not None else None,
                "nominal_capacity": properties.nominal_capacity if properties is not None else None,
            }
        )
        return cluster


def _make_area() -> _AreaForStarter:
    return _AreaForStarter()


def _set_res_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    monkeypatch.setenv("PEGASE_RES_TS_OUTPUT_DIRECTORY", str(tmp_path))


def test_generate_res_clusters_invalid_payload_type(tmp_path, monkeypatch):
    _set_res_directory(monkeypatch, tmp_path)
    with pytest.raises(RESGenerationError, match="expected object"):
        generate_res_clusters(_make_area(), "FR", cast(Any, cast(Any, cast(Any, ["list", "not", "dict"]))))


def test_generate_res_clusters_invalid_group_values(tmp_path, monkeypatch):
    _set_res_directory(monkeypatch, tmp_path)
    with pytest.raises(RESGenerationError, match="Invalid RES group payload"):
        generate_res_clusters(_make_area(), "FR", {"wind_onshore": "string_not_dict"})


def test_generate_res_clusters_missing_properties_keys(tmp_path, monkeypatch):
    _set_res_directory(monkeypatch, tmp_path)
    with pytest.raises(RESGenerationError, match="Missing RES properties.capacity"):
        generate_res_clusters(_make_area(), "FR", {"wind_onshore": {"properties": {"group": "wind_onshore"}}})


def test_generate_res_clusters_invalid_series_list(tmp_path, monkeypatch):
    _set_res_directory(monkeypatch, tmp_path)
    with pytest.raises(RESGenerationError, match="expected string"):
        generate_res_clusters(
            _make_area(),
            "AT",
            {"wind_onshore": {"properties": {"group": "wind_onshore", "capacity": 10}, "series": "not_a_list"}},
        )


def test_generate_res_clusters_starter_registers_payload(tmp_path, monkeypatch):
    file_path = tmp_path / "series.arrow"
    pd.DataFrame({"v": [0.4] * 8760}).to_feather(file_path)

    _set_res_directory(monkeypatch, tmp_path)

    area = _make_area()
    res = {
        "wind_onshore": {
            "properties": {"group": "wind_onshore", "capacity": 1000},
            "series": ["series.arrow"],
        }
    }

    generate_res_clusters(area, "AT", res)

    assert len(area.payloads) == 1
    assert area.payloads[0]["cluster"] == "wind_onshore"
    assert area.payloads[0]["enabled"] is True
    assert "wind_onshore" in area.timeseries
    assert len(area.timeseries["wind_onshore"]) == 8760


def test_generate_res_clusters_uses_craft_renewable_api_when_available(tmp_path, monkeypatch):
    file_path = tmp_path / "series.arrow"
    pd.DataFrame({"v": [0.4] * 8760}).to_feather(file_path)

    _set_res_directory(monkeypatch, tmp_path)

    area = _make_area()
    res = {
        "wind_onshore": {
            "properties": {"group": "wind_onshore", "capacity": 1000},
            "series": ["series.arrow"],
        }
    }

    generate_res_clusters(area, "AT", res)

    assert len(area.created) == 1
    cluster = area.created[0]
    assert cluster.name == "wind_onshore"
    assert cluster.properties.enabled is True
    assert cluster.properties.unit_count == 1
    assert cluster.properties.nominal_capacity == 1000.0
    assert cluster.properties.group == "Wind Onshore"
    assert cluster.series is not None
    assert isinstance(cluster.series, pd.DataFrame)
    assert len(cluster.series.index) == 8760


def test_generate_res_clusters_handles_res_from_mixed_area_payload(tmp_path, monkeypatch):
    area_data: dict[str, Any] = {
        "hydro": {"properties": "HydroProperties as JSON"},
        "thermals": {"at_ccgt": {"properties": "ThermalProperties as JSON"}},
        "misc": {"biomass": {"properties": {"capacity": 640.0, "group": "biomass"}}},
        "loads": ["load_AT_2031.arrow"],
        "res": {
            "wind_onshore": {
                "properties": {"group": "wind_onshore", "capacity": 850},
                "series": ["wind_onshore/at_onshore_demo/2031.arrow"],
            }
        },
    }

    assert area_data["hydro"]
    assert area_data["thermals"]
    assert area_data["misc"]
    assert area_data["loads"]

    series_list = area_data["res"]["wind_onshore"]["series"]
    assert isinstance(series_list, list)
    series_rel_path = series_list[0]
    series_path = tmp_path / series_rel_path
    series_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"v": [0.25] * 8760}).to_feather(series_path)

    _set_res_directory(monkeypatch, tmp_path)

    area = _make_area()

    generate_res_clusters(area, "AT", area_data["res"])

    assert len(area.created) == 1
    assert area.created[0].properties.group == "Wind Onshore"
    assert area.created[0].properties.nominal_capacity == 850.0
    assert area.created[0].series is not None


def test_generate_res_clusters_mixed_payload_still_rejects_invalid_res_group(tmp_path, monkeypatch):
    area_data: dict[str, Any] = {
        "hydro": {"properties": "HydroProperties as JSON"},
        "thermals": {"it_ccgt": {"properties": "ThermalProperties as JSON"}},
        "misc": {"other": {"properties": {"capacity": 42.0, "group": "other"}}},
        "loads": ["load_IT_2031.arrow"],
        "res": {
            "tidal": {
                "properties": {"group": "tidal", "capacity": 180},
                "series": ["tidal/it_tidal_demo/2031.arrow"],
            }
        },
    }

    area = _make_area()
    _set_res_directory(monkeypatch, tmp_path)

    with pytest.raises(RESGenerationError, match="Unsupported RES group"):
        generate_res_clusters(area, "IT", area_data["res"])


def test_generate_res_clusters_rejects_multiple_series_for_enabled_cluster(tmp_path, monkeypatch):
    (tmp_path / "a.arrow").parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"v": [0.2] * 8760}).to_feather(tmp_path / "a.arrow")
    pd.DataFrame({"v": [0.3] * 8760}).to_feather(tmp_path / "b.arrow")

    area = _make_area()
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_onshore": {
            "properties": {"group": "wind_onshore", "capacity": 500},
            "series": ["a.arrow", "b.arrow"],
        }
    }

    with pytest.raises(RESGenerationError, match="Expected exactly one RES series file"):
        generate_res_clusters(area, "BE", res)


def test_generate_res_clusters_computes_fr_weighted_series_from_aggregation(tmp_path, monkeypatch):
    pd.DataFrame({"v": [0.5] * 8760}).to_feather(tmp_path / "fr01_t1.arrow")
    pd.DataFrame({"v": [0.0] * 8760}).to_feather(tmp_path / "fr01_t2.arrow")
    pd.DataFrame({"v": [1.0] * 8760}).to_feather(tmp_path / "fr02_t1.arrow")

    area = _make_area()
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_offshore": {
            "properties": {"group": "wind_offshore", "capacity": 1200},
            "series": [],
            "fr_aggregation": {
                "zone_weights": {"FR01": 0.25, "FR02": 0.75},
                "tech_weights_by_zone": {
                    "FR01": {"offshore_tech1": 0.6, "offshore_tech2": 0.4},
                    "FR02": {"offshore_tech1": 1.0},
                },
                "series_by_zone_and_tech": {
                    "FR01": {"offshore_tech1": "fr01_t1.arrow", "offshore_tech2": "fr01_t2.arrow"},
                    "FR02": {"offshore_tech1": "fr02_t1.arrow"},
                },
            },
        }
    }

    generate_res_clusters(area, "FR", res)

    assert len(area.payloads) == 1
    assert "wind_offshore" in area.timeseries
    assert float(area.timeseries["wind_offshore"].iloc[0, 0]) == pytest.approx(0.825)


def test_fr_aggregation_negative_zone_weight(tmp_path, monkeypatch):
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_offshore": {
            "properties": {"group": "wind_offshore", "capacity": 10},
            "series": [],
            "fr_aggregation": {
                "zone_weights": {"FR01": -1.0},
                "tech_weights_by_zone": {},
                "series_by_zone_and_tech": {},
            },
        }
    }
    with pytest.raises(RESGenerationError, match="Negative zone weight"):
        generate_res_clusters(_make_area(), "FR", res)


def test_fr_aggregation_sum_zone_weight_zero(tmp_path, monkeypatch):
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_offshore": {
            "properties": {"group": "wind_offshore", "capacity": 10},
            "series": [],
            "fr_aggregation": {
                "zone_weights": {"FR01": 0.0},
                "tech_weights_by_zone": {},
                "series_by_zone_and_tech": {},
            },
        }
    }
    with pytest.raises(RESGenerationError, match="Sum of zone_weights must be > 0"):
        generate_res_clusters(_make_area(), "FR", res)


def test_fr_aggregation_unknown_zone_in_tech_weights(tmp_path, monkeypatch):
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_offshore": {
            "properties": {"group": "wind_offshore", "capacity": 10},
            "series": [],
            "fr_aggregation": {
                "zone_weights": {"FR01": 1.0},
                "tech_weights_by_zone": {"FR02": {}},
                "series_by_zone_and_tech": {},
            },
        }
    }
    with pytest.raises(RESGenerationError, match="not found in zone_weights"):
        generate_res_clusters(_make_area(), "FR", res)


def test_fr_aggregation_negative_tech_weight(tmp_path, monkeypatch):
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_offshore": {
            "properties": {"group": "wind_offshore", "capacity": 10},
            "series": [],
            "fr_aggregation": {
                "zone_weights": {"FR01": 1.0},
                "tech_weights_by_zone": {"FR01": {"t1": -1.0}},
                "series_by_zone_and_tech": {},
            },
        }
    }
    with pytest.raises(RESGenerationError, match="Negative technology weight"):
        generate_res_clusters(_make_area(), "FR", res)


def test_compute_zone_average_inconsistent_length():
    s1 = pd.Series([0.5] * 10)
    s2 = pd.Series([0.5] * 5)
    with pytest.raises(RESGenerationError, match="Inconsistent series length"):
        _compute_zone_average(zone="FR01", tech_weights={"t1": 1.0, "t2": 1.0}, series_by_tech={"t1": s1, "t2": s2})


def test_compute_zone_average_no_usable_series():
    s1 = pd.Series([0.5] * 10)
    with pytest.raises(RESGenerationError, match="No usable technology series"):
        _compute_zone_average(zone="FR01", tech_weights={"t1": 0.0}, series_by_tech={"t1": s1})


def test_resolve_res_base_directory_invalid_type(monkeypatch):
    class MockSettings:
        res_ts_directory = "not_a_path"

    monkeypatch.setattr("antares.datamanager.generator.generate_res_clusters.settings", MockSettings())
    with pytest.raises(RESGenerationError, match="must be a Path"):
        _resolve_res_base_directory()


def test_generate_res_clusters_missing_capacity_registers_disabled_without_timeseries(tmp_path, monkeypatch):
    area = _make_area()
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_onshore": {
            "properties": {"group": "wind_onshore", "capacity": None},
            "series": [],
        }
    }

    generate_res_clusters(area, "AT", res)

    assert len(area.payloads) == 1
    assert area.payloads[0]["enabled"] is False
    assert "wind_onshore" not in area.timeseries


def test_resolve_capacity_and_enabled_tiny_positive_value_is_enabled():
    capacity, enabled = resolve_res_capacity_and_enabled(installed_power=1e-13)

    assert capacity == pytest.approx(1e-13)
    assert enabled is True


def test_generate_res_clusters_rejects_cluster_key_group_mismatch(tmp_path, monkeypatch):
    area = _make_area()
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_onshore": {
            "properties": {"group": "wind_offshore", "capacity": 100.0},
            "series": ["dummy.arrow"],
        }
    }

    with pytest.raises(RESGenerationError, match="key must match properties.group"):
        generate_res_clusters(area, "AT", res)


def test_generate_res_clusters_rejects_fr_with_non_empty_series(tmp_path, monkeypatch):
    area = _make_area()
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_offshore": {
            "properties": {"group": "wind_offshore", "capacity": 1200},
            "series": ["fr.arrow"],
            "fr_aggregation": {},
        }
    }

    with pytest.raises(RESGenerationError, match="expects empty series"):
        generate_res_clusters(area, "FR", res)


def test_generate_res_clusters_rejects_non_fr_with_fr_aggregation(tmp_path, monkeypatch):
    pd.DataFrame({"v": [0.2] * 8760}).to_feather(tmp_path / "at.arrow")

    area = _make_area()
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_onshore": {
            "properties": {"group": "wind_onshore", "capacity": 850},
            "series": ["at.arrow"],
            "fr_aggregation": {"zone_weights": {"FR01": 1.0}},
        }
    }

    with pytest.raises(RESGenerationError, match="only supported for FR"):
        generate_res_clusters(area, "AT", res)


def test_generate_res_clusters_rejects_fr_zone_key_outside_fr01_fr26(tmp_path, monkeypatch):
    pd.DataFrame({"v": [0.5] * 8760}).to_feather(tmp_path / "zone.arrow")

    area = _make_area()
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_offshore": {
            "properties": {"group": "wind_offshore", "capacity": 1000},
            "series": [],
            "fr_aggregation": {
                "zone_weights": {"FR27": 1.0},
                "tech_weights_by_zone": {"FR27": {"offshore_tech1": 1.0}},
                "series_by_zone_and_tech": {"FR27": {"offshore_tech1": "zone.arrow"}},
            },
        }
    }

    with pytest.raises(RESGenerationError, match="Invalid FR zone key"):
        generate_res_clusters(area, "FR", res)


def test_generate_res_clusters_rejects_fr_technology_key_mismatch(tmp_path, monkeypatch):
    pd.DataFrame({"v": [0.5] * 8760}).to_feather(tmp_path / "fr01.arrow")

    area = _make_area()
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_offshore": {
            "properties": {"group": "wind_offshore", "capacity": 1200},
            "series": [],
            "fr_aggregation": {
                "zone_weights": {"FR01": 1.0},
                "tech_weights_by_zone": {"FR01": {"offshore_tech1": 1.0}},
                "series_by_zone_and_tech": {"FR01": {"offshore_tech2": "fr01.arrow"}},
            },
        }
    }

    with pytest.raises(RESGenerationError, match="Unexpected technology keys"):
        generate_res_clusters(area, "FR", res)


def test_generate_res_clusters_rejects_active_zone_missing_tech_weights(tmp_path, monkeypatch):
    area = _make_area()
    _set_res_directory(monkeypatch, tmp_path)
    pd.DataFrame({"v": [0.5] * 8760}).to_feather(tmp_path / "fr01.arrow")

    res = {
        "wind_offshore": {
            "properties": {"group": "wind_offshore", "capacity": 1200},
            "series": [],
            "fr_aggregation": {
                "zone_weights": {"FR01": 0.5, "FR02": 0.5},
                "tech_weights_by_zone": {
                    "FR01": {"offshore_tech1": 1.0}
                    # FR02 intentionally omitted to trigger validation
                },
                "series_by_zone_and_tech": {"FR01": {"offshore_tech1": "fr01.arrow"}},
            },
        }
    }

    with pytest.raises(RESGenerationError, match="Active zone 'FR02' is missing from tech_weights_by_zone"):
        generate_res_clusters(area, "FR", res)


def test_generate_res_clusters_accepts_fr_zone_without_leading_zero(tmp_path, monkeypatch):
    pd.DataFrame({"v": [0.5] * 8760}).to_feather(tmp_path / "fr1.arrow")

    area = _make_area()
    _set_res_directory(monkeypatch, tmp_path)
    res = {
        "wind_offshore": {
            "properties": {"group": "wind_offshore", "capacity": 1000},
            "series": [],
            "fr_aggregation": {
                "zone_weights": {"FR1": 1.0},
                "tech_weights_by_zone": {"FR1": {"offshore_tech1": 1.0}},
                "series_by_zone_and_tech": {"FR1": {"offshore_tech1": "fr1.arrow"}},
            },
        }
    }

    generate_res_clusters(area, "FR", res)

    assert len(area.payloads) == 1
    assert "wind_offshore" in area.timeseries
