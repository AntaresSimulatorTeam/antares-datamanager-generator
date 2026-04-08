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

from typing import Any

import pandas as pd

from antares.datamanager.exceptions.exceptions import RESGenerationError
from antares.datamanager.generator.generate_res_clusters import (
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


def test_read_res_hourly_series_validates_row_count(tmp_path):
    file_path = tmp_path / "ts.arrow"
    pd.DataFrame({"v": [0.1] * 10}).to_feather(file_path)

    with pytest.raises(RESGenerationError, match="must contain 8760 rows"):
        read_res_hourly_series(base_dir=tmp_path, filename="ts.arrow")


class _AreaForStarter:
    def __init__(self):
        self.payloads = []
        self.timeseries = {}

    def register_res_payload(self, payload):
        self.payloads.append(payload)

    def register_res_timeseries(self, cluster_name, series):
        self.timeseries[cluster_name] = series


def _set_res_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    monkeypatch.setenv("PEGASE_RES_TS_OUTPUT_DIRECTORY", str(tmp_path))


def test_generate_res_clusters_starter_registers_payload(tmp_path, monkeypatch):
    file_path = tmp_path / "series.arrow"
    pd.DataFrame({"v": [0.4] * 8760}).to_feather(file_path)

    _set_res_directory(monkeypatch, tmp_path)

    area = _AreaForStarter()
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

    area = _AreaForStarter()

    generate_res_clusters(area, "AT", area_data["res"])

    assert len(area.payloads) == 1
    assert area.payloads[0]["group"] == "Wind Onshore"
    assert area.payloads[0]["nominal_capacity_mw"] == 850.0
    assert "wind_onshore" in area.timeseries


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

    area = _AreaForStarter()
    _set_res_directory(monkeypatch, tmp_path)

    with pytest.raises(RESGenerationError, match="Unsupported RES group"):
        generate_res_clusters(area, "IT", area_data["res"])


def test_generate_res_clusters_rejects_multiple_series_for_enabled_cluster(tmp_path, monkeypatch):
    (tmp_path / "a.arrow").parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"v": [0.2] * 8760}).to_feather(tmp_path / "a.arrow")
    pd.DataFrame({"v": [0.3] * 8760}).to_feather(tmp_path / "b.arrow")

    area = _AreaForStarter()
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

    area = _AreaForStarter()
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
    assert float(area.timeseries["wind_offshore"].iloc[0]) == pytest.approx(0.825)


def test_generate_res_clusters_missing_capacity_registers_disabled_without_timeseries(tmp_path, monkeypatch):
    area = _AreaForStarter()
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
    area = _AreaForStarter()
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
    area = _AreaForStarter()
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

    area = _AreaForStarter()
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

    area = _AreaForStarter()
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

    area = _AreaForStarter()
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

    with pytest.raises(RESGenerationError, match="Technology keys mismatch"):
        generate_res_clusters(area, "FR", res)
