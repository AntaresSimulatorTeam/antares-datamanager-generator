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
import pandas as pd
from unittest.mock import MagicMock
from pathlib import Path
from antares.datamanager.generator.generate_hydro import generate_hydro

class MockHydro:
    def __init__(self):
        self.properties_updated = None
        self.allocations = None
        self.series = {}

    def update_properties(self, props):
        self.properties_updated = props

    def set_allocation(self, allocations):
        self.allocations = allocations

    def set_mod_series(self, df):
        self.series["mod"] = df

    def set_ror_series(self, df):
        self.series["ror"] = df

    def set_mingen(self, df):
        self.series["mingen"] = df

    def set_reservoir(self, df):
        self.series["reservoir"] = df

    def set_maxpower(self, df):
        self.series["maxpower"] = df

class MockArea:
    def __init__(self, name="at"):
        self.name = name
        self.hydro = MockHydro()

def test_generate_hydro_updates_properties_and_series(tmp_path, monkeypatch):
    # Setup
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_TS_OUTPUT_DIRECTORY", str(tmp_path))
    
    # Create dummy arrow files
    series_names = [
        "AT_mod.arrow",
        "AT_ror.arrow",
        "AT_mingen.arrow",
        "AT_reservoir_levels.arrow",
        "AT_maxpower.arrow"
    ]
    for name in series_names:
        df = pd.DataFrame({"v": [1.0] * 8760})
        df.to_feather(tmp_path / name)

    area_obj = MockArea(name="at")
    hydro_data = {
        "properties": {
            "follow_load": True,
            "inter_daily_breakdown": 1,
            "inter_daily_modulation": 1,
            "inter_monthly_breakdown": 1,
            "reservoir": False,
            "reservoir_capacity": 5000,
            "pumping_efficiency": 80,
            "initialize_reservoir_date": 0,
            "use_water": True,
            "allocation": {
                "area1": 0.5,
                "area2": 0.5
            }
        },
        "series": series_names
    }

    # Execute
    generate_hydro(area_obj, hydro_data)

    # Verify properties
    props = area_obj.hydro.properties_updated
    assert props.follow_load is True
    assert props.reservoir_capacity == 5000
    assert props.pumping_efficiency == 80
    assert props.intra_daily_modulation == 1
    
    # Verify allocations
    assert len(area_obj.hydro.allocations) == 2
    assert area_obj.hydro.allocations[0].area_id == "area1"
    assert area_obj.hydro.allocations[0].coefficient == 0.5

    # Verify series
    assert "mod" in area_obj.hydro.series
    assert "ror" in area_obj.hydro.series
    assert "mingen" in area_obj.hydro.series
    assert "reservoir" in area_obj.hydro.series
    assert "maxpower" in area_obj.hydro.series
    assert len(area_obj.hydro.series["mod"]) == 8760

def test_generate_hydro_with_duplicate_allocations(tmp_path, monkeypatch):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_TS_OUTPUT_DIRECTORY", str(tmp_path))

    area_obj = MockArea(name="at")
    # Simulation with different casing for same area
    hydro_data = {
        "properties": {
            "allocation": {
                "AREA1": 0.5,
                "area1": 0.3
            }
        },
        "series": []
    }
    
    generate_hydro(area_obj, hydro_data)
    
    allocations = area_obj.hydro.allocations
    assert len(allocations) == 1
    assert allocations[0].area_id == "area1"
    assert allocations[0].coefficient == 0.3

def test_generate_hydro_excludes_self_allocation(tmp_path, monkeypatch):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_TS_OUTPUT_DIRECTORY", str(tmp_path))

    area_obj = MockArea(name="FR")
    hydro_data = {
        "properties": {
            "allocation": {
                "FR": 1.0,
                "AT": 2.0
            }
        },
        "series": []
    }
    
    generate_hydro(area_obj, hydro_data)
    
    allocations = area_obj.hydro.allocations
    assert len(allocations) == 1
    assert allocations[0].area_id == "at"
    assert allocations[0].coefficient == 2.0

def test_generate_hydro_handles_properties_as_list(tmp_path, monkeypatch):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_TS_OUTPUT_DIRECTORY", str(tmp_path))

    area_obj = MockArea()
    hydro_data = {
        "properties": [
            {"follow_load": True, "reservoir_capacity": 3000}
        ],
        "series": []
    }

    # This should no longer fail
    generate_hydro(area_obj, hydro_data)
    
    props = area_obj.hydro.properties_updated
    assert props.follow_load is True
    assert props.reservoir_capacity == 3000

def test_generate_hydro_with_new_json_structure(tmp_path, monkeypatch):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_TS_OUTPUT_DIRECTORY", str(tmp_path))

    # Create dummy arrow files
    series_names = [
        "AT_mod.arrow",
        "AT_ror.arrow",
        "AT_mingen.arrow",
        "AT_reservoir_levels.arrow",
        "AT_maxpower.arrow"
    ]
    for name in series_names:
        df = pd.DataFrame({"v": [1.0] * 8760})
        df.to_feather(tmp_path / name)

    area_obj = MockArea(name="AT")
    hydro_data = {
        "properties": [
            {
                "follow_load": True,
                "inter_daily_breakdown": 3,
                "inter_daily_modulation": 3,
                "inter_monthly_breakdown": 1,
                "reservoir": True,
                "reservoir_capacity": 10000000,
                "pumping_efficiency": 1,
                "initialize_reservoir_date": 6,
                "use_water": False,
                "allocation": None,
                "series": None
            }
        ],
        "series": series_names,
        "allocation": {
            "FR": 2
        }
    }

    # Execute
    generate_hydro(area_obj, hydro_data)

    # Verify properties
    props = area_obj.hydro.properties_updated
    assert props.follow_load is True
    assert props.intra_daily_modulation == 3
    assert props.reservoir_capacity == 10000000

    # Verify allocations
    assert len(area_obj.hydro.allocations) == 1
    assert area_obj.hydro.allocations[0].area_id == "fr"
    assert area_obj.hydro.allocations[0].coefficient == 2

    # Verify series
    assert "mod" in area_obj.hydro.series
    assert "ror" in area_obj.hydro.series
    assert "mingen" in area_obj.hydro.series
    assert "reservoir" in area_obj.hydro.series
    assert "maxpower" in area_obj.hydro.series

def test_generate_hydro_handles_empty_properties_list(tmp_path, monkeypatch):
    monkeypatch.setenv("NAS_PATH", str(tmp_path))
    monkeypatch.setenv("PEGASE_HYDRO_TS_OUTPUT_DIRECTORY", str(tmp_path))

    area_obj = MockArea()
    hydro_data = {
        "properties": [],
        "series": []
    }

    # This should no longer fail
    generate_hydro(area_obj, hydro_data)
    
    props = area_obj.hydro.properties_updated
    assert props.follow_load is None
