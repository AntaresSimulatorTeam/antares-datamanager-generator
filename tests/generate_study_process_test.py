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

import json

from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from antares.datamanager.generator.generate_study_process import add_areas_to_study, add_links_to_study, load_study_data


@pytest.fixture
def mock_json_data():
    return {
        "test_study": {
            "areas": {
                "area1": {
                    "hydro": {
                        "every matrices name inside HydroMatrixName enum": "matrix hash",
                        "properties": "HydroProperties as JSON",
                    },
                    "ui": "AreaUI class as JSON",
                    "properties": "AreaProperties as JSON",
                    "loads": ["load_area1_2030-2031.txt.1b39a7db-53be-496d-aef0-1ab4692010a3.arrow"],
                },
                "area2": {
                    "hydro": {
                        "every matrices name inside HydroMatrixName enum": "matrix hash",
                        "properties": "HydroProperties as JSON",
                    },
                    "ui": "AreaUI class as JSON",
                    "properties": "AreaProperties as JSON",
                    "loads": ["load_area2_2030-2031.txt.1b39a7db-53be-496d-aef0-1ab4692010a3.arrow"],
                },
            },
            "links": {"area1/area2": {}},
        }
    }


@patch("builtins.open", new_callable=mock_open)
@patch("antares.datamanager.env_variables.EnvVariableType")
def test_load_study_data(mock_env_class, mock_open_file, mock_json_data):
    mock_env_instance = MagicMock()
    mock_env_instance.get_env_variable.return_value = "/mock/path"
    mock_env_class.return_value = mock_env_instance

    mock_open_file.return_value.__enter__.return_value.read.return_value = json.dumps(mock_json_data)

    study_name, areas, links, area_loads, area_thermals, random_gen_settings = load_study_data("test_study")

    assert study_name == "test_study"
    assert areas == ["area1", "area2"]
    assert area_loads == {
        "area1": ["load_area1_2030-2031.txt.1b39a7db-53be-496d-aef0-1ab4692010a3.arrow"],
        "area2": ["load_area2_2030-2031.txt.1b39a7db-53be-496d-aef0-1ab4692010a3.arrow"],
    }
    assert "area1/area2" in links


def test_add_areas_to_study_with_fixed_seed():
    mock_study = MagicMock()

    areas = ["area1", "area2"]
    area_loads = {}  # Ajout d’un mock pour le paramètre manquant
    area_thermals = {}

    add_areas_to_study(mock_study, areas, area_loads, area_thermals)
    assert mock_study.create_area.call_count == 2


@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
@patch("antares.datamanager.generator.generate_study_process.pd.read_feather")
def test_add_areas_to_study_calls_create_area_and_set_load(mock_read_feather, mock_generator_load_directory):
    mock_study = MagicMock()
    mock_area_obj = MagicMock()
    mock_study.create_area.return_value = mock_area_obj
    mock_generator_load_directory.return_value = "/fake/path"
    mock_read_feather.return_value = "fake_df"

    areas = ["A", "B"]
    area_loads = {"A": ["loadA.feather"], "B": ["loadB.feather", "loadB2.feather"]}
    area_thermals = {}

    add_areas_to_study(mock_study, areas, area_loads, area_thermals)

    assert mock_study.create_area.call_count == 2
    assert mock_area_obj.set_load.call_count == 3
    mock_read_feather.assert_any_call(Path("/fake/path/loadA.feather"))
    mock_read_feather.assert_any_call(Path("/fake/path/loadB.feather"))
    mock_read_feather.assert_any_call(Path("/fake/path/loadB2.feather"))


def test_add_links_to_study_calls_create_link():
    mock_study = MagicMock()
    mock_link = MagicMock()
    mock_study.create_link.return_value = mock_link

    links = {
        "FR/CH": {
            "winterHcIndirectMw": 1300,
            "winterHpDirectMw": 1200,
            "summerHcDirectMw": 1100,
            "winterHpIndirectMw": 1300,
            "ui": "LinkUi class as JSON",
            "summerHcIndirectMw": 1000,
            "capacity_direct": "matrix hash",
            "winterHcDirectMw": 1200,
            "summerHpDirectMw": 1300,
            "summerHpIndirectMw": 1200,
            "parameters": "matrix hash",
            "properties": "LinkProperties as JSON",
            "capacity_indirect": "matrix hash",
        },
        "FR/IT": {
            "winterHcIndirectMw": 1200,
            "winterHpDirectMw": 1200,
            "summerHcDirectMw": 1200,
            "winterHpIndirectMw": 1200,
            "ui": "LinkUi class as JSON",
            "summerHcIndirectMw": 1200,
            "capacity_direct": "matrix hash",
            "winterHcDirectMw": 1200,
            "summerHpDirectMw": 1200,
            "summerHpIndirectMw": 1200,
            "parameters": "matrix hash",
            "properties": "LinkProperties as JSON",
            "capacity_indirect": "matrix hash",
        },
    }

    # Patch the capacity generation functions to avoid randomness
    with patch("antares.datamanager.generator.generate_link_capacity_data", return_value="mock_df"):
        # When
        add_links_to_study(mock_study, links)

    # Then
    assert mock_study.create_link.call_count == 2
    assert mock_link.set_capacity_direct.call_count == 2
    assert mock_link.set_capacity_indirect.call_count == 2
