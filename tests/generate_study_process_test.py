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
import os

from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from antares.craft import APIconf, Month
from antares.datamanager.core.dependencies import get_study_factory
from antares.datamanager.core.settings import GenerationMode
from antares.datamanager.exceptions.exceptions import APIGenerationError, AreaGenerationError
from antares.datamanager.generator.generate_study_process import (
    _package_and_upload_local_study,
    add_areas_to_study,
    add_links_to_study,
    generate_study,
    read_study_data_from_json,
)
from antares.datamanager.generator.study_adapters import APIStudyFactory, LocalStudyFactory
from antares.datamanager.main import create_study


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
                    "thermals": {
                        "thermal_name": {
                            "properties": "ThermalProperties class as JSON",
                            "series": "matrix hash",
                            "fuel_cost": "matrix hash",
                            "c02_cpst": "matrix hash",
                            "data": "matrix hash",
                            "modulation": "matrix hash",
                        }
                    },
                    "ui": "AreaUI class as JSON",
                    "properties": {"energy_cost_unsupplied": "4000.0", "energy_cost_spilled": "200.0"},
                    "loads": ["load_area1_2030-2031.txt.1b39a7db-53be-496d-aef0-1ab4692010a3.arrow"],
                },
                "area2": {
                    "hydro": {
                        "every matrices name inside HydroMatrixName enum": "matrix hash",
                        "properties": "HydroProperties as JSON",
                    },
                    "thermals": {
                        "thermal_name": {
                            "properties": "ThermalProperties class as JSON",
                            "series": "matrix hash",
                            "fuel_cost": "matrix hash",
                            "c02_cpst": "matrix hash",
                            "data": "matrix hash",
                            "modulation": "matrix hash",
                        }
                    },
                    "ui": "AreaUI class as JSON",
                    "properties": {"energy_cost_unsupplied": "4000.0", "energy_cost_spilled": "1500.0"},
                    "loads": ["load_area2_2030-2031.txt.1b39a7db-53be-496d-aef0-1ab4692010a3.arrow"],
                },
            },
            "links": {"area1/area2": {}},
        }
    }


@patch("builtins.open", new_callable=mock_open)
@patch("antares.datamanager.generator.generate_study_process.settings")
def test_read_study_data_from_json(mock_settings, mock_open_file, mock_json_data):
    mock_settings.study_json_directory = Path("/mock/path")

    mock_open_file.return_value.__enter__.return_value.read.return_value = json.dumps(mock_json_data)

    study_data = read_study_data_from_json("test_study")

    assert study_data.name == "test_study"
    assert sorted(list(study_data.areas.keys())) == ["area1", "area2"]
    assert study_data.areas["area1"]["properties"]["energy_cost_unsupplied"] == "4000.0"
    assert study_data.areas["area1"]["properties"]["energy_cost_spilled"] == "200.0"
    assert study_data.area_loads == {
        "area1": ["load_area1_2030-2031.txt.1b39a7db-53be-496d-aef0-1ab4692010a3.arrow"],
        "area2": ["load_area2_2030-2031.txt.1b39a7db-53be-496d-aef0-1ab4692010a3.arrow"],
    }
    assert "area1/area2" in study_data.links


@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
def test_add_areas_to_study_with_fixed_seed(mock_load_dir):
    mock_load_dir.return_value = Path("/mock/load/dir")
    mock_study = MagicMock()

    from antares.datamanager.models.study_data_json_model import StudyData

    study_data = StudyData(name="test", areas={"area1": {}, "area2": {}})

    add_areas_to_study(mock_study, study_data)
    assert mock_study.create_area.call_count == 2


@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
@patch("antares.datamanager.generator.generate_study_process.pd.read_feather")
def test_add_areas_to_study_calls_create_area_and_set_load(mock_read_feather, mock_generator_load_directory):
    mock_study = MagicMock()
    mock_area_obj = MagicMock()
    mock_study.create_area.return_value = mock_area_obj
    mock_generator_load_directory.return_value = "/fake/path"
    mock_read_feather.return_value = "fake_df"

    from antares.datamanager.models.study_data_json_model import StudyData

    study_data = StudyData(
        name="test",
        areas={"A": {}, "B": {}},
        area_loads={"A": ["loadA.feather"], "B": ["loadB.feather", "loadB2.feather"]},
    )

    add_areas_to_study(mock_study, study_data)

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
            "hurdleCost": None,
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
            "hurdleCost": None,
        },
    }

    # Patch the capacity generation functions to avoid randomness
    with patch(
        "antares.datamanager.generator.generate_study_process.generate_link_capacity_df", return_value="mock_df"
    ):
        # When
        add_links_to_study(mock_study, links)

    # Then
    assert mock_study.create_link.call_count == 2
    assert mock_link.set_capacity_direct.call_count == 2
    assert mock_link.set_capacity_indirect.call_count == 2


def test_add_links_to_study_with_hurdle_cost():
    """
    Ensure that when hurdleCost is provided, we set link parameters and update properties.
    """
    mock_study = MagicMock()
    mock_link = MagicMock()
    mock_study.create_link.return_value = mock_link

    hurdle_value = 0.1
    links = {
        "A/B": {
            # minimal keys required by generate_link_capacity_df for both modes
            "winterHcDirectMw": 1,
            "winterHpDirectMw": 1,
            "summerHcDirectMw": 1,
            "summerHpDirectMw": 1,
            "winterHcIndirectMw": 1,
            "winterHpIndirectMw": 1,
            "summerHcIndirectMw": 1,
            "summerHpIndirectMw": 1,
            "hurdleCost": 0.1,
        }
    }

    with patch(
        "antares.datamanager.generator.generate_study_process.generate_link_capacity_df", return_value="mock_df"
    ):
        add_links_to_study(mock_study, links)

    # Property update method should be called once
    assert mock_link.update_properties.call_count == 1

    # set_parameters should be called with a DataFrame of shape (8760, 6)
    assert mock_link.set_parameters.call_count == 1
    df_passed = mock_link.set_parameters.call_args[0][0]
    # Lazy import pandas to avoid hard dependency at import time
    import pandas as pd

    assert isinstance(df_passed, pd.DataFrame)
    assert df_passed.shape == (8760, 6)
    # The first two columns should contain the hurdle value
    assert float(df_passed.iloc[0, 0]) == float(hurdle_value)
    assert float(df_passed.iloc[100, 1]) == float(hurdle_value)
    # The last four columns should be zeros
    assert float(df_passed.iloc[0, 2]) == 0.0
    assert float(df_passed.iloc[8759, 5]) == 0.0
    mock_study = MagicMock()
    mock_link = MagicMock()
    mock_study.create_link.return_value = mock_link

    links = {
        "FR/CH": {
            "winterHcIndirectMw": 1300,
            "winterHpDirectMw": 1200,
            "summerHcDirectMw": 1100,
            "winterHpIndirectMw": 1300,
            "hurdleCost": 0.1,
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
            "hurdleCost": 0.1,
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
    with patch(
        "antares.datamanager.generator.generate_study_process.generate_link_capacity_df", return_value="mock_df"
    ):
        # When
        add_links_to_study(mock_study, links)

    # Then
    assert mock_study.create_link.call_count == 2
    assert mock_link.set_capacity_direct.call_count == 2
    assert mock_link.set_capacity_indirect.call_count == 2


@patch("antares.datamanager.generator.generate_study_process.read_study_data_from_json")
@patch("antares.datamanager.generator.generate_study_process.add_areas_to_study")
@patch("antares.datamanager.generator.generate_study_process.add_links_to_study")
def test_generate_study_calls_all_functions(mock_add_links, mock_add_areas, mock_read_study_data_from_json):
    mock_study = MagicMock()
    mock_study.service.study_id = "dummy_id"
    mock_study.path = ""

    mock_factory = MagicMock()
    mock_factory.create_study.return_value = mock_study

    from antares.datamanager.models.study_data_json_model import StudyData

    study_data = StudyData(
        name="study_name",
        areas={"area1": {}, "area2": {}},
        links={"area1/area2": {}},
        area_loads={"area1": ["load1"], "area2": ["load2"]},
        area_thermals={"area1": {}, "area2": {}},
        enable_random_ts=True,
        nb_years=3,
    )
    mock_read_study_data_from_json.return_value = study_data

    result = generate_study("dummy_id", mock_factory)

    mock_read_study_data_from_json.assert_called_once_with("dummy_id")
    mock_factory.create_study.assert_called_once_with("study_name")
    args, _ = mock_study.update_settings.call_args
    study_settings_update = args[0]
    assert study_settings_update.general_parameters.first_month_in_year == Month.JULY  # Value from .env
    mock_add_areas.assert_called_once_with(mock_study, study_data)
    mock_add_links.assert_called_once_with(mock_study, study_data.links)
    mock_study.generate_thermal_timeseries.assert_called_once_with(3)
    assert result == {"message": "Study study_name successfully generated", "study_id": "dummy_id", "study_path": ""}


@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
def test_add_areas_to_study_creates_thermal_clusters(mock_generator_load_directory):
    mock_study = MagicMock()
    mock_area_obj = MagicMock()
    mock_study.create_area.return_value = mock_area_obj
    mock_generator_load_directory.return_value = "/fake/path"

    from antares.datamanager.models.study_data_json_model import StudyData

    study_data = StudyData(
        name="test",
        areas={"A": {}},
        area_loads={"A": []},
        area_thermals={
            "A": {
                "cluster1": {"properties": {"enabled": True, "nominal_capacity": 2.0}},
                "cluster2": {"properties": {"must_run": True}},
            }
        },
    )

    with (
        patch(
            "antares.datamanager.generator.generate_thermal_clusters.ThermalClusterProperties",
            side_effect=lambda **kwargs: kwargs,  # input as dictionary
        ),
        patch(
            "antares.datamanager.generator.generate_sts_clusters.STStorageProperties",
            side_effect=lambda **kwargs: kwargs,
        ),
    ):
        add_areas_to_study(mock_study, study_data)

        assert mock_area_obj.create_thermal_cluster.call_count == 2
        mock_area_obj.create_thermal_cluster.assert_any_call("cluster1", {"enabled": True, "nominal_capacity": 2.0})
        mock_area_obj.create_thermal_cluster.assert_any_call("cluster2", {"must_run": True})


@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
def test_add_areas_to_study_creates_sts_clusters(mock_generator_load_directory):
    mock_study = MagicMock()
    mock_area_obj = MagicMock()
    mock_study.create_area.return_value = mock_area_obj
    mock_generator_load_directory.return_value = "/fake/path"

    from antares.datamanager.models.study_data_json_model import StudyData

    study_data = StudyData(
        name="test",
        areas={"A": {}},
        area_loads={"A": []},
        area_sts={
            "A": {
                "sts1": {"properties": {"enabled": True, "group": "battery"}},
            }
        },
    )

    with patch(
        "antares.datamanager.generator.generate_sts_clusters.STStorageProperties",
        side_effect=lambda **kwargs: kwargs,
    ):
        add_areas_to_study(mock_study, study_data)

        assert mock_area_obj.create_st_storage.call_count == 1
        mock_area_obj.create_st_storage.assert_any_call("sts1", {"enabled": True, "group": "battery"})


@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
def test_add_areas_to_study_with_unit_count_and_data_sets_prepro(mock_load_dir):
    mock_load_dir.return_value = Path("/mock/load/dir")
    mock_study = MagicMock()
    mock_area_obj = MagicMock()
    mock_cluster_obj = MagicMock()
    mock_study.create_area.return_value = mock_area_obj
    mock_area_obj.create_thermal_cluster.return_value = mock_cluster_obj

    from antares.datamanager.models.study_data_json_model import StudyData

    study_data = StudyData(
        name="test",
        areas={"A": {}},
        area_loads={"A": []},
        area_thermals={
            "A": {
                "clusterX": {
                    "properties": {"enabled": True, "unit_count": 3},
                    "data": {
                        "fo_duration": 1,
                        "po_duration": 2,
                        "fo_monthly_rate": [10] * 12,
                        "po_monthly_rate": [20] * 12,
                        "npo_max_winter": 5,
                        "npo_max_summer": 10,
                        "nb_unit": 1,
                    },
                }
            }
        },
    )

    class DummyProps:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    sentinel_matrix = "PREPRO_MATRIX"

    with (
        patch(
            "antares.datamanager.generator.generate_thermal_clusters.ThermalClusterProperties",
            side_effect=lambda **kwargs: DummyProps(**kwargs),
        ),
        patch(
            "antares.datamanager.generator.generate_thermal_clusters.create_prepro_data_matrix",
            return_value=sentinel_matrix,
        ) as mock_create_matrix,
    ):
        add_areas_to_study(mock_study, study_data)

        # create_thermal_cluster called once with DummyProps instance
        assert mock_area_obj.create_thermal_cluster.call_count == 1
        # Ensure prepro matrix was computed with provided data and unit_count
        mock_create_matrix.assert_called_once()
        # And set on the cluster object
        mock_cluster_obj.set_prepro_data.assert_called_once_with(sentinel_matrix)


@patch("antares.datamanager.generator.generate_study_process.import_study_api")
@patch("antares.datamanager.generator.generate_study_process.shutil")
@patch("antares.datamanager.generator.generate_study_process.os.remove")
@patch("antares.datamanager.generator.generate_study_process.settings")
def test_package_and_upload_local_study_success(mock_settings, mock_os_remove, mock_shutil, mock_import_api):
    mock_settings.nas_path = Path("/mock/nas")
    mock_settings.api_host = "http://mock-api"
    mock_settings.api_token = "mock-token"
    mock_settings.verify_ssl = False

    study_name = "test_study_123"
    expected_study_path = Path("/mock/nas") / study_name
    mock_shutil.make_archive.return_value = "/mock/nas/test_study_123.zip"
    with patch("pathlib.Path.exists", return_value=True):
        _package_and_upload_local_study(study_name)

    # assert
    mock_shutil.make_archive.assert_called_once_with(str(expected_study_path), "zip", root_dir=expected_study_path)
    assert mock_import_api.call_count == 1
    args, _ = mock_import_api.call_args
    assert isinstance(args[0], APIconf)
    assert args[0].api_host == "http://mock-api"
    assert args[0].token == "mock-token"
    assert args[1] == Path("/mock/nas/test_study_123.zip")

    mock_os_remove.assert_called_once_with("/mock/nas/test_study_123.zip")
    mock_shutil.rmtree.assert_called_once_with(expected_study_path)


class TestInfrastructure:
    """
    Tests for main, adapters, config
    """

    @patch.dict(
        os.environ,
        {
            "GENERATION_MODE": "LOCAL",
            "NAS_PATH": "/env/nas",
            "PEGASE_LOAD_OUTPUT_DIRECTORY": "load_dir",
            "PEGASE_STUDY_JSON_OUTPUT_DIRECTORY": "json_dir",
            "PEGASE_PARAM_MODULATION_OUTPUT_DIRECTORY": "mod_dir",
        },
    )
    def test_settings_initialization(self):
        from antares.datamanager.core.settings import settings

        assert settings.generation_mode == GenerationMode.LOCAL
        assert settings.nas_path == Path("/env/nas")
        assert settings.load_output_directory == Path("/env/nas/load_dir")

    @patch("antares.datamanager.core.dependencies.settings")
    def test_get_study_factory_selection(self, mock_settings):
        mock_settings.generation_mode = GenerationMode.LOCAL
        mock_settings.nas_path = Path("/tmp/nas")

        factory = get_study_factory()
        assert isinstance(factory, LocalStudyFactory)

        mock_settings.generation_mode = GenerationMode.API
        mock_settings.api_host = "http://localhost"
        mock_settings.api_token = "token"

        factory = get_study_factory()
        assert isinstance(factory, APIStudyFactory)

    @patch("antares.datamanager.generator.study_adapters.create_study_local")
    def test_local_adapter_calls_service(self, mock_create_local):
        factory = LocalStudyFactory(Path("/root"))
        factory.create_study("StudyName", "8.8")
        mock_create_local.assert_called_once_with("StudyName", "8.8", Path("/root"))

    @patch("antares.datamanager.generator.study_adapters.create_study_api")
    def test_api_adapter_calls_service(self, mock_create_api):
        mock_conf = MagicMock()
        factory = APIStudyFactory(mock_conf)
        factory.create_study("StudyName", "8.8")
        mock_create_api.assert_called_once_with("StudyName", "8.8", mock_conf)

    @patch("antares.datamanager.main.generate_study")
    def test_main_create_study_function_direct(self, mock_generate_study):
        mock_factory = MagicMock()
        mock_generate_study.return_value = {"message": "success"}

        response = create_study("my_study_id", factory=mock_factory)

        assert response == {"message": "success"}
        mock_generate_study.assert_called_once_with("my_study_id", mock_factory)


@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
def test_add_areas_to_study_uses_ui_and_properties_from_json(mock_load_dir):
    mock_load_dir.return_value = Path("/mock/load/dir")
    mock_study = MagicMock()

    from antares.datamanager.models.study_data_json_model import StudyData

    study_data = StudyData(
        name="test",
        areas={
            "AreaJson": {
                "ui": {"x": 10, "y": 20, "color_rgb": [1, 2, 3]},
                "properties": {"energy_cost_unsupplied": "4000.0", "energy_cost_spilled": "200.5"},
            }
        },
    )

    class DummyUI:
        def __init__(self, **kwargs):
            self.x = kwargs.get("x")
            self.y = kwargs.get("y")
            self.color_rgb = kwargs.get("color_rgb")

    with patch("antares.datamanager.generator.generate_study_process.AreaUi", side_effect=lambda **kw: DummyUI(**kw)):
        add_areas_to_study(mock_study, study_data)

    # Verify create_area called once with matching UI and parsed properties
    assert mock_study.create_area.call_count == 1
    _, kwargs = mock_study.create_area.call_args
    ui_passed = kwargs.get("ui")
    props_passed = kwargs.get("properties")
    assert isinstance(ui_passed, DummyUI)
    assert ui_passed.x == 10 and ui_passed.y == 20 and ui_passed.color_rgb == [1, 2, 3]
    # Properties should be an object exposing attributes with float values
    assert hasattr(props_passed, "energy_cost_unsupplied")
    assert hasattr(props_passed, "energy_cost_spilled")
    assert float(getattr(props_passed, "energy_cost_unsupplied")) == 4000.0
    assert float(getattr(props_passed, "energy_cost_spilled")) == 200.5


@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
@patch("antares.datamanager.generator.generate_study_process.generate_random_color", return_value=[9, 9, 9])
@patch("antares.datamanager.generator.generate_study_process.generate_random_coordinate", return_value=(7, 8))
def test_add_areas_to_study_invalid_ui_falls_back_to_random(mock_coord, mock_color, mock_load_dir):
    mock_load_dir.return_value = Path("/mock/load/dir")
    mock_study = MagicMock()

    from antares.datamanager.models.study_data_json_model import StudyData

    study_data = StudyData(
        name="test",
        areas={"X": {"ui": {"unknown": 1}}},
    )

    class DummyUI:
        def __init__(self, **kwargs):
            self.x = kwargs.get("x")
            self.y = kwargs.get("y")
            self.color_rgb = kwargs.get("color_rgb")

    # The first call (from invalid dict) should raise, the second call (fallback with x,y,color_rgb) should succeed
    def area_ui_constructor(**kwargs):
        if "unknown" in kwargs:
            raise TypeError("invalid field")
        return DummyUI(**kwargs)

    with patch("antares.datamanager.generator.generate_study_process.AreaUi", side_effect=area_ui_constructor):
        add_areas_to_study(mock_study, study_data)

    assert mock_study.create_area.call_count == 1
    _, kwargs = mock_study.create_area.call_args
    ui_passed = kwargs.get("ui")
    assert isinstance(ui_passed, DummyUI)
    assert ui_passed.x == 7 and ui_passed.y == 8 and ui_passed.color_rgb == [9, 9, 9]
    mock_coord.assert_called_once()
    mock_color.assert_called_once()


@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
def test_add_areas_to_study_maps_api_error_to_area_error(mock_load_dir):
    mock_load_dir.return_value = Path("/mock/load/dir")
    mock_study = MagicMock()
    # Simulate API error when creating the area
    mock_study.create_area.side_effect = APIGenerationError("backend failed")

    from antares.datamanager.models.study_data_json_model import StudyData

    study_data = StudyData(
        name="test",
        areas={"ERR": {}},
    )

    with pytest.raises(AreaGenerationError) as exc:
        add_areas_to_study(mock_study, study_data)

    assert "ERR" in str(exc.value)
    assert "backend failed" in str(exc.value)
