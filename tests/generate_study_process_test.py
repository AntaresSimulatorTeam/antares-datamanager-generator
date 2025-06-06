import pytest

import json

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
                },
                "area2": {
                    "hydro": {
                        "every matrices name inside HydroMatrixName enum": "matrix hash",
                        "properties": "HydroProperties as JSON",
                    },
                    "ui": "AreaUI class as JSON",
                    "properties": "AreaProperties as JSON",
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

    study_name, areas, links = load_study_data("test_study")

    assert study_name == "test_study"
    assert areas == ["area1", "area2"]
    assert "area1/area2" in links


def test_add_areas_to_study_with_fixed_seed():
    mock_study = MagicMock()

    areas = ["area1", "area2"]

    add_areas_to_study(mock_study, areas)

    assert mock_study.create_area.call_count == 2


def test_add_links_to_study_calls_create_link():
    # Given
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
