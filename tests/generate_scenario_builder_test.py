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

from pathlib import Path
from unittest.mock import MagicMock, patch

from antares.craft.model.study import Study
from antares.datamanager.generator.generate_scenario_builder import generate_scenario_builder
from antares.datamanager.models.study_data_json_model import StudyData


def test_generate_scenario_builder_no_climatic_data():
    study = MagicMock(spec=Study)
    study_data = StudyData(name="test_study", scenario_builder_config={})
    used_files = set()

    generate_scenario_builder(study, study_data, used_files)

    study.get_scenario_builder.assert_not_called()


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_load(mock_settings, mock_read_feather):
    # Mock settings
    mock_settings.load_output_directory = Path("/tmp/load")
    mock_settings.nb_years = 10

    # Mock pandas df to return 5 columns (5 TS)
    mock_df = MagicMock()
    mock_df.shape = (8760, 5)
    mock_read_feather.return_value = mock_df

    # Mock study and areas
    study = MagicMock()
    area1 = MagicMock()
    area2 = MagicMock()
    study.get_areas.return_value = {"area1": area1, "area2": area2}

    # Mock scenario builder
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    # Study data
    study_data = StudyData(
        name="test_study",
        nb_years=10,
        scenario_builder_config={"Climatic data": ["load", "hydro"]},
        area_loads={"FR": ["load_fr.arrow"]},
    )

    # Patch Path.exists to return True for the load file
    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    # Verify sb.load.get_area was called for each area
    assert sb.load.get_area.call_count == 2
    assert sb.hydro.get_area.call_count == 2

    # Verify scenario series (1 to 5 repeated for 10 years)
    # Expected: [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
    expected_scenario = [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
    sb.load.get_area("area1").set_new_scenario.assert_called_with(expected_scenario)
    sb.hydro.get_area("area1").set_new_scenario.assert_called_with(expected_scenario)

    # Verify study.set_scenario_builder was called
    study.set_scenario_builder.assert_called_with(sb)


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_load_no_fr(mock_settings, mock_read_feather):
    # Test when FR is not present, it should take the first available area
    mock_settings.load_output_directory = Path("/tmp/load")
    mock_df = MagicMock()
    mock_df.shape = (8760, 3)
    mock_read_feather.return_value = mock_df

    study = MagicMock()
    area1 = MagicMock()
    study.get_areas.return_value = {"area1": area1}
    sb = MagicMock()
    # Initialize all area mocks to avoid AttributeError
    sb.load.get_area.return_value = MagicMock()
    sb.hydro.get_area.return_value = MagicMock()
    sb.wind.get_area.return_value = MagicMock()
    sb.solar.get_area.return_value = MagicMock()

    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        nb_years=5,
        scenario_builder_config={"Climatic data": ["load"]},
        area_loads={"area1": ["load_a1.arrow"]},
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    expected_scenario = [1, 2, 3, 1, 2]
    sb.load.get_area("area1").set_new_scenario.assert_called_with(expected_scenario)


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_validation_error(mock_settings, mock_read_feather):
    # Mock settings
    mock_settings.load_output_directory = Path("/tmp/load")
    mock_settings.hydro_ts_directory = Path("/tmp/hydro")

    # Mock load with 5 TS
    df_load = MagicMock()
    df_load.shape = (8760, 5)

    # Mock hydro with 3 TS
    df_hydro = MagicMock()
    df_hydro.shape = (8760, 3)

    mock_read_feather.side_effect = [df_load, df_load, df_hydro]

    study = MagicMock()
    study.get_areas.return_value = {"FR": MagicMock()}
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        nb_years=10,
        scenario_builder_config={"Climatic data": ["load", "hydro"]},
        area_loads={"FR": ["load_fr.arrow"]},
        area_hydro={"FR": {"series": ["hydro_fr.arrow"]}},
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        with pytest.raises(ValueError) as excinfo:
            generate_scenario_builder(study, study_data, set())

        assert "Timeseries must have the same number of columns" in str(excinfo.value)
        assert "Found 3 for hydro but expected 5" in str(excinfo.value)


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_reference_from_hydro(mock_settings, mock_read_feather):
    # Test that if load is NOT in climatic data and NOT in study, reference is taken from hydro
    mock_settings.hydro_ts_directory = Path("/tmp/hydro")
    mock_settings.nb_years = 5

    # Mock hydro with 3 TS
    df_hydro = MagicMock()
    df_hydro.shape = (8760, 3)
    mock_read_feather.return_value = df_hydro

    study = MagicMock()
    study.get_areas.return_value = {"FR": MagicMock()}
    sb = MagicMock()
    # Initialize area mocks
    sb.hydro.get_area.return_value = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        nb_years=5,
        scenario_builder_config={"Climatic data": ["hydro"]},
        area_loads={},  # No load
        area_hydro={"FR": {"series": ["hydro_fr.arrow"]}},
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    expected_scenario = [1, 2, 3, 1, 2]
    sb.hydro.get_area("FR").set_new_scenario.assert_called_with(expected_scenario)


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_reference_from_load_even_if_not_in_modulo(mock_settings, mock_read_feather):
    # Test that if load is NOT in climatic data but IS in study, it is used as reference
    mock_settings.load_output_directory = Path("/tmp/load")
    mock_settings.hydro_ts_directory = Path("/tmp/hydro")
    mock_settings.nb_years = 5

    # Mock load with 10 TS, hydro with 10 TS
    df_load = MagicMock()
    df_load.shape = (8760, 10)
    df_hydro = MagicMock()
    df_hydro.shape = (8760, 10)

    mock_read_feather.side_effect = [df_load, df_hydro]

    study = MagicMock()
    study.get_areas.return_value = {"FR": MagicMock()}
    sb = MagicMock()
    sb.hydro.get_area.return_value = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        nb_years=5,
        scenario_builder_config={"Climatic data": ["hydro"]},
        area_loads={"FR": ["load_fr.arrow"]},
        area_hydro={"FR": {"series": ["hydro_fr.arrow"]}},
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    # Reference should be 10 (from load), even if load is not in climatic data
    expected_scenario = [1, 2, 3, 4, 5]  # Only 5 years requested in study
    sb.hydro.get_area("FR").set_new_scenario.assert_called_with(expected_scenario)


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_priority_to_load(mock_settings, mock_read_feather):
    # Test that load is ALWAYS the priority for nb_ts if it exists
    mock_settings.load_output_directory = Path("/tmp/load")
    mock_settings.hydro_ts_directory = Path("/tmp/hydro")
    mock_settings.nb_years = 5

    # Mock load with 10 TS, hydro with 20 TS
    df_load = MagicMock()
    df_load.shape = (8760, 10)
    df_hydro = MagicMock()
    df_hydro.shape = (8760, 20)

    # _get_nb_ts will be called for each category in order until found
    # In my current implementation, it checks load first, then others.
    # In this test, we want to make sure it picks load's 10, not hydro's 20.

    # Sequence of calls in _generate_scenerased_climatic_data_series:
    # 1. _get_nb_ts(study_data, "load") -> returns 10
    # 2. _get_nb_ts(study_data, "load") (during validation) -> returns 10
    # 3. _get_nb_ts(study_data, "hydro") (during validation) -> returns 20 -> should RAISE error if both exist and differ

    mock_read_feather.side_effect = [df_load, df_load, df_hydro]

    study = MagicMock()
    study.get_areas.return_value = {"FR": MagicMock()}
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        nb_years=5,
        scenario_builder_config={"Climatic data": ["load", "hydro"]},
        area_loads={"FR": ["load_fr.arrow"]},
        area_hydro={"FR": {"series": ["hydro_fr.arrow"]}},
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        with pytest.raises(ValueError) as excinfo:
            generate_scenario_builder(study, study_data, set())

        assert "Found 20 for hydro but expected 10" in str(excinfo.value)


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_res_group_normalization(mock_settings, mock_read_feather):
    # Test that wind_onshore is correctly identified even with different group naming styles
    mock_settings.res_ts_directory = Path("/tmp/res")
    mock_settings.nb_years = 5

    df_res = MagicMock()
    df_res.shape = (8760, 187)
    mock_read_feather.return_value = df_res

    study = MagicMock()
    area_at = MagicMock()
    study.get_areas.return_value = {"AT": area_at}

    sb = MagicMock()
    # Mock ScenarioCluster for renewable
    mock_cluster_matrix = MagicMock()
    sb.renewable.get_cluster.return_value = mock_cluster_matrix
    study.get_scenario_builder.return_value = sb

    # Mock renewable clusters for AT
    cluster_c1 = MagicMock()
    cluster_c1.properties.group = "wind_onshore"
    cluster_c1.id = "c1"
    area_at.get_renewables.return_value = {"c1": cluster_c1}

    # Case: group name is "wind_onshore" (underscores)
    study_data = StudyData(
        name="test",
        nb_years=5,
        scenario_builder_config={"Climatic data": ["wind_onshore"]},
        area_res={"AT": {"clusters": {"c1": {"properties": {"group": "wind_onshore"}, "series": ["w.arrow"]}}}},
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    # Verify it was called via sb.renewable.get_cluster
    expected_scenario = [1, 2, 3, 4, 5]
    sb.renewable.get_cluster.assert_called_with("AT", "c1")
    mock_cluster_matrix.set_new_scenario.assert_called_with(expected_scenario)

    # Case: group name is "Wind Onshore" (spaces and caps)
    cluster_c1.properties.group = "Wind Onshore"
    mock_cluster_matrix.set_new_scenario.reset_mock()
    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())
    mock_cluster_matrix.set_new_scenario.assert_called_with(expected_scenario)


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_multi_area_res_search(mock_settings, mock_read_feather):
    # Test that _get_nb_ts searches across all areas if FR has no data for that category
    mock_settings.load_output_directory = Path("/tmp/load")
    mock_settings.res_ts_directory = Path("/tmp/res")
    mock_settings.nb_years = 5

    # Mock load with 200 TS
    df_load = MagicMock()
    df_load.shape = (8760, 200)

    # Mock wind with 187 TS
    df_wind = MagicMock()
    df_wind.shape = (8760, 187)

    # First call: _get_nb_ts("load") -> returns 200
    # Second call: _get_nb_ts("load") during validation -> returns 200
    # Third call: _get_nb_ts("wind_onshore") during validation -> should find AT data and return 187
    mock_read_feather.side_effect = [df_load, df_load, df_wind]

    study = MagicMock()
    study.get_areas.return_value = {"FR": MagicMock(), "AT": MagicMock()}
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        nb_years=5,
        scenario_builder_config={"Climatic data": ["load", "wind_onshore"]},
        area_loads={"FR": ["load_fr.arrow"]},
        area_res={
            "FR": {"clusters": {}},  # No RES clusters for FR
            "AT": {"clusters": {"c1": {"properties": {"group": "wind_onshore"}, "series": ["wind_at.arrow"]}}},
        },
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        with pytest.raises(ValueError) as excinfo:
            generate_scenario_builder(study, study_data, set())

        assert "Found 187 for wind_onshore but expected 200" in str(excinfo.value)


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_res_direct_tech_structure(mock_settings, mock_read_feather):
    # Test that _get_nb_ts correctly handles RES structured by technology (Case 1)
    mock_settings.load_output_directory = Path("/tmp/load")
    mock_settings.res_ts_directory = Path("/tmp/res")
    mock_settings.nb_years = 5

    # Mock load with 200 TS
    df_load = MagicMock()
    df_load.shape = (8760, 200)

    # Mock wind with 186 TS
    df_wind = MagicMock()
    df_wind.shape = (8760, 186)

    mock_read_feather.side_effect = [df_load, df_load, df_wind]

    study = MagicMock()
    study.get_areas.return_value = {"FR": MagicMock(), "AT": MagicMock()}
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        nb_years=5,
        scenario_builder_config={"Climatic data": ["load", "wind_onshore"]},
        area_loads={"FR": ["load_fr.arrow"]},
        area_res={"AT": {"wind_onshore": {"capacity": 2000, "series": ["wind_at.arrow"]}}},
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        with pytest.raises(ValueError) as excinfo:
            generate_scenario_builder(study, study_data, set())

        assert "Found 186 for wind_onshore but expected 200" in str(excinfo.value)


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_thermal_nuclearfr(mock_settings, mock_read_feather):
    mock_settings.nuclear_modulation_ts_directory = Path("/tmp/nuclear_modulation")
    mock_settings.nb_years = 5

    df_limit = MagicMock()
    df_limit.shape = (8760, 3)
    df_daily = MagicMock()
    df_daily.shape = (365, 3)
    df_weekly = MagicMock()
    df_weekly.shape = (52, 3)

    mock_read_feather.side_effect = [df_limit, df_daily, df_weekly]

    study = MagicMock()
    sb = MagicMock()
    mock_bc_group = MagicMock()
    sb.binding_constraint.get_group.return_value = mock_bc_group
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        nb_years=5,
        scenario_builder_config={"Thermal": ["nuclearfr", "z_p2g_asservi", "nucleary_nuc_modulation"]},
        nuclear_modulation_binding_constraints={
            "group": "scenarised200",
            "nbTsColumns": 200,
            "constraints": [
                {"name": "Nuc_modulation_limit", "series": "limit.arrow"},
                {"name": "Nuc_modulation_daily", "series": "daily.arrow"},
                {"name": "Nuc_modulation_weekly", "series": "weekly.arrow"},
            ],
        },
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    sb.binding_constraint.get_group.assert_called_with("scenarised200")
    expected_scenario = [1, 2, 3, 1, 2]
    mock_bc_group.set_new_scenario.assert_called_with(expected_scenario)
    study.set_scenario_builder.assert_called_with(sb)


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_thermal_nuclearfr_column_mismatch_raises(mock_settings, mock_read_feather):
    mock_settings.nuclear_modulation_ts_directory = Path("/tmp/nuclear_modulation")

    df_limit = MagicMock()
    df_limit.shape = (8760, 3)
    df_daily = MagicMock()
    df_daily.shape = (365, 5)  # Mismatch

    mock_read_feather.side_effect = [df_limit, df_daily]

    study = MagicMock()
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        nb_years=5,
        scenario_builder_config={"Thermal": ["nuclearfr"]},
        nuclear_modulation_binding_constraints={
            "group": "scenarised200",
            "constraints": [
                {"name": "nuc_modulation_limit", "series": "limit.arrow"},
                {"name": "nuc_modulation_daily", "series": "daily.arrow"},
            ],
        },
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        with pytest.raises(ValueError) as excinfo:
            generate_scenario_builder(study, study_data, set())

    assert "Timeseries must have the same number of columns for nuclear modulation constraints" in str(excinfo.value)
    assert "Found 5 for nuc_modulation_daily but expected 3" in str(excinfo.value)


def test_generate_scenario_builder_thermal_empty_nuclear_modulation():
    study = MagicMock()
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        nb_years=5,
        scenario_builder_config={"Thermal": ["nuclearfr"]},
        nuclear_modulation_binding_constraints=None,
    )

    generate_scenario_builder(study, study_data, set())
    study.set_scenario_builder.assert_called_with(sb)
    sb.binding_constraint.get_group.assert_not_called()


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_thermal_nuclearfr_fallback_nb_ts_columns(mock_settings, mock_read_feather):
    mock_settings.nuclear_modulation_ts_directory = Path("/tmp/nuclear_modulation")

    study = MagicMock()
    sb = MagicMock()
    mock_bc_group = MagicMock()
    sb.binding_constraint.get_group.return_value = mock_bc_group
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        nb_years=3,
        scenario_builder_config={"Thermal": ["nuclearfr"]},
        nuclear_modulation_binding_constraints={
            "group": "scenarised200",
            "nbTsColumns": 2,
            "constraints": [],
        },
    )

    generate_scenario_builder(study, study_data, set())

    sb.binding_constraint.get_group.assert_called_with("scenarised200")
    expected_scenario = [1, 2, 1]
    mock_bc_group.set_new_scenario.assert_called_with(expected_scenario)


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_thermal_nuclear_fr_clusters_from_feather(mock_settings, mock_read_feather):
    mock_settings.nuclear_availability_ts_directory = Path("/tmp/nuclear_ts")
    mock_settings.nuclear_modulation_ts_directory = Path("/tmp/nuclear_modulation")

    df_nuc1 = MagicMock()
    df_nuc1.shape = (8760, 4)
    df_nuc2 = MagicMock()
    df_nuc2.shape = (8760, 2)

    def read_feather_side_effect(path):
        if "nuc1.arrow" in str(path):
            return df_nuc1
        elif "nuc2.arrow" in str(path):
            return df_nuc2
        return MagicMock()

    mock_read_feather.side_effect = read_feather_side_effect

    study = MagicMock()
    sb = MagicMock()
    mock_thermal_cluster_1 = MagicMock()
    mock_thermal_cluster_2 = MagicMock()
    mock_thermal_cluster_gas = MagicMock()

    def get_cluster_side_effect(area_id, cluster_id):
        if cluster_id == "fr_nuc_1":
            return mock_thermal_cluster_1
        elif cluster_id == "fr_nuc_2":
            return mock_thermal_cluster_2
        return mock_thermal_cluster_gas

    sb.thermal.get_cluster.side_effect = get_cluster_side_effect
    study.get_scenario_builder.return_value = sb

    # Area FR
    mock_fr_area = MagicMock()
    mock_fr_area.id = "fr"
    mock_fr_area.name = "FR"

    cluster_1 = MagicMock()
    cluster_1.properties.group = "nuclear"
    cluster_2 = MagicMock()
    cluster_2.properties.group = "nuclear"
    cluster_gas = MagicMock()
    cluster_gas.properties.group = "gas"

    mock_fr_area.get_thermals.return_value = {
        "fr_nuc_1": cluster_1,
        "fr_nuc_2": cluster_2,
        "fr_gas": cluster_gas,
    }

    # Area BE with nuclear (should not be touched by nuclearfr)
    mock_be_area = MagicMock()
    mock_be_area.id = "be"
    mock_be_area.name = "BE"
    cluster_be_nuc = MagicMock()
    cluster_be_nuc.properties.group = "nuclear"
    mock_be_area.get_thermals.return_value = {"be_nuc_1": cluster_be_nuc}

    study.get_areas.return_value = {"fr": mock_fr_area, "be": mock_be_area}

    study_data = StudyData(
        name="test_study",
        nb_years=5,
        scenario_builder_config={"Thermal": ["nuclearfr"]},
        area_nuclear={
            "fr": {
                "clusters": {
                    "fr_nuc_1": {"series": "nuc1.arrow"},
                    "fr_nuc_2": {"series": "nuc2.arrow"},
                }
            }
        },
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    # Cluster 1: nb_ts = 4 -> scenario for 5 years: [1, 2, 3, 4, 1]
    mock_thermal_cluster_1.set_new_scenario.assert_called_with([1, 2, 3, 4, 1])
    # Cluster 2: nb_ts = 2 -> scenario for 5 years: [1, 2, 1, 2, 1]
    mock_thermal_cluster_2.set_new_scenario.assert_called_with([1, 2, 1, 2, 1])
    # Gas cluster should not have scenario set
    mock_thermal_cluster_gas.set_new_scenario.assert_not_called()


def test_generate_scenario_builder_thermal_nuclear_fr_clusters_from_matrix():
    study = MagicMock()
    sb = MagicMock()
    mock_thermal_cluster_1 = MagicMock()
    sb.thermal.get_cluster.return_value = mock_thermal_cluster_1
    study.get_scenario_builder.return_value = sb

    mock_fr_area = MagicMock()
    mock_fr_area.id = "fr"
    mock_fr_area.name = "FR"

    cluster_1 = MagicMock()
    cluster_1.properties.group = "nuclear"
    matrix_mock = MagicMock()
    matrix_mock.shape = (8760, 3)
    cluster_1.get_series_matrix.return_value = matrix_mock

    mock_fr_area.get_thermals.return_value = {"fr_nuc_1": cluster_1}
    study.get_areas.return_value = {"fr": mock_fr_area}

    study_data = StudyData(
        name="test_study",
        nb_years=5,
        scenario_builder_config={"Thermal": ["nuclearfr"]},
        area_nuclear={},
    )

    generate_scenario_builder(study, study_data, set())

    # nb_ts = 3 -> scenario for 5 years: [1, 2, 3, 1, 2]
    mock_thermal_cluster_1.set_new_scenario.assert_called_with([1, 2, 3, 1, 2])
    sb.thermal.get_cluster.assert_called_with("fr", "fr_nuc_1")


def test_generate_scenario_builder_thermal_nuclear_fr_clusters_fallback_default_1():
    study = MagicMock()
    sb = MagicMock()
    mock_thermal_cluster_1 = MagicMock()
    sb.thermal.get_cluster.return_value = mock_thermal_cluster_1
    study.get_scenario_builder.return_value = sb

    mock_fr_area = MagicMock()
    mock_fr_area.id = "fr"
    mock_fr_area.name = "FR"

    cluster_1 = MagicMock()
    cluster_1.properties.group = "nuclear"
    cluster_1.get_series_matrix.return_value = None

    mock_fr_area.get_thermals.return_value = {"fr_nuc_1": cluster_1}
    study.get_areas.return_value = {"fr": mock_fr_area}

    study_data = StudyData(
        name="test_study",
        nb_years=3,
        scenario_builder_config={"Thermal": ["nuclearfr"]},
        area_nuclear={},
    )

    generate_scenario_builder(study, study_data, set())

    # nb_ts fallback = 1 -> scenario for 3 years: [1, 1, 1]
    mock_thermal_cluster_1.set_new_scenario.assert_called_with([1, 1, 1])


def test_generate_scenario_builder_links_from_study_link():
    study = MagicMock()
    sb = MagicMock()
    mock_link_sb = MagicMock()
    sb.link.get_link.return_value = mock_link_sb
    study.get_scenario_builder.return_value = sb

    mock_link = MagicMock()
    mock_df = MagicMock()
    mock_df.shape = (8760, 4)
    mock_link.get_capacity_direct.return_value = mock_df

    study.get_links.return_value = {"nl / z_p2h_pachybride": mock_link}

    study_data = StudyData(
        name="test_study",
        nb_years=6,
        scenario_builder_config={"Links": ["nl/z_p2h_pachybride"]},
    )

    generate_scenario_builder(study, study_data, set())

    sb.link.get_link.assert_called_with("nl / z_p2h_pachybride")
    mock_link_sb.set_new_scenario.assert_called_with([1, 2, 3, 4, 1, 2])
    study.set_scenario_builder.assert_called_with(sb)


@patch("antares.datamanager.generator.generate_scenario_builder.generate_link_capacity_df")
def test_generate_scenario_builder_links_from_study_data(mock_gen_cap):
    study = MagicMock()
    sb = MagicMock()
    mock_link_sb = MagicMock()
    sb.link.get_link.return_value = mock_link_sb
    study.get_scenario_builder.return_value = sb
    study.get_links.return_value = {}

    mock_df = MagicMock()
    mock_df.shape = (8760, 3)
    mock_gen_cap.return_value = mock_df

    study_data = StudyData(
        name="test_study",
        nb_years=4,
        scenario_builder_config={"Links": ["at/fr"]},
        links={"at/fr": {"winterhcdirectmw": 1000}},
        seed_tsgen_link=42,
    )

    generate_scenario_builder(study, study_data, set())

    sb.link.get_link.assert_called_with("at / fr")
    mock_link_sb.set_new_scenario.assert_called_with([1, 2, 3, 1])
    mock_gen_cap.assert_called_once_with(
        {"winterhcdirectmw": 1000},
        "direct",
        seed_tsgen_link=42,
        link_name="at-fr",
    )


def test_generate_scenario_builder_links_fallback_default_1():
    study = MagicMock()
    sb = MagicMock()
    mock_link_sb = MagicMock()
    sb.link.get_link.return_value = mock_link_sb
    study.get_scenario_builder.return_value = sb
    study.get_links.return_value = {}

    study_data = StudyData(
        name="test_study",
        nb_years=3,
        scenario_builder_config={"links": ["nl/z_p2h_pachybride"]},
        links={},
    )

    generate_scenario_builder(study, study_data, set())

    sb.link.get_link.assert_called_with("nl / z_p2h_pachybride")
    mock_link_sb.set_new_scenario.assert_called_with([1, 1, 1])


def test_generate_scenario_builder_links_reversed_order_and_casing():
    study = MagicMock()
    sb = MagicMock()
    mock_link_sb = MagicMock()
    sb.link.get_link.return_value = mock_link_sb
    study.get_scenario_builder.return_value = sb

    mock_link = MagicMock()
    mock_df = MagicMock()
    mock_df.shape = (8760, 2)
    mock_link.get_capacity_direct.return_value = mock_df

    study.get_links.return_value = {"at / fr": mock_link}

    study_data = StudyData(
        name="test_study",
        nb_years=3,
        scenario_builder_config={"LINKS": ["FR / AT"]},
    )

    generate_scenario_builder(study, study_data, set())

    sb.link.get_link.assert_called_with("at / fr")
    mock_link_sb.set_new_scenario.assert_called_with([1, 2, 1])
