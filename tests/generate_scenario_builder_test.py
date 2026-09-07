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

    # Mock pandas df to return 5 columns (5 TS)
    mock_df = MagicMock()
    mock_df.shape = (8760, 5)
    mock_read_feather.return_value = mock_df

    # Mock study and areas
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 10
    area1 = MagicMock()
    area2 = MagicMock()
    study.get_areas.return_value = {"area1": area1, "area2": area2}

    # Mock scenario builder
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    # Study data
    study_data = StudyData(
        name="test_study",
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
    study.get_settings.return_value.general_parameters.nb_years = 5
    sb = MagicMock()
    # Initialize all area mocks to avoid AttributeError
    sb.load.get_area.return_value = MagicMock()
    sb.hydro.get_area.return_value = MagicMock()
    sb.wind.get_area.return_value = MagicMock()
    sb.solar.get_area.return_value = MagicMock()

    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
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

    # Mock hydro with 3 TS
    df_hydro = MagicMock()
    df_hydro.shape = (8760, 3)
    mock_read_feather.return_value = df_hydro

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 5
    study.get_areas.return_value = {"FR": MagicMock()}
    sb = MagicMock()
    # Initialize area mocks
    sb.hydro.get_area.return_value = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
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

    # Mock load with 10 TS, hydro with 10 TS
    df_load = MagicMock()
    df_load.shape = (8760, 10)
    df_hydro = MagicMock()
    df_hydro.shape = (8760, 10)

    mock_read_feather.side_effect = [df_load, df_hydro]

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 5
    study.get_areas.return_value = {"FR": MagicMock()}
    sb = MagicMock()
    sb.hydro.get_area.return_value = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
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
    study.get_settings.return_value.general_parameters.nb_years = 5
    study.get_areas.return_value = {"FR": MagicMock()}
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
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
def test_generate_scenario_builder_climatic_data_excludes_y_nuc_modulation_from_load_and_hydro(
    mock_settings, mock_read_feather
):
    mock_settings.load_output_directory = Path("/tmp/load")
    mock_settings.hydro_ts_directory = Path("/tmp/hydro")

    df_load = MagicMock()
    df_load.shape = (8760, 5)
    df_hydro = MagicMock()
    df_hydro.shape = (8760, 5)

    mock_read_feather.side_effect = [df_load, df_load, df_hydro]

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3
    area_fr = MagicMock()
    area_fr.name = "FR"
    area_y = MagicMock()
    area_y.name = "y_nuc_modulation"
    area_be = MagicMock()
    area_be.name = "BE"

    study.get_areas.return_value = {
        "fr": area_fr,
        "y_nuc_modulation": area_y,
        "be": area_be,
    }

    sb = MagicMock()
    mock_load_fr = MagicMock()
    mock_load_y = MagicMock()
    mock_load_be = MagicMock()
    mock_hydro_fr = MagicMock()
    mock_hydro_y = MagicMock()
    mock_hydro_be = MagicMock()

    def get_load_area_side_effect(area_id):
        if area_id == "fr":
            return mock_load_fr
        elif area_id == "y_nuc_modulation":
            return mock_load_y
        elif area_id == "be":
            return mock_load_be
        return MagicMock()

    def get_hydro_area_side_effect(area_id):
        if area_id == "fr":
            return mock_hydro_fr
        elif area_id == "y_nuc_modulation":
            return mock_hydro_y
        elif area_id == "be":
            return mock_hydro_be
        return MagicMock()

    sb.load.get_area.side_effect = get_load_area_side_effect
    sb.hydro.get_area.side_effect = get_hydro_area_side_effect
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"Climatic data": ["load", "hydro"]},
        area_loads={"FR": ["load_fr.arrow"]},
        area_hydro={"FR": {"series": ["hydro_fr.arrow"]}},
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    expected_scenario = [1, 2, 3]
    # FR and BE must be scenarized
    mock_load_fr.set_new_scenario.assert_called_with(expected_scenario)
    mock_hydro_fr.set_new_scenario.assert_called_with(expected_scenario)
    mock_load_be.set_new_scenario.assert_called_with(expected_scenario)
    mock_hydro_be.set_new_scenario.assert_called_with(expected_scenario)

    # y_nuc_modulation must be excluded from load and hydro scenarization
    mock_load_y.set_new_scenario.assert_not_called()
    mock_hydro_y.set_new_scenario.assert_not_called()


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_res_group_normalization(mock_settings, mock_read_feather):
    # Test that wind_onshore is correctly identified even with different group naming styles
    mock_settings.res_ts_directory = Path("/tmp/res")

    df_res = MagicMock()
    df_res.shape = (8760, 187)
    mock_read_feather.return_value = df_res

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 5
    study.get_settings.return_value.general_parameters.nb_years = 5
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
    study.get_settings.return_value.general_parameters.nb_years = 5
    study.get_areas.return_value = {"FR": MagicMock(), "AT": MagicMock()}
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
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

    # Mock load with 200 TS
    df_load = MagicMock()
    df_load.shape = (8760, 200)

    # Mock wind with 186 TS
    df_wind = MagicMock()
    df_wind.shape = (8760, 186)

    mock_read_feather.side_effect = [df_load, df_load, df_wind]

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 5
    study.get_areas.return_value = {"FR": MagicMock(), "AT": MagicMock()}
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
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

    df_limit = MagicMock()
    df_limit.shape = (8760, 3)
    df_daily = MagicMock()
    df_daily.shape = (365, 3)
    df_weekly = MagicMock()
    df_weekly.shape = (52, 3)

    mock_read_feather.side_effect = [df_limit, df_daily, df_weekly]

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 5
    sb = MagicMock()
    mock_bc_group = MagicMock()
    sb.binding_constraint.get_group.return_value = mock_bc_group
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
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
    study.get_settings.return_value.general_parameters.nb_years = 5
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
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
    study.get_settings.return_value.general_parameters.nb_years = 3
    sb = MagicMock()
    mock_bc_group = MagicMock()
    sb.binding_constraint.get_group.return_value = mock_bc_group
    study.get_scenario_builder.return_value = sb

    study_data = StudyData(
        name="test_study",
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
    study.get_settings.return_value.general_parameters.nb_years = 5
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
    study.get_settings.return_value.general_parameters.nb_years = 5

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
        scenario_builder_config={"Thermal": ["nuclearfr"]},
        area_nuclear={},
    )

    generate_scenario_builder(study, study_data, set())

    # nb_ts = 3 -> scenario for 5 years: [1, 2, 3, 1, 2]
    mock_thermal_cluster_1.set_new_scenario.assert_called_with([1, 2, 3, 1, 2])
    sb.thermal.get_cluster.assert_called_with("fr", "fr_nuc_1")


def test_generate_scenario_builder_thermal_nuclear_fr_clusters_fallback_default_1():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3

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
        scenario_builder_config={"Thermal": ["nuclearfr"]},
        area_nuclear={},
    )

    generate_scenario_builder(study, study_data, set())

    # nb_ts fallback = 1 -> scenario for 3 years: [1, 1, 1]
    mock_thermal_cluster_1.set_new_scenario.assert_called_with([1, 1, 1])


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_thermal_y_nuc_modulation_clusters_from_feather(mock_settings, mock_read_feather):
    mock_settings.nuclear_availability_ts_directory = Path("/tmp/nuclear_ts")

    df_y_nuc1 = MagicMock()
    df_y_nuc1.shape = (8760, 4)
    df_y_nuc2 = MagicMock()
    df_y_nuc2.shape = (8760, 2)

    def read_feather_side_effect(path):
        if "y_nuc1.arrow" in str(path):
            return df_y_nuc1
        elif "y_nuc2.arrow" in str(path):
            return df_y_nuc2
        return MagicMock()

    mock_read_feather.side_effect = read_feather_side_effect

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 5

    sb = MagicMock()
    mock_thermal_cluster_1 = MagicMock()
    mock_thermal_cluster_2 = MagicMock()
    mock_thermal_cluster_other = MagicMock()

    def get_cluster_side_effect(area_id, cluster_id):
        if cluster_id == "y_nuc_1":
            return mock_thermal_cluster_1
        elif cluster_id == "y_nuc_2":
            return mock_thermal_cluster_2
        return mock_thermal_cluster_other

    sb.thermal.get_cluster.side_effect = get_cluster_side_effect
    study.get_scenario_builder.return_value = sb

    # Area y_nuc_modulation
    mock_y_area = MagicMock()
    mock_y_area.id = "y_nuc_modulation"
    mock_y_area.name = "y_nuc_modulation"

    cluster_1 = MagicMock()
    cluster_1.properties.group = "nuclear"
    cluster_2 = MagicMock()
    cluster_2.properties.group = "nuclear"
    cluster_other = MagicMock()
    cluster_other.properties.group = "other"

    mock_y_area.get_thermals.return_value = {
        "y_nuc_1": cluster_1,
        "y_nuc_2": cluster_2,
        "y_other": cluster_other,
    }

    # Area FR (should not be touched by y_nuc_modulation only config)
    mock_fr_area = MagicMock()
    mock_fr_area.id = "fr"
    mock_fr_area.name = "FR"
    cluster_fr_nuc = MagicMock()
    cluster_fr_nuc.properties.group = "nuclear"
    mock_fr_area.get_thermals.return_value = {"fr_nuc_1": cluster_fr_nuc}

    study.get_areas.return_value = {"y_nuc_modulation": mock_y_area, "fr": mock_fr_area}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"Thermal": ["y_nuc_modulation"]},
        area_nuclear={
            "y_nuc_modulation": {
                "clusters": {
                    "y_nuc_1": {"series": "y_nuc1.arrow"},
                    "y_nuc_2": {"series": "y_nuc2.arrow"},
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
    # Other group cluster should not have scenario set
    mock_thermal_cluster_other.set_new_scenario.assert_not_called()


def test_generate_scenario_builder_thermal_y_nuc_modulation_clusters_from_matrix():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 5

    sb = MagicMock()
    mock_thermal_cluster_1 = MagicMock()
    sb.thermal.get_cluster.return_value = mock_thermal_cluster_1
    study.get_scenario_builder.return_value = sb

    mock_y_area = MagicMock()
    mock_y_area.id = "y_nuc_modulation"
    mock_y_area.name = "y_nuc_modulation"

    cluster_1 = MagicMock()
    cluster_1.properties.group = "nuclear"
    matrix_mock = MagicMock()
    matrix_mock.shape = (8760, 3)
    cluster_1.get_series_matrix.return_value = matrix_mock

    mock_y_area.get_thermals.return_value = {"y_nuc_1": cluster_1}
    study.get_areas.return_value = {"y_nuc_modulation": mock_y_area}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"thermal": ["nucleary_nuc_modulation"]},
        area_nuclear={},
    )

    generate_scenario_builder(study, study_data, set())

    # nb_ts = 3 -> scenario for 5 years: [1, 2, 3, 1, 2]
    mock_thermal_cluster_1.set_new_scenario.assert_called_with([1, 2, 3, 1, 2])
    sb.thermal.get_cluster.assert_called_with("y_nuc_modulation", "y_nuc_1")


def test_generate_scenario_builder_thermal_y_nuc_modulation_clusters_fallback_default_1():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3

    sb = MagicMock()
    mock_thermal_cluster_1 = MagicMock()
    sb.thermal.get_cluster.return_value = mock_thermal_cluster_1
    study.get_scenario_builder.return_value = sb

    mock_y_area = MagicMock()
    mock_y_area.id = "y_nuc_modulation"
    mock_y_area.name = "y_nuc_modulation"

    cluster_1 = MagicMock()
    cluster_1.properties.group = "nuclear"
    cluster_1.get_series_matrix.return_value = None

    mock_y_area.get_thermals.return_value = {"y_nuc_1": cluster_1}
    study.get_areas.return_value = {"y_nuc_modulation": mock_y_area}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"Thermal": ["y_nuc_modulation"]},
        area_nuclear={},
    )

    generate_scenario_builder(study, study_data, set())

    # nb_ts fallback = 1 -> scenario for 3 years: [1, 1, 1]
    mock_thermal_cluster_1.set_new_scenario.assert_called_with([1, 1, 1])


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_thermal_z_p2g_asservi_clusters_from_feather(mock_settings, mock_read_feather):
    mock_settings.nuclear_availability_ts_directory = Path("/tmp/nuclear_ts")

    df_cluster1 = MagicMock()
    df_cluster1.shape = (8760, 4)
    df_cluster2 = MagicMock()
    df_cluster2.shape = (8760, 2)

    def read_feather_side_effect(path):
        if "p2g_cluster1.arrow" in str(path):
            return df_cluster1
        elif "p2g_cluster2.arrow" in str(path):
            return df_cluster2
        return MagicMock()

    mock_read_feather.side_effect = read_feather_side_effect

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 5
    sb = MagicMock()
    mock_thermal_cluster_1 = MagicMock()
    mock_thermal_cluster_2 = MagicMock()
    mock_thermal_cluster_fr = MagicMock()

    def get_cluster_side_effect(area_id, cluster_id):
        if cluster_id == "p2g_1":
            return mock_thermal_cluster_1
        elif cluster_id == "p2g_2":
            return mock_thermal_cluster_2
        return mock_thermal_cluster_fr

    sb.thermal.get_cluster.side_effect = get_cluster_side_effect
    study.get_scenario_builder.return_value = sb

    # Area z_p2g_asservi with multiple thermal groups (all groups should be scenarized)
    mock_p2g_area = MagicMock()
    mock_p2g_area.id = "z_p2g_asservi"
    mock_p2g_area.name = "z_p2g_asservi"

    cluster_1 = MagicMock()
    cluster_1.properties.group = "gas"
    cluster_2 = MagicMock()
    cluster_2.properties.group = "other"

    mock_p2g_area.get_thermals.return_value = {
        "p2g_1": cluster_1,
        "p2g_2": cluster_2,
    }

    # Area FR (should not be touched)
    mock_fr_area = MagicMock()
    mock_fr_area.id = "fr"
    mock_fr_area.name = "FR"
    cluster_fr = MagicMock()
    cluster_fr.properties.group = "nuclear"
    mock_fr_area.get_thermals.return_value = {"fr_cluster": cluster_fr}

    study.get_areas.return_value = {"z_p2g_asservi": mock_p2g_area, "fr": mock_fr_area}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"Thermal": ["*@z_p2g_asservi"]},
        area_thermals={
            "z_p2g_asservi": {
                "p2g_1": {"series": "p2g_cluster1.arrow"},
                "p2g_2": {"series": "p2g_cluster2.arrow"},
            }
        },
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    # Cluster 1: nb_ts = 4 -> scenario for 5 years: [1, 2, 3, 4, 1]
    mock_thermal_cluster_1.set_new_scenario.assert_called_with([1, 2, 3, 4, 1])
    # Cluster 2: nb_ts = 2 -> scenario for 5 years: [1, 2, 1, 2, 1]
    mock_thermal_cluster_2.set_new_scenario.assert_called_with([1, 2, 1, 2, 1])
    # Cluster in FR should not be called
    mock_thermal_cluster_fr.set_new_scenario.assert_not_called()


def test_generate_scenario_builder_thermal_z_p2g_asservi_clusters_from_matrix():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 5

    sb = MagicMock()
    mock_thermal_cluster_1 = MagicMock()
    sb.thermal.get_cluster.return_value = mock_thermal_cluster_1
    study.get_scenario_builder.return_value = sb

    mock_p2g_area = MagicMock()
    mock_p2g_area.id = "z_p2g_asservi"
    mock_p2g_area.name = "z_p2g_asservi"

    cluster_1 = MagicMock()
    cluster_1.properties.group = "custom_group"
    matrix_mock = MagicMock()
    matrix_mock.shape = (8760, 3)
    cluster_1.get_series_matrix.return_value = matrix_mock

    mock_p2g_area.get_thermals.return_value = {"p2g_1": cluster_1}
    study.get_areas.return_value = {"z_p2g_asservi": mock_p2g_area}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"thermal": ["z_p2g_asservi"]},
        area_thermals={},
    )

    generate_scenario_builder(study, study_data, set())

    # nb_ts = 3 -> scenario for 5 years: [1, 2, 3, 1, 2]
    mock_thermal_cluster_1.set_new_scenario.assert_called_with([1, 2, 3, 1, 2])
    sb.thermal.get_cluster.assert_called_with("z_p2g_asservi", "p2g_1")


def test_generate_scenario_builder_thermal_z_p2g_asservi_clusters_fallback_default_1():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3

    sb = MagicMock()
    mock_thermal_cluster_1 = MagicMock()
    sb.thermal.get_cluster.return_value = mock_thermal_cluster_1
    study.get_scenario_builder.return_value = sb

    mock_p2g_area = MagicMock()
    mock_p2g_area.id = "z_p2g_asservi"
    mock_p2g_area.name = "z_p2g_asservi"

    cluster_1 = MagicMock()
    cluster_1.properties.group = "gas"
    cluster_1.get_series_matrix.return_value = None

    mock_p2g_area.get_thermals.return_value = {"p2g_1": cluster_1}
    study.get_areas.return_value = {"z_p2g_asservi": mock_p2g_area}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"Thermal": ["z_p2g_asservi"]},
        area_thermals={},
    )

    generate_scenario_builder(study, study_data, set())

    # nb_ts fallback = 1 -> scenario for 3 years: [1, 1, 1]
    mock_thermal_cluster_1.set_new_scenario.assert_called_with([1, 1, 1])


def test_generate_scenario_builder_thermal_z_p2g_asservi_area_not_found():
    study = MagicMock()
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb
    study.get_areas.return_value = {}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"Thermal": ["*@z_p2g_asservi"]},
        area_thermals={},
    )

    generate_scenario_builder(study, study_data, set())
    sb.thermal.get_cluster.assert_not_called()


def test_generate_scenario_builder_links_from_study_link():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 6
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
        scenario_builder_config={"Links": ["nl/z_p2h_pachybride"]},
    )

    generate_scenario_builder(study, study_data, set())

    sb.link.get_link.assert_called_with("nl / z_p2h_pachybride")
    mock_link_sb.set_new_scenario.assert_called_with([1, 2, 3, 4, 1, 2])
    study.set_scenario_builder.assert_called_with(sb)


@patch("antares.datamanager.generator.generate_scenario_builder.generate_link_capacity_df")
def test_generate_scenario_builder_links_from_study_data(mock_gen_cap):
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 4
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
    study.get_settings.return_value.general_parameters.nb_years = 3
    sb = MagicMock()
    mock_link_sb = MagicMock()
    sb.link.get_link.return_value = mock_link_sb
    study.get_scenario_builder.return_value = sb
    study.get_links.return_value = {}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"links": ["nl/z_p2h_pachybride"]},
        links={},
    )

    generate_scenario_builder(study, study_data, set())

    sb.link.get_link.assert_called_with("nl / z_p2h_pachybride")
    mock_link_sb.set_new_scenario.assert_called_with([1, 1, 1])


def test_generate_scenario_builder_links_reversed_order_and_casing():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3
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
        scenario_builder_config={"LINKS": ["FR / AT"]},
    )

    generate_scenario_builder(study, study_data, set())

    sb.link.get_link.assert_called_with("at / fr")
    mock_link_sb.set_new_scenario.assert_called_with([1, 2, 1])


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_sts_inflows_from_feather(mock_settings, mock_read_feather):
    mock_settings.sts_ts_directory = Path("/mock/sts_ts")

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 5
    sb = MagicMock()
    mock_storage_sb_1 = MagicMock()
    mock_storage_sb_2 = MagicMock()
    sb.storage_inflows.get_storage.side_effect = lambda area, storage: {
        ("area1", "psp_c"): mock_storage_sb_1,
        ("area2", "psp_o"): mock_storage_sb_2,
    }.get((area, storage), MagicMock())
    study.get_scenario_builder.return_value = sb

    mock_df_1 = MagicMock()
    mock_df_1.shape = (8760, 4)
    mock_df_2 = MagicMock()
    mock_df_2.shape = (8760, 2)
    mock_read_feather.side_effect = lambda path: mock_df_1 if "psp_c" in str(path) else mock_df_2

    # Area 1: has psp_closed cluster (matching) and battery cluster (non-matching)
    mock_storage_1 = MagicMock()
    mock_storage_1.id = "psp_c"
    mock_storage_1.properties.group = "psp_closed"

    mock_storage_bat = MagicMock()
    mock_storage_bat.id = "bat1"
    mock_storage_bat.properties.group = "battery"

    mock_area_1 = MagicMock()
    mock_area_1.id = "area1"
    mock_area_1.get_st_storages.return_value = {"psp_c": mock_storage_1, "bat1": mock_storage_bat}

    # Area 2: has psp_open cluster (matching)
    mock_storage_2 = MagicMock()
    mock_storage_2.id = "psp_o"
    mock_storage_2.properties.group = "psp_open"

    mock_area_2 = MagicMock()
    mock_area_2.id = "area2"
    mock_area_2.get_st_storages.return_value = {"psp_o": mock_storage_2}

    study.get_areas.return_value = {"area1": mock_area_1, "area2": mock_area_2}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"STS Inflows": ["psp_closed", "psp_open", "pondage"]},
        area_sts={
            "area1": {
                "psp_c": {"series": ["inflows.psp_c.arrow"]},
                "bat1": {"series": ["inflows.bat.arrow"]},
            },
            "area2": {
                "psp_o": {"series": ["inflows.psp_o.arrow"]},
            },
        },
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    # psp_c has 4 TS -> for 5 years: [1, 2, 3, 4, 1]
    mock_storage_sb_1.set_new_scenario.assert_called_once_with([1, 2, 3, 4, 1])
    # psp_o has 2 TS -> for 5 years: [1, 2, 1, 2, 1]
    mock_storage_sb_2.set_new_scenario.assert_called_once_with([1, 2, 1, 2, 1])

    # Ensure battery storage in area1 was never configured in sb.storage_inflows
    calls = sb.storage_inflows.get_storage.call_args_list
    configured_pairs = [(c[0][0], c[0][1]) for c in calls]
    assert ("area1", "bat1") not in configured_pairs


def test_generate_scenario_builder_sts_inflows_priority_to_study_matrix():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3
    sb = MagicMock()
    mock_storage_sb = MagicMock()
    sb.storage_inflows.get_storage.return_value = mock_storage_sb
    study.get_scenario_builder.return_value = sb

    mock_storage = MagicMock()
    mock_storage.id = "psp_open_1"
    mock_storage.properties.group = "psp_open"

    # Study matrix has 1 column
    mock_df_matrix = MagicMock()
    mock_df_matrix.shape = (8760, 1)
    mock_storage.get_storage_inflows.return_value = mock_df_matrix

    mock_area = MagicMock()
    mock_area.id = "fr"
    mock_area.get_st_storages.return_value = {"psp_open_1": mock_storage}
    study.get_areas.return_value = {"fr": mock_area}

    # StudyData has a series file with 4 columns
    mock_df_file = MagicMock()
    mock_df_file.shape = (8760, 4)

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"STS Inflows": ["psp_open@*"]},
        area_sts={
            "fr": {
                "psp_open_1": {"series": ["inflows.psp_open_1.feather"]},
            }
        },
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather", return_value=mock_df_file):
        with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
            generate_scenario_builder(study, study_data, set())

    # Priority to matrix in study (1 TS) -> [1, 1, 1], not from feather file (4 TS) -> [1, 2, 3]
    mock_storage_sb.set_new_scenario.assert_called_once_with([1, 1, 1])


def test_generate_scenario_builder_sts_inflows_from_matrix():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 4
    sb = MagicMock()
    mock_storage_sb = MagicMock()
    sb.storage_inflows.get_storage.return_value = mock_storage_sb
    study.get_scenario_builder.return_value = sb

    mock_storage = MagicMock()
    mock_storage.id = "pondage_1"
    mock_storage.properties.group = "pondage"

    mock_df = MagicMock()
    mock_df.shape = (8760, 3)
    mock_storage.get_storage_inflows.return_value = mock_df

    mock_area = MagicMock()
    mock_area.id = "area_fr"
    mock_area.get_st_storages.return_value = {"pondage_1": mock_storage}

    study.get_areas.return_value = {"area_fr": mock_area}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"sts_inflows": ["pondage"]},
        area_sts={},
    )

    generate_scenario_builder(study, study_data, set())

    sb.storage_inflows.get_storage.assert_called_with("area_fr", "pondage_1")
    # 3 TS -> 4 years: [1, 2, 3, 1]
    mock_storage_sb.set_new_scenario.assert_called_with([1, 2, 3, 1])


def test_generate_scenario_builder_sts_inflows_fallback_1():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3
    sb = MagicMock()
    mock_storage_sb = MagicMock()
    sb.storage_inflows.get_storage.return_value = mock_storage_sb
    study.get_scenario_builder.return_value = sb

    mock_storage = MagicMock()
    mock_storage.id = "psp_c"
    mock_storage.properties.group = "PSP_CLOSED"
    mock_storage.get_storage_inflows.side_effect = Exception("No matrix")

    mock_area = MagicMock()
    mock_area.id = "area1"
    mock_area.get_st_storages.return_value = {"psp_c": mock_storage}

    study.get_areas.return_value = {"area1": mock_area}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"STS Inflows": ["psp_closed"]},
        area_sts={},
    )

    generate_scenario_builder(study, study_data, set())

    sb.storage_inflows.get_storage.assert_called_with("area1", "psp_c")
    mock_storage_sb.set_new_scenario.assert_called_with([1, 1, 1])


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_with_wildcards_and_at_syntax(mock_settings, mock_read_feather):
    mock_settings.load_output_directory = Path("/tmp/load")
    mock_settings.hydro_ts_directory = Path("/tmp/hydro")
    mock_settings.res_ts_directory = Path("/tmp/res")
    mock_settings.nuclear_availability_ts_directory = Path("/tmp/nuclear_ts")
    mock_settings.nuclear_modulation_ts_directory = Path("/tmp/nuclear_modulation")
    mock_settings.sts_ts_directory = Path("/tmp/sts")

    df_climatic = MagicMock()
    df_climatic.shape = (8760, 4)
    df_nuc = MagicMock()
    df_nuc.shape = (8760, 2)
    df_sts = MagicMock()
    df_sts.shape = (8760, 3)

    def read_feather_side_effect(path):
        p_str = str(path)
        if "nuc" in p_str:
            return df_nuc
        elif "sts" in p_str or "inflows" in p_str:
            return df_sts
        return df_climatic

    mock_read_feather.side_effect = read_feather_side_effect

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 4
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    # Area FR
    area_fr = MagicMock()
    area_fr.id = "fr"
    area_fr.name = "FR"

    # Renewables in FR
    cluster_wind_on = MagicMock()
    cluster_wind_on.properties.group = "wind_onshore"
    cluster_wind_off = MagicMock()
    cluster_wind_off.properties.group = "wind_offshore"
    cluster_solar_pv = MagicMock()
    cluster_solar_pv.properties.group = "solar_pv"
    cluster_solar_th = MagicMock()
    cluster_solar_th.properties.group = "solar_thermo"

    area_fr.get_renewables.return_value = {
        "wind_on_1": cluster_wind_on,
        "wind_off_1": cluster_wind_off,
        "solar_pv_1": cluster_solar_pv,
        "solar_th_1": cluster_solar_th,
    }

    # Thermals in FR
    cluster_nuc_fr = MagicMock()
    cluster_nuc_fr.properties.group = "nuclear"
    area_fr.get_thermals.return_value = {"nuc_fr_1": cluster_nuc_fr}

    # STS in FR
    storage_psp_c = MagicMock()
    storage_psp_c.id = "psp_c"
    storage_psp_c.properties.group = "psp_closed"
    storage_psp_o = MagicMock()
    storage_psp_o.id = "psp_o"
    storage_psp_o.properties.group = "psp_open"
    storage_pondage = MagicMock()
    storage_pondage.id = "pondage_1"
    storage_pondage.properties.group = "pondage"

    area_fr.get_st_storages.return_value = {
        "psp_c": storage_psp_c,
        "psp_o": storage_psp_o,
        "pondage_1": storage_pondage,
    }

    # Area y_nuc_modulation
    area_y_nuc = MagicMock()
    area_y_nuc.id = "y_nuc_modulation"
    area_y_nuc.name = "y_nuc_modulation"
    cluster_nuc_y = MagicMock()
    cluster_nuc_y.properties.group = "nuclear"
    area_y_nuc.get_thermals.return_value = {"nuc_y_1": cluster_nuc_y}
    area_y_nuc.get_renewables.return_value = {}
    area_y_nuc.get_st_storages.return_value = {}

    # Area AT
    area_at = MagicMock()
    area_at.id = "at"
    area_at.name = "AT"
    storage_psp_at = MagicMock()
    storage_psp_at.id = "psp_at"
    storage_psp_at.properties.group = "psp"
    mock_c_ve = MagicMock()
    mock_c_ve.id = "ve"
    mock_c_ve.name = "VE"
    storage_psp_at.get_constraints.return_value = {"ve": mock_c_ve}
    area_at.get_st_storages.return_value = {"psp_at": storage_psp_at}
    area_at.get_thermals.return_value = {}
    area_at.get_renewables.return_value = {}

    study.get_areas.return_value = {
        "fr": area_fr,
        "y_nuc_modulation": area_y_nuc,
        "at": area_at,
    }

    # Links
    mock_link = MagicMock()
    mock_link.get_capacity_direct.return_value = df_climatic
    study.get_links.return_value = {"nl / z_p2h_pachybride": mock_link}

    # ScenarioBuilder mocks
    mock_load_area = MagicMock()
    mock_hydro_area = MagicMock()
    mock_ren_cluster = MagicMock()
    mock_thermal_cluster = MagicMock()
    mock_link_sb = MagicMock()
    mock_sts_storage = MagicMock()
    mock_sts_constraint = MagicMock()

    sb.load.get_area.return_value = mock_load_area
    sb.hydro.get_area.return_value = mock_hydro_area
    sb.renewable.get_cluster.return_value = mock_ren_cluster
    sb.thermal.get_cluster.return_value = mock_thermal_cluster
    sb.link.get_link.return_value = mock_link_sb
    sb.storage_inflows.get_storage.return_value = mock_sts_storage
    sb.storage_constraints.get_constraint.return_value = mock_sts_constraint

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={
            "Climatic data": [
                "load",
                "hydro",
                "wind_onshore@*",
                "wind_offshore@*",
                "solar_pv@*",
                "solar_thermo@*",
            ],
            "Thermal": ["nuclear@fr", "*@z_p2g_asservi", "nuclear@y_nuc_modulation"],
            "Links": ["nl/z_p2h_pachybride"],
            "STS Inflows": ["psp_closed@*", "psp_open@*", "pondage@*"],
            "STS Constraints": ["AT@PSP@VE"],
        },
        area_loads={"FR": ["load_fr.arrow"]},
        area_hydro={"FR": {"series": ["hydro_fr.arrow"]}},
        area_res={
            "FR": {
                "wind_onshore": {"series": ["wind_on.arrow"]},
                "wind_offshore": {"series": ["wind_off.arrow"]},
                "solar_pv": {"series": ["solar_pv.arrow"]},
                "solar_thermo": {"series": ["solar_th.arrow"]},
            }
        },
        area_nuclear={
            "FR": {"clusters": {"nuc_fr_1": {"series": "nuc_fr.arrow"}}},
            "y_nuc_modulation": {"clusters": {"nuc_y_1": {"series": "nuc_y.arrow"}}},
        },
        area_sts={
            "FR": {
                "psp_c": {"series": ["inflows.psp_c.arrow"]},
                "psp_o": {"series": ["inflows.psp_o.arrow"]},
                "pondage_1": {"series": ["inflows.pondage.arrow"]},
            },
            "AT": {
                "psp_at": {
                    "constraintParameters": {"VE": {}},
                    "stsConstraintsSeriesList": ["ve.csv.uuid.arrow"],
                }
            },
        },
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    # Verify Climatic Data: nb_ts = 4 -> [1, 2, 3, 4]
    mock_load_area.set_new_scenario.assert_called_with([1, 2, 3, 4])
    mock_hydro_area.set_new_scenario.assert_called_with([1, 2, 3, 4])
    assert mock_ren_cluster.set_new_scenario.call_count == 4
    mock_ren_cluster.set_new_scenario.assert_called_with([1, 2, 3, 4])

    # Verify Thermal: nb_ts = 2 -> [1, 2, 1, 2]
    assert mock_thermal_cluster.set_new_scenario.call_count == 2
    mock_thermal_cluster.set_new_scenario.assert_called_with([1, 2, 1, 2])

    # Verify Links: nb_ts = 4 -> [1, 2, 3, 4]
    mock_link_sb.set_new_scenario.assert_called_with([1, 2, 3, 4])

    # Verify STS Inflows: nb_ts = 3 -> [1, 2, 3, 1]
    assert mock_sts_storage.set_new_scenario.call_count == 3
    mock_sts_storage.set_new_scenario.assert_called_with([1, 2, 3, 1])

    # Verify STS Constraints: nb_ts = 3 -> [1, 2, 3, 1]
    sb.storage_constraints.get_constraint.assert_called_with("at", "psp_at", "VE")
    mock_sts_constraint.set_new_scenario.assert_called_with([1, 2, 3, 1])


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_sts_constraints_from_feather(mock_settings, mock_read_feather):
    mock_settings.sts_ts_directory = Path("/tmp/sts")

    df_sts_constraint = MagicMock()
    df_sts_constraint.shape = (8760, 3)
    mock_read_feather.return_value = df_sts_constraint

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 5
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    # Setup Area AT
    area_at = MagicMock()
    area_at.id = "at"
    area_at.name = "AT"

    storage_psp = MagicMock()
    storage_psp.id = "psp_storage_1"
    storage_psp.properties.group = "psp"
    mock_c_ve = MagicMock()
    mock_c_ve.id = "VE"
    mock_c_ve.name = "VE"
    storage_psp.get_constraints.return_value = {"VE": mock_c_ve}

    area_at.get_st_storages.return_value = {"psp_storage_1": storage_psp}
    study.get_areas.return_value = {"at": area_at}

    mock_constraint_matrix = MagicMock()
    sb.storage_constraints.get_constraint.return_value = mock_constraint_matrix

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"STS Constraints": ["AT@PSP@VE"]},
        area_sts={
            "AT": {
                "psp_storage_1": {
                    "constraintParameters": {
                        "VE": {
                            "variable": "injection",
                            "operator": "greater",
                        }
                    },
                    "stsConstraintsSeriesList": ["ve.csv.uuid123.arrow"],
                }
            }
        },
    )

    with patch("antares.datamanager.generator.generate_scenario_builder.Path.exists", return_value=True):
        generate_scenario_builder(study, study_data, set())

    sb.storage_constraints.get_constraint.assert_called_with("at", "psp_storage_1", "VE")
    # nb_ts = 3 -> repeat up to 5 years: [1, 2, 3, 1, 2]
    mock_constraint_matrix.set_new_scenario.assert_called_with([1, 2, 3, 1, 2])


@patch("antares.datamanager.generator.generate_scenario_builder.pd.read_feather")
@patch("antares.datamanager.generator.generate_scenario_builder.settings")
def test_generate_scenario_builder_sts_constraints_from_matrix(mock_settings, mock_read_feather):
    mock_settings.sts_ts_directory = Path("/tmp/sts")

    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    area_at = MagicMock()
    area_at.id = "at"
    area_at.name = "AT"

    storage_psp = MagicMock()
    storage_psp.id = "psp_storage_1"
    storage_psp.properties.group = "psp_closed"
    mock_c_ve = MagicMock()
    mock_c_ve.id = "psp"
    mock_c_ve.name = "psp"
    storage_psp.get_constraints.return_value = {"psp": mock_c_ve}

    mock_term_matrix = MagicMock()
    mock_term_matrix.shape = (8760, 2)
    storage_psp.get_constraint_term.return_value = mock_term_matrix

    area_at.get_st_storages.return_value = {"psp_storage_1": storage_psp}
    study.get_areas.return_value = {"at": area_at}

    mock_constraint_matrix = MagicMock()
    sb.storage_constraints.get_constraint.return_value = mock_constraint_matrix

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"STS Constraints": ["AT@PSP@psp"]},
        area_sts={
            "AT": {
                "psp_storage_1": {
                    "constraintParameters": {"psp": {}},
                    "stsConstraintsSeriesList": [],
                }
            }
        },
    )

    generate_scenario_builder(study, study_data, set())

    sb.storage_constraints.get_constraint.assert_called_with("at", "psp_storage_1", "psp")
    # nb_ts = 2 -> [1, 2, 1]
    mock_constraint_matrix.set_new_scenario.assert_called_with([1, 2, 1])


def test_generate_scenario_builder_sts_constraints_fallback_default_1():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    area_at = MagicMock()
    area_at.id = "at"
    area_at.name = "AT"

    storage_psp = MagicMock()
    storage_psp.id = "psp_storage_1"
    storage_psp.properties.group = "psp"
    mock_c_ve = MagicMock()
    mock_c_ve.id = "ve"
    mock_c_ve.name = "ve"
    storage_psp.get_constraints.return_value = {"ve": mock_c_ve}
    storage_psp.get_constraint_term.return_value = None

    area_at.get_st_storages.return_value = {"psp_storage_1": storage_psp}
    study.get_areas.return_value = {"at": area_at}

    mock_constraint_matrix = MagicMock()
    sb.storage_constraints.get_constraint.return_value = mock_constraint_matrix

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"STS Constraints": ["AT@PSP@ve"]},
        area_sts={},
    )

    generate_scenario_builder(study, study_data, set())

    sb.storage_constraints.get_constraint.assert_called_with("at", "psp_storage_1", "ve")
    mock_constraint_matrix.set_new_scenario.assert_called_with([1, 1, 1])


def test_generate_scenario_builder_sts_constraints_filters_only_targeted_constraint():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    # Area FR with pondage_2h cluster containing multiple constraints
    area_fr = MagicMock()
    area_fr.id = "fr"
    area_fr.name = "FR"

    storage_pondage = MagicMock()
    storage_pondage.id = "pondage_2h_fr"
    storage_pondage.properties.group = "pondage_2h"

    mock_c1 = MagicMock()
    mock_c1.id = "v2g_limit_fr"
    mock_c1.name = "v2g_limit_fr"

    mock_c2 = MagicMock()
    mock_c2.id = "other_constraint"
    mock_c2.name = "other_constraint"

    storage_pondage.get_constraints.return_value = {
        "v2g_limit_fr": mock_c1,
        "other_constraint": mock_c2,
    }
    storage_pondage.get_constraint_term.return_value = None

    area_fr.get_st_storages.return_value = {"pondage_2h_fr": storage_pondage}
    study.get_areas.return_value = {"fr": area_fr}

    mock_constraint_matrix = MagicMock()
    sb.storage_constraints.get_constraint.return_value = mock_constraint_matrix

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"STS Constraints": ["FR@PONDAGE_2h@v2g_limit_fr"]},
        area_sts={},
    )

    generate_scenario_builder(study, study_data, set())

    # Only v2g_limit_fr should be configured, NOT other_constraint
    assert sb.storage_constraints.get_constraint.call_count == 1
    sb.storage_constraints.get_constraint.assert_called_once_with("fr", "pondage_2h_fr", "v2g_limit_fr")
    mock_constraint_matrix.set_new_scenario.assert_called_once_with([1, 1, 1])


def test_generate_scenario_builder_sts_constraints_filters_only_targeted_cluster():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    # Area FR with pondage_2h and pondage_4h clusters both having group="pondage" and constraint "v2g_limit_fr"
    area_fr = MagicMock()
    area_fr.id = "fr"
    area_fr.name = "FR"

    storage_pondage_2h = MagicMock()
    storage_pondage_2h.id = "fr_pondage_2h"
    storage_pondage_2h.name = "fr_pondage_2h"
    storage_pondage_2h.properties.group = "pondage"

    storage_pondage_4h = MagicMock()
    storage_pondage_4h.id = "fr_pondage_4h"
    storage_pondage_4h.name = "fr_pondage_4h"
    storage_pondage_4h.properties.group = "pondage"

    mock_c1 = MagicMock()
    mock_c1.id = "v2g_limit_fr"
    mock_c1.name = "v2g_limit_fr"

    mock_c2 = MagicMock()
    mock_c2.id = "v2g_limit_fr"
    mock_c2.name = "v2g_limit_fr"

    storage_pondage_2h.get_constraints.return_value = {"v2g_limit_fr": mock_c1}
    storage_pondage_2h.get_constraint_term.return_value = None

    storage_pondage_4h.get_constraints.return_value = {"v2g_limit_fr": mock_c2}
    storage_pondage_4h.get_constraint_term.return_value = None

    area_fr.get_st_storages.return_value = {
        "fr_pondage_2h": storage_pondage_2h,
        "fr_pondage_4h": storage_pondage_4h,
    }
    study.get_areas.return_value = {"fr": area_fr}

    mock_constraint_matrix = MagicMock()
    sb.storage_constraints.get_constraint.return_value = mock_constraint_matrix

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"STS Constraints": ["FR@PONDAGE_2h@v2g_limit_fr"]},
        area_sts={},
    )

    generate_scenario_builder(study, study_data, set())

    # Only fr_pondage_2h should be configured, NOT fr_pondage_4h
    assert sb.storage_constraints.get_constraint.call_count == 1
    sb.storage_constraints.get_constraint.assert_called_once_with("fr", "fr_pondage_2h", "v2g_limit_fr")
    mock_constraint_matrix.set_new_scenario.assert_called_once_with([1, 1, 1])


def test_generate_scenario_builder_sts_inflows_psp_open_closed_and_pondage():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    mock_sb_psp_c = MagicMock()
    mock_sb_psp_o = MagicMock()
    mock_sb_pondage = MagicMock()
    mock_sb_bat = MagicMock()

    sb.storage_inflows.get_storage.side_effect = lambda area, storage: {
        ("fr", "fr_psp_closed"): mock_sb_psp_c,
        ("fr", "fr_psp_open"): mock_sb_psp_o,
        ("fr", "fr_pondage"): mock_sb_pondage,
        ("fr", "fr_battery"): mock_sb_bat,
    }.get((area, storage), MagicMock())

    area_fr = MagicMock()
    area_fr.id = "fr"
    area_fr.name = "FR"

    # In Antares/RTE datasets, PSP clusters often have properties.group = "psp" or "PSP"
    storage_psp_c = MagicMock()
    storage_psp_c.id = "fr_psp_closed"
    storage_psp_c.name = "fr_psp_closed"
    storage_psp_c.properties.group = "psp"

    storage_psp_o = MagicMock()
    storage_psp_o.id = "fr_psp_open"
    storage_psp_o.name = "fr_psp_open"
    storage_psp_o.properties.group = "psp"

    storage_pondage = MagicMock()
    storage_pondage.id = "fr_pondage"
    storage_pondage.name = "fr_pondage"
    storage_pondage.properties.group = "pondage"

    storage_bat = MagicMock()
    storage_bat.id = "fr_battery"
    storage_bat.name = "fr_battery"
    storage_bat.properties.group = "battery"

    area_fr.get_st_storages.return_value = {
        "fr_psp_closed": storage_psp_c,
        "fr_psp_open": storage_psp_o,
        "fr_pondage": storage_pondage,
        "fr_battery": storage_bat,
    }
    study.get_areas.return_value = {"fr": area_fr}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"STS Inflows": ["psp_closed@*", "psp_open@*", "pondage@*"]},
        area_sts={},
    )

    generate_scenario_builder(study, study_data, set())

    # psp_closed, psp_open, and pondage should all be configured
    mock_sb_psp_c.set_new_scenario.assert_called_once_with([1, 1, 1])
    mock_sb_psp_o.set_new_scenario.assert_called_once_with([1, 1, 1])
    mock_sb_pondage.set_new_scenario.assert_called_once_with([1, 1, 1])

    # battery should not be configured
    configured_storages = [c[0][1] for c in sb.storage_inflows.get_storage.call_args_list]
    assert "fr_battery" not in configured_storages
    assert "fr_psp_closed" in configured_storages
    assert "fr_psp_open" in configured_storages
    assert "fr_pondage" in configured_storages


def test_generate_scenario_builder_sts_inflows_zone_specific():
    study = MagicMock()
    study.get_settings.return_value.general_parameters.nb_years = 3
    sb = MagicMock()
    study.get_scenario_builder.return_value = sb

    mock_sb_fr = MagicMock()
    mock_sb_be = MagicMock()

    sb.storage_inflows.get_storage.side_effect = lambda area, storage: {
        ("fr", "fr_psp_closed"): mock_sb_fr,
        ("be", "be_psp_closed"): mock_sb_be,
    }.get((area, storage), MagicMock())

    area_fr = MagicMock()
    area_fr.id = "fr"
    area_fr.name = "FR"
    storage_fr = MagicMock()
    storage_fr.id = "fr_psp_closed"
    storage_fr.name = "fr_psp_closed"
    storage_fr.properties.group = "psp_closed"
    area_fr.get_st_storages.return_value = {"fr_psp_closed": storage_fr}

    area_be = MagicMock()
    area_be.id = "be"
    area_be.name = "BE"
    storage_be = MagicMock()
    storage_be.id = "be_psp_closed"
    storage_be.name = "be_psp_closed"
    storage_be.properties.group = "psp_closed"
    area_be.get_st_storages.return_value = {"be_psp_closed": storage_be}

    study.get_areas.return_value = {"fr": area_fr, "be": area_be}

    study_data = StudyData(
        name="test_study",
        scenario_builder_config={"STS Inflows": ["psp_closed@fr"]},
        area_sts={},
    )

    generate_scenario_builder(study, study_data, set())

    # Only fr_psp_closed should be configured, not be_psp_closed
    mock_sb_fr.set_new_scenario.assert_called_once_with([1, 1, 1])
    mock_sb_be.set_new_scenario.assert_not_called()
