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

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from antares.craft import (
    BindingConstraintFrequency,
    BindingConstraintOperator,
    Month,
)
from antares.craft.model.area import Area
from antares.craft.model.study import Study
from antares.datamanager.generator.generate_dsr_clusters import (
    _create_dsr_binding_constraints,
    _build_dsr_constraint_names,
    create_dsr_cluster,
    create_dsr_modulation_matrix_from_series,
    create_dsr_prepro_data_matrix,
    generate_dsr_binding_constraints,
    generate_dsr_clusters,
)


def test_create_dsr_modulation_matrix_from_series_empty_returns_default_df():
    df = create_dsr_modulation_matrix_from_series(None)
    assert isinstance(df, pd.DataFrame)
    # Expect 8760 rows and 4 columns with default values [1,1,1,0]
    assert df.shape == (8760, 4)
    assert df.iloc[0].tolist() == [1, 1, 1, 0]
    assert df.iloc[-1].tolist() == [1, 1, 1, 0]


def test_create_dsr_modulation_matrix_from_series_builds_dataframe():
    # Arrange
    series = pd.Series([10, 20])

    # Act
    df = create_dsr_modulation_matrix_from_series(series)

    # Assert
    assert df.shape == (2, 4)
    assert df.iloc[0].tolist() == [1, 1, 10, 0.0]
    assert df.iloc[1].tolist() == [1, 1, 20, 0.0]


@patch("antares.datamanager.generator.generate_dsr_clusters.generate_dsr_binding_constraints")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_modulation_matrix_from_series")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_cluster")
@patch("antares.datamanager.generator.generate_dsr_clusters.pd.read_feather")
@patch("antares.datamanager.generator.generate_dsr_clusters.Path.exists")
def test_generate_dsr_clusters_calls_area_methods(
    mock_exists,
    mock_read_feather,
    mock_create_dsr_cluster,
    mock_create_modulation,
    mock_generate_constraints,
):
    # Arrange
    mock_exists.return_value = True

    study = MagicMock(spec=Study)
    area_obj = MagicMock(spec=Area)
    area_obj.name = "be"

    dsr_data = {
        "dsr_1": {
            "properties": {"unit_count": 10},
            "data": {"some": "data"},
            "modulation": ["CM_file.arrow"],
        }
    }

    mock_series = pd.Series([10, 20])
    mock_read_feather.return_value = pd.DataFrame({"val": mock_series})

    mock_modulation = pd.DataFrame([[1, 1, 0.5, 0]] * 8760)
    mock_create_modulation.return_value = mock_modulation

    used_files: set[Path] = set()

    # Act
    generate_dsr_clusters(study, area_obj, dsr_data, used_files=used_files)

    # Assert
    mock_read_feather.assert_called_once()
    mock_create_modulation.assert_called_once()
    mock_create_dsr_cluster.assert_called_once_with(
        area_obj,
        "dsr_1",
        dsr_data["dsr_1"],
        mock_modulation,
        None,
    )
    mock_generate_constraints.assert_called_once()
    call_args = mock_generate_constraints.call_args.args
    assert call_args[0] == study
    assert call_args[1] == "be"
    assert call_args[2] == "dsr_1"
    assert call_args[3] == dsr_data
    pd.testing.assert_series_equal(call_args[4], mock_series, check_names=False)
    assert len(used_files) == 1


@patch("antares.datamanager.generator.generate_dsr_clusters.generate_dsr_binding_constraints")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_modulation_matrix_from_series")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_cluster")
def test_generate_dsr_clusters_with_empty_modulation(
    mock_create_dsr_cluster,
    mock_create_modulation,
    mock_generate_constraints,
):
    # Arrange
    study = MagicMock(spec=Study)
    area_obj = MagicMock(spec=Area)
    area_obj.name = "be"

    dsr_data = {
        "dsr_1": {
            "modulation": [],
        }
    }

    # Act
    generate_dsr_clusters(study, area_obj, dsr_data)

    # Assert
    mock_create_modulation.assert_called_once()

    series = mock_create_modulation.call_args.args[0]

    assert isinstance(series, pd.Series)
    assert len(series) == 8760
    assert np.all(series == 1)

    mock_create_dsr_cluster.assert_called_once()
    mock_generate_constraints.assert_called_once()


@patch("antares.datamanager.generator.generate_dsr_clusters.logger")
@patch("antares.datamanager.generator.generate_dsr_clusters.generate_dsr_binding_constraints")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_modulation_matrix_from_series")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_cluster")
@patch("antares.datamanager.generator.generate_dsr_clusters.Path.exists")
def test_generate_dsr_clusters_logs_warning_when_file_not_found(
    mock_exists,
    mock_create_dsr_cluster,
    mock_create_modulation,
    mock_generate_constraints,
    mock_logger,
):
    # Arrange
    mock_exists.return_value = False

    study = MagicMock(spec=Study)
    area_obj = MagicMock(spec=Area)
    area_obj.name = "be"

    dsr_data = {
        "dsr_1": {
            "modulation": ["CM_missing.arrow"],
        }
    }

    # Act
    generate_dsr_clusters(study, area_obj, dsr_data)

    # Assert
    mock_logger.warning.assert_called_once()
    mock_create_dsr_cluster.assert_called_once()
    mock_generate_constraints.assert_called_once()


@pytest.mark.parametrize(
    "area_name, cluster_name, expected_bc_name, expected_bc_term_name",
    [
        ("be", "area_dsr_1", "dsr1_be_stock", "be_dsr_1"),
        ("fr", "dsr_DSR_peak", "DSRpeak_fr_stock", "fr_DSR_peak"),
        ("de", "zone_dsr", "dsr_de_stock", "de_dsr"),
    ],
)
def test_build_dsr_constraint_names(area_name, cluster_name, expected_bc_name, expected_bc_term_name):
    bc_name, bc_term_name = _build_dsr_constraint_names(area_name, cluster_name)
    assert bc_name == expected_bc_name
    assert bc_term_name == expected_bc_term_name


def test_create_dsr_binding_constraints_with_empty_matrix():
    study = MagicMock(spec=Study)
    _create_dsr_binding_constraints(study, "be", "area_dsr_1", pd.DataFrame())
    study.create_binding_constraint.assert_not_called()


def test_create_dsr_binding_constraints_creates_constraint():
    study = MagicMock(spec=Study)
    df = pd.DataFrame({"area_dsr_1": [10.0] * 366})

    _create_dsr_binding_constraints(study, "be", "area_dsr_1", df)

    study.create_binding_constraint.assert_called_once()
    call_kwargs = study.create_binding_constraint.call_args.kwargs

    assert call_kwargs["name"] == "dsr1_be_stock"
    assert call_kwargs["properties"].enabled is True
    assert call_kwargs["properties"].time_step == BindingConstraintFrequency.DAILY
    assert call_kwargs["properties"].operator == BindingConstraintOperator.LESS

    assert len(call_kwargs["terms"]) == 1
    term = call_kwargs["terms"][0]
    assert term.data.area == "be"
    assert term.data.cluster == "be_dsr_1"
    assert term.weight == 1
    assert term.offset == 0

    pd.testing.assert_frame_equal(call_kwargs["less_term_matrix"], df)


@patch("antares.datamanager.generator.generate_dsr_clusters._create_dsr_binding_constraints")
def test_generate_dsr_binding_constraints_with_binding_constraint_true_and_series(mock_create_bc):
    study = MagicMock(spec=Study)
    dsr_data = {
        "cluster_1": {
            "data": {
                "max_hour_per_day": 2,
                "nb_hour_per_day": 4,
                "capacity": 10,
                "binding_constraint": True,
            }
        }
    }
    # 8760 hours -> 365 days of 24 hours
    # Each day mean is 1.5
    cluster_series_data = pd.Series([1.5] * 8760)

    generate_dsr_binding_constraints(study, "be", "cluster_1", dsr_data, cluster_series_data)

    mock_create_bc.assert_called_once()
    args = mock_create_bc.call_args.args
    assert args[0] == study
    assert args[1] == "be"
    assert args[2] == "cluster_1"

    matrix = args[3]
    # coeff = 24 * 2 / 4 = 12
    # volume_no_modulation = 10 * 12 = 120
    # daily_mean = 1.5
    # less_term_matrix = 120 * 1.5 = 180 for 365 days, and 0 for day 366
    assert len(matrix) == 366
    assert matrix.columns.tolist() == ["cluster_1"]
    assert np.all(matrix.iloc[:365]["cluster_1"] == 180.0)
    assert matrix.iloc[365]["cluster_1"] == 0.0


@patch("antares.datamanager.generator.generate_dsr_clusters._create_dsr_binding_constraints")
def test_generate_dsr_binding_constraints_without_binding_constraint(mock_create_bc):
    study = MagicMock(spec=Study)
    dsr_data = {
        "cluster_1": {
            "data": {
                "max_hour_per_day": 1,
                "nb_hour_per_day": 2,
                "capacity": 5,
                "binding_constraint": False,
            }
        }
    }
    cluster_series_data = pd.Series([2.0] * 8760)

    generate_dsr_binding_constraints(study, "be", "cluster_1", dsr_data, cluster_series_data)

    mock_create_bc.assert_called_once()
    matrix = mock_create_bc.call_args.args[3]
    # coeff = 24 * 1 / 2 = 12
    # volume_no_modulation = 5 * 12 = 60
    assert len(matrix) == 366
    assert matrix.columns.tolist() == ["cluster_1"]
    assert np.all(matrix.iloc[:365]["cluster_1"] == 60)
    assert matrix.iloc[365]["cluster_1"] == 0


@patch("antares.datamanager.generator.generate_dsr_clusters._create_dsr_binding_constraints")
def test_generate_dsr_binding_constraints_with_binding_constraint_true_but_no_series(mock_create_bc):
    study = MagicMock(spec=Study)
    dsr_data = {
        "cluster_1": {
            "data": {
                "max_hour_per_day": 1,
                "nb_hour_per_day": 1,
                "capacity": 3,
                "binding_constraint": True,
            }
        }
    }

    generate_dsr_binding_constraints(study, "be", "cluster_1", dsr_data, None)

    mock_create_bc.assert_called_once()
    matrix = mock_create_bc.call_args.args[3]
    # coeff = 24 * 1 / 1 = 24
    # volume_no_modulation = 3 * 24 = 72
    assert len(matrix) == 366
    assert np.all(matrix.iloc[:365]["cluster_1"] == 72)
    assert matrix.iloc[365]["cluster_1"] == 0


def test_create_dsr_cluster():
    area_obj = MagicMock(spec=Area)
    thermal_cluster_mock = MagicMock()
    area_obj.create_thermal_cluster.return_value = thermal_cluster_mock

    cluster_values = {
        "properties": {"unit_count": 5},
        "data": {
            "fo_duration": 2,
            "fo_monthly_rate": [0.1] * 12,
        },
    }
    modulation_matrix = pd.DataFrame([[1, 1, 1, 0]] * 8760)

    create_dsr_cluster(area_obj, "cluster_1", cluster_values, modulation_matrix, first_month=Month.JANUARY)

    area_obj.create_thermal_cluster.assert_called_once()
    thermal_cluster_mock.set_prepro_data.assert_called_once()
    thermal_cluster_mock.set_prepro_modulation.assert_called_once_with(modulation_matrix)


def test_create_dsr_prepro_data_matrix_empty_data():
    df = create_dsr_prepro_data_matrix({})
    assert df.shape == (365, 6)
    assert df.iloc[0].tolist() == [1, 1, 0, 0, 0, 0]


def test_create_dsr_prepro_data_matrix_with_data():
    data = {
        "fo_duration": 3,
        "fo_monthly_rate": [0.05] * 12,
    }
    df = create_dsr_prepro_data_matrix(data, first_month=Month.JANUARY)
    assert df.shape == (365, 6)
    # Column 0: fo_duration (3), Column 1: po_duration (1), Column 2: fo_rate_daily (0.05)
    assert (df[0] == 3).all()
    assert (df[1] == 1).all()
    assert (df[2] == 0.05).all()


def test_create_dsr_prepro_data_matrix_invalid_monthly_rate():
    data = {
        "fo_duration": 3,
        "fo_monthly_rate": [0.05] * 10,  # not 12
    }
    with pytest.raises(ValueError, match="fo_monthly_rate must have 12 values"):
        create_dsr_prepro_data_matrix(data)
