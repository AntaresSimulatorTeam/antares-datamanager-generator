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

from unittest.mock import MagicMock, patch

import pandas as pd

from antares.craft.model.area import Area
from antares.datamanager.generator.generate_dsr_clusters import (
    create_dsr_modulation_matrix_from_series,
    generate_dsr_clusters,
)


def test_create_dsr_modulation_matrix_from_series_empty_returns_default_df():
    df = create_dsr_modulation_matrix_from_series(None, 0)
    assert isinstance(df, pd.DataFrame)
    # Expect 8760 rows and 4 columns with default values [1,1,1,0]
    assert df.shape == (8760, 4)
    assert df.iloc[0].tolist() == [1, 1, 1, 0]
    assert df.iloc[-1].tolist() == [1, 1, 1, 0]


def test_create_dsr_modulation_matrix_from_series_builds_dataframe():
    # Arrange
    series = pd.Series([10, 20])
    global_max = 40

    # Act
    df = create_dsr_modulation_matrix_from_series(series, global_max)

    # Assert
    assert df.shape == (2, 4)
    # Expect [[1, 1, value/global_max, 0], ...]
    # 10/40 = 0.25, 20/40 = 0.5
    assert df.iloc[0].tolist() == [1, 1, 0.25, 0.0]
    assert df.iloc[1].tolist() == [1, 1, 0.5, 0.0]


@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_modulation_matrix_from_series")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_cluster")
@patch("antares.datamanager.generator.generate_dsr_clusters.pd.read_feather")
@patch("antares.datamanager.generator.generate_dsr_clusters.Path.exists")
def test_generate_dsr_clusters_calls_area_methods(
    mock_exists, mock_read_feather, mock_create_dsr_cluster, mock_create_modulation
):
    # Arrange
    mock_exists.return_value = True
    area_obj = MagicMock(spec=Area)
    cluster_name = "dsr_1"
    dsr_data = {
        cluster_name: {"properties": {"unit_count": 10}, "data": {"some": "data"}, "modulation": ["CM_file.arrow"]}
    }

    mock_modulation = pd.DataFrame([[1, 1, 0.5, 0]] * 8760)

    mock_read_feather.return_value = pd.DataFrame({"val": [10, 20]})
    mock_create_modulation.return_value = mock_modulation

    # Act
    generate_dsr_clusters(area_obj, dsr_data)

    # Assert
    mock_create_dsr_cluster.assert_called_once()


@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_modulation_matrix_from_series")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_cluster")
@patch("antares.datamanager.generator.generate_dsr_clusters.pd.read_feather")
@patch("antares.datamanager.generator.generate_dsr_clusters.Path.exists")
def test_generate_dsr_clusters_calculates_global_max_with_multiple_clusters(
    mock_exists, mock_read_feather, mock_create_dsr_cluster, mock_create_modulation
):
    # Arrange
    mock_exists.return_value = True
    area_obj = MagicMock(spec=Area)

    # Series 1: [10, 20], Series 2: [30, 10]
    # Sum: [40, 30], Global Max should be 40
    series1 = pd.Series([10, 20], name=0)
    series2 = pd.Series([30, 10], name=0)

    def side_effect(path):
        if "CM_file1" in str(path):
            return pd.DataFrame({0: series1})
        if "CM_file2" in str(path):
            return pd.DataFrame({0: series2})
        return pd.DataFrame()

    mock_read_feather.side_effect = side_effect

    dsr_data = {
        "dsr_1": {"properties": {}, "data": {}, "modulation": ["CM_file1.arrow"]},
        "dsr_2": {"properties": {}, "data": {}, "modulation": ["CM_file2.arrow"]},
    }

    # Act
    generate_dsr_clusters(area_obj, dsr_data)

    # Assert
    # global_max should be max([10+30, 20+10]) = 40
    expected_global_max = 40.0

    # Verify that create_dsr_modulation_matrix_from_series was called for each cluster with the global_max
    assert mock_create_modulation.call_count == 2

    # Check first call
    args1, kwargs1 = mock_create_modulation.call_args_list[0]
    # args1[0] is series, args1[1] is global_max
    pd.testing.assert_series_equal(args1[0], series1)
    assert args1[1] == expected_global_max

    # Check second call
    args2, kwargs2 = mock_create_modulation.call_args_list[1]
    pd.testing.assert_series_equal(args2[0], series2)
    assert args2[1] == expected_global_max


@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_modulation_matrix_from_series")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_cluster")
@patch("antares.datamanager.generator.generate_dsr_clusters.pd.read_feather")
@patch("antares.datamanager.generator.generate_dsr_clusters.Path.exists")
def test_generate_dsr_clusters_calculates_global_max_with_user_example_data(
    mock_exists, mock_read_feather, mock_create_dsr_cluster, mock_create_modulation
):
    # Test case inspired by user example:
    # date              FR_DSR_0_industrie  FR_DSR_0_tertiaire  FR_DSR_0_implicite
    # 01/07/2028 07:00  0.871431371268047   1                   1
    # Sum at 07:00 = 2.871431371268047

    # Arrange
    mock_exists.return_value = True
    area_obj = MagicMock(spec=Area)

    # We simulate 3 clusters, each with its own CM file containing one column of data
    val_industrie = 0.871431371268047
    val_tertiaire = 1.0
    val_implicite = 1.0

    series_industrie = pd.Series([0.89867, val_industrie], name=0)
    series_tertiaire = pd.Series([0.0, val_tertiaire], name=0)
    series_implicite = pd.Series([0.0, val_implicite], name=0)

    # Sum at index 0: 0.89867 + 0 + 0 = 0.89867
    # Sum at index 1: 0.87143 + 1 + 1 = 2.87143
    # Global Max should be 2.871431371268047

    def side_effect(path):
        if "industrie" in str(path):
            return pd.DataFrame({0: series_industrie})
        if "tertiaire" in str(path):
            return pd.DataFrame({0: series_tertiaire})
        if "implicite" in str(path):
            return pd.DataFrame({0: series_implicite})
        return pd.DataFrame()

    mock_read_feather.side_effect = side_effect

    dsr_data = {
        "FR_DSR_0_industrie": {"properties": {}, "data": {}, "modulation": ["CM_industrie.arrow"]},
        "FR_DSR_0_tertiaire": {"properties": {}, "data": {}, "modulation": ["CM_tertiaire.arrow"]},
        "FR_DSR_0_implicite": {"properties": {}, "data": {}, "modulation": ["CM_implicite.arrow"]},
    }

    # Act
    generate_dsr_clusters(area_obj, dsr_data)

    # Assert
    expected_global_max = val_industrie + val_tertiaire + val_implicite

    assert mock_create_modulation.call_count == 3

    # Check that each call used the same expected_global_max
    for i in range(3):
        args, _ = mock_create_modulation.call_args_list[i]
        assert args[1] == expected_global_max


@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_modulation_matrix_from_series")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_cluster")
@patch("antares.datamanager.generator.generate_dsr_clusters.pd.read_feather")
@patch("antares.datamanager.generator.generate_dsr_clusters.Path.exists")
def test_generate_dsr_clusters_calculates_global_max_case_insensitive_cm(
    mock_exists, mock_read_feather, mock_create_dsr_cluster, mock_create_modulation
):
    # Test case to reproduce issue where "cm_" (lowercase) is used instead of "CM_"
    # Arrange
    mock_exists.return_value = True
    area_obj = MagicMock(spec=Area)

    series = pd.Series([100, 200], name=0)
    mock_read_feather.return_value = pd.DataFrame({0: series})

    # Use lowercase "cm_" in for modulation filename
    dsr_data = {
        "dsr_1": {"properties": {}, "data": {}, "modulation": ["cm_lowercase_file.arrow"]},
    }

    # Act
    generate_dsr_clusters(area_obj, dsr_data)

    # Assert
    # If case-insensitive search works, global_max should be 200.0
    # If it fails, global_max will be 0
    expected_global_max = 200.0

    args, _ = mock_create_modulation.call_args
    assert args[1] == expected_global_max


@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_modulation_matrix_from_series")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_cluster")
@patch("antares.datamanager.generator.generate_dsr_clusters.pd.read_feather")
@patch("antares.datamanager.generator.generate_dsr_clusters.Path.exists")
def test_generate_dsr_clusters_calculates_global_max_with_second_user_example(
    mock_exists, mock_read_feather, mock_create_dsr_cluster, mock_create_modulation
):
    # date              FR_DSR_implicite  FR_DSR_industrie  FR_DSR_tertiaire
    # 07/01/2028 00:00  2                 2                 2
    # Sum at 00:00 = 6
    # 07/01/2028 01:00  1                 1                 1
    # Sum at 01:00 = 3
    # 07/01/2028 05:00  0                 0                 0
    # Sum at 05:00 = 0

    # Arrange
    mock_exists.return_value = True
    area_obj = MagicMock(spec=Area)

    series_implicite = pd.Series([2, 1, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0], name=0)
    series_industrie = pd.Series([2, 1, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0], name=0)
    series_tertiaire = pd.Series([2, 1, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0], name=0)

    # Sum at index 0: 2 + 2 + 2 = 6
    # Global Max should be 6.0

    def side_effect(path):
        if "implicite" in str(path):
            return pd.DataFrame({0: series_implicite})
        if "industrie" in str(path):
            return pd.DataFrame({0: series_industrie})
        if "tertiaire" in str(path):
            return pd.DataFrame({0: series_tertiaire})
        return pd.DataFrame()

    mock_read_feather.side_effect = side_effect

    dsr_data = {
        "FR_DSR_implicite": {"properties": {}, "data": {}, "modulation": ["CM_implicite.arrow"]},
        "FR_DSR_industrie": {"properties": {}, "data": {}, "modulation": ["CM_industrie.arrow"]},
        "FR_DSR_tertiaire": {"properties": {}, "data": {}, "modulation": ["CM_tertiaire.arrow"]},
    }

    # Act
    generate_dsr_clusters(area_obj, dsr_data)

    # Assert
    expected_global_max = 6.0

    assert mock_create_modulation.call_count == 3
    for i in range(3):
        args, _ = mock_create_modulation.call_args_list[i]
        assert args[1] == expected_global_max
