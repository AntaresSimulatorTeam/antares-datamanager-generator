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

import numpy as np
import pandas as pd

from antares.craft.model.area import Area
from antares.datamanager.generator.generate_dsr_clusters import (
    create_dsr_modulation_matrix_from_series,
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

    area_obj = MagicMock(spec=Area)

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

    expected_df = pd.DataFrame({"constraint": ["c1"]})
    mock_generate_constraints.return_value = expected_df

    # Act
    result = generate_dsr_clusters(area_obj, dsr_data)

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
    pd.testing.assert_frame_equal(result, expected_df)


@patch("antares.datamanager.generator.generate_dsr_clusters.generate_dsr_binding_constraints")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_modulation_matrix_from_series")
@patch("antares.datamanager.generator.generate_dsr_clusters.create_dsr_cluster")
def test_generate_dsr_clusters_with_empty_modulation(
    mock_create_dsr_cluster,
    mock_create_modulation,
    mock_generate_constraints,
):
    # Arrange
    area_obj = MagicMock(spec=Area)

    dsr_data = {
        "dsr_1": {
            "modulation": [],
        }
    }

    mock_generate_constraints.return_value = pd.DataFrame()

    # Act
    generate_dsr_clusters(area_obj, dsr_data)

    # Assert
    mock_create_modulation.assert_called_once()

    series = mock_create_modulation.call_args.args[0]

    assert isinstance(series, pd.Series)
    assert len(series) == 8760
    assert np.all(series == 1)

    mock_create_dsr_cluster.assert_called_once()


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

    area_obj = MagicMock(spec=Area)

    dsr_data = {
        "dsr_1": {
            "modulation": ["CM_missing.arrow"],
        }
    }

    mock_generate_constraints.return_value = pd.DataFrame()

    # Act
    generate_dsr_clusters(area_obj, dsr_data)

    # Assert
    mock_logger.warning.assert_called_once()
    mock_create_dsr_cluster.assert_called_once()
