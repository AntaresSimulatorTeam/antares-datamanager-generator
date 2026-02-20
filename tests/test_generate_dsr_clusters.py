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
@patch("antares.datamanager.generator.generate_dsr_clusters.create_thermal_cluster_with_prepro")
@patch("antares.datamanager.generator.generate_dsr_clusters.pd.read_feather")
@patch("antares.datamanager.generator.generate_dsr_clusters.Path.exists")
def test_generate_dsr_clusters_calls_area_methods(
    mock_exists, mock_read_feather, mock_create_thermal_cluster_with_prepro, mock_create_modulation
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
    mock_create_thermal_cluster_with_prepro.assert_called_once()
