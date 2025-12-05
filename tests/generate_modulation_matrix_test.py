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
from unittest.mock import patch

import pandas as pd

from antares.datamanager.generator.generate_thermal_matrices_data import (
    create_modulation_matrix,
)


def test_create_modulation_matrix_empty_returns_default_df():
    df = create_modulation_matrix([])
    assert isinstance(df, pd.DataFrame)
    # Expect 8760 rows and 4 columns with default values [1,1,1,0]
    assert df.shape == (8760, 4)
    assert df.iloc[0].tolist() == [1, 1, 1, 0]
    assert df.iloc[-1].tolist() == [1, 1, 1, 0]


@patch("antares.datamanager.generator.generate_thermal_matrices_data.generator_param_modulation_directory")
@patch("antares.datamanager.generator.generate_thermal_matrices_data.pd.read_feather")
def test_create_modulation_matrix_builds_dataframe(mock_read_feather, mock_mod_dir):
    # Arrange
    mock_mod_dir.return_value = Path("/fake/mod")

    # First call for CM, second for MR
    cm_values = pd.DataFrame({"val": [0.1, 0.2]})
    mr_values = pd.DataFrame({"val": [0.9, 0.8]})
    mock_read_feather.side_effect = [cm_values, mr_values]

    cluster_modulation = [
        "CM_cluster.arrow",
        "MR_cluster.arrow",
    ]

    # Act
    df = create_modulation_matrix(cluster_modulation)

    # Assert
    assert df.shape == (2, 4)
    # Expect [[1,1,cm,mr], ...]
    assert df.iloc[0].tolist() == [1, 1, 0.1, 0.9]
    assert df.iloc[1].tolist() == [1, 1, 0.2, 0.8]

    # Check that files were read from the composed paths
    mock_read_feather.assert_any_call(Path("/fake/mod/CM_cluster.arrow"))
    mock_read_feather.assert_any_call(Path("/fake/mod/MR_cluster.arrow"))


@patch("antares.datamanager.generator.generate_thermal_matrices_data.generator_param_modulation_directory")
@patch("antares.datamanager.generator.generate_thermal_matrices_data.pd.read_feather")
def test_create_modulation_matrix_raises_on_mismatched_rows(mock_read_feather, mock_mod_dir):
    mock_mod_dir.return_value = Path("/fake/mod")

    cm_values = pd.DataFrame({"val": [0.1, 0.2]})  # 2 rows
    mr_values = pd.DataFrame({"val": [0.9, 0.8, 0.7]})  # 3 rows
    mock_read_feather.side_effect = [cm_values, mr_values]

    with pytest.raises(ValueError):
        create_modulation_matrix(["CM_x.arrow", "MR_x.arrow"])


@patch("antares.datamanager.generator.generate_thermal_matrices_data.generator_param_modulation_directory")
@patch("antares.datamanager.generator.generate_thermal_matrices_data.pd.read_feather")
def test_create_modulation_matrix_raises_when_missing_cm_or_mr(mock_read_feather, mock_mod_dir):
    mock_mod_dir.return_value = Path("/fake/mod")

    # Only CM provided
    with pytest.raises(FileNotFoundError):
        create_modulation_matrix(["CM_only.arrow"])  # missing MR

    # Only MR provided
    with pytest.raises(FileNotFoundError):
        create_modulation_matrix(["MR_only.arrow"])  # missing CM
