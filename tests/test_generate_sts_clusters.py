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
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
from antares.datamanager.generator.generate_sts_clusters import generate_sts_clusters

def test_generate_sts_clusters_basic():
    mock_area = MagicMock()
    mock_storage = MagicMock()
    mock_area.create_st_storage.return_value = mock_storage
    
    sts_data = {
        "cluster1": {
            "properties": {
                "enabled": True,
                "group": "battery",
                "reservoir_capacity": 100
            },
            "series": []
        }
    }
    
    with patch("antares.datamanager.generator.generate_sts_clusters.STStorageProperties", side_effect=lambda **kwargs: kwargs):
        generate_sts_clusters(mock_area, sts_data)
    
    mock_area.create_st_storage.assert_called_once_with("cluster1", {"enabled": True, "group": "battery", "reservoir_capacity": 100})

@patch("antares.datamanager.generator.generate_sts_clusters.pd.read_feather")
@patch("antares.datamanager.generator.generate_sts_clusters.settings")
def test_generate_sts_clusters_matrices(mock_settings, mock_read_feather):
    mock_area = MagicMock()
    mock_storage = MagicMock()
    mock_area.create_st_storage.return_value = mock_storage
    
    mock_settings.sts_ts_directory = Path("/fake/sts_ts")
    
    # The arrow file will have one column, not necessarily named TS1
    df_dummy = pd.DataFrame({"SomeColumn": [1.0, 2.0]})
    mock_read_feather.return_value = df_dummy
    
    sts_data = {
        "cluster1": {
            "properties": {},
            "series": [
                "inflows.xlsx.uuid1.arrow",
                "lower_curve.xlsx.uuid2.arrow",
                "Pmax_injection.xlsx.uuid3.arrow",
                "Pmax_soutirage.xlsx.uuid4.arrow",
                "upper_curve.xlsx.uuid5.arrow",
                "unknown.xlsx.uuid6.arrow"
            ]
        }
    }
    
    # Mock Path.exists to return True for all these files
    with patch("antares.datamanager.generator.generate_sts_clusters.Path.exists", return_value=True):
        generate_sts_clusters(mock_area, sts_data)
    
    # Verify setters were called with the first column as a DataFrame
    expected_df = df_dummy.iloc[:, [0]]
    mock_storage.set_storage_inflows.assert_called_once()
    actual_df = mock_storage.set_storage_inflows.call_args[0][0]
    pd.testing.assert_frame_equal(actual_df, expected_df)

    mock_storage.set_lower_rule_curve.assert_called_once()
    mock_storage.update_pmax_injection.assert_called_once()
    mock_storage.set_pmax_withdrawal.assert_called_once()
    mock_storage.set_upper_rule_curve.assert_called_once()
    
    # Total calls to read_feather should be 5 (not 6, because unknown prefix is skipped)
    assert mock_read_feather.call_count == 5

@patch("antares.datamanager.generator.generate_sts_clusters.settings")
def test_generate_sts_clusters_missing_file(mock_settings):
    mock_area = MagicMock()
    mock_storage = MagicMock()
    mock_area.create_st_storage.return_value = mock_storage
    mock_settings.sts_ts_directory = Path("/fake/sts_ts")
    
    sts_data = {
        "cluster1": {
            "properties": {},
            "series": ["inflows.xlsx.uuid1.arrow"]
        }
    }
    
    with patch("antares.datamanager.generator.generate_sts_clusters.Path.exists", return_value=False):
        with patch("builtins.print") as mock_print:
            generate_sts_clusters(mock_area, sts_data)
            mock_print.assert_any_call("Warning: STS matrix file not found: /fake/sts_ts/inflows.xlsx.uuid1.arrow")
    
    mock_storage.set_storage_inflows.assert_not_called()
