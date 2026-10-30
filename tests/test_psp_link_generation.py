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

import pandas as pd

from antares.datamanager.generator.generate_psp_link import (
    create_psp_link,
    get_maxpower_data,
)


def test_get_maxpower_data_success(tmp_path):
    """Test loading maxpower data from arrow file."""

    # Create a test arrow file with maxpower data (all columns must have same length)
    df = pd.DataFrame(
        {
            "at_pumping": [100.0, 150.0, 120.0],
            "at_generating": [200.0, 250.0, 180.0],
            "fr_pumping": [300.0, 350.0, 310.0],
            "fr_generating": [400.0, 450.0, 420.0],
        }
    )
    maxpower_file = tmp_path / "AT_psp_maxpower.b97589c6-a023-4b5a-bfdb-0991272d52bb.arrow"
    df.to_feather(maxpower_file)
    other_area_file = tmp_path / "FR_psp_maxpower.53a7cc20-dbc4-4a2d-b511-e7254969da23.arrow"
    pd.DataFrame({"fr_pumping": [900.0], "fr_generating": [1000.0]}).to_feather(other_area_file)

    used_files = {maxpower_file, other_area_file}
    max_pumping, max_generating = get_maxpower_data("AT", used_files)

    assert max_pumping == 150.0
    assert max_generating == 250.0
    assert maxpower_file in used_files


def test_get_maxpower_data_file_not_found(tmp_path):
    """Test handling of a maxpower file not registered in used_files."""
    df = pd.DataFrame({"at_pumping": [100.0], "at_generating": [200.0]})
    df.to_feather(tmp_path / "AT_psp_maxpower.b97589c6-a023-4b5a-bfdb-0991272d52bb.arrow")

    used_files = set()
    max_pumping, max_generating = get_maxpower_data("AT", used_files)

    assert max_pumping == 0.0
    assert max_generating == 0.0


def test_get_maxpower_data_missing_columns(tmp_path):
    """Test handling of missing columns in maxpower file."""

    # Create arrow file with only fr data, no at data
    df = pd.DataFrame(
        {
            "fr_pumping": [300.0, 350.0],
            "fr_generating": [400.0, 450.0],
        }
    )
    maxpower_file = tmp_path / "AT_psp_maxpower.b97589c6-a023-4b5a-bfdb-0991272d52bb.arrow"
    df.to_feather(maxpower_file)

    used_files = {maxpower_file}
    max_pumping, max_generating = get_maxpower_data("AT", used_files)

    assert max_pumping == 0.0
    assert max_generating == 0.0


def test_get_maxpower_data_case_insensitive(tmp_path):
    """Test case-insensitive column name matching."""

    # Create arrow file with mixed case column names
    # Note: The file name uses the area name as-is, but columns are matched case-insensitively
    df = pd.DataFrame(
        {
            "AT_PUMPING": [100.0, 150.0, 120.0],
            "At_Generating": [200.0, 250.0, 180.0],
        }
    )
    maxpower_file = tmp_path / "AT_psp_maxpower.b97589c6-a023-4b5a-bfdb-0991272d52bb.arrow"
    df.to_feather(maxpower_file)

    used_files = {maxpower_file}
    # Call with "AT" to match the file name
    max_pumping, max_generating = get_maxpower_data("AT", used_files)

    assert max_pumping == 150.0
    assert max_generating == 250.0


@patch("antares.datamanager.generator.generate_psp_link.generate_link_parameters_df")
@patch("antares.datamanager.generator.generate_psp_link.generate_constant_link_capacity_df")
@patch("antares.datamanager.generator.generate_psp_link.get_maxpower_data")
def test_create_psp_link_success(mock_get_maxpower, mock_link_capacity_df, mock_link_params_df):
    """Test successful creation of PSP link."""
    mock_get_maxpower.return_value = (100.0, 200.0)

    mock_df_direct = MagicMock()
    mock_df_indirect = MagicMock()
    mock_df_params = MagicMock()

    mock_link_capacity_df.side_effect = [mock_df_direct, mock_df_indirect]
    mock_link_params_df.return_value = mock_df_params

    mock_study = MagicMock()
    mock_link = MagicMock()
    mock_study.create_link.return_value = mock_link

    used_files = set()
    create_psp_link(mock_study, "AT", "w_hydro_open_at", used_files)

    # Verify link creation
    mock_study.create_link.assert_called_once_with(area_from="at", area_to="w_hydro_open_at")

    # Verify capacity matrices
    assert mock_link_capacity_df.call_count == 2
    mock_link_capacity_df.assert_any_call(100.0)  # pumping for direct
    mock_link_capacity_df.assert_any_call(200.0)  # generating for indirect

    mock_link.set_capacity_direct.assert_called_once_with(mock_df_direct)
    mock_link.set_capacity_indirect.assert_called_once_with(mock_df_indirect)

    # Verify hurdle costs
    mock_link_params_df.assert_called_once_with(0.01)
    mock_link.update_properties.assert_called_once()
    mock_link.set_parameters.assert_called_once_with(mock_df_params)


@patch("antares.datamanager.generator.generate_psp_link.generate_constant_link_capacity_df")
@patch("antares.datamanager.generator.generate_psp_link.get_maxpower_data")
def test_create_psp_link_handles_error(mock_get_maxpower, mock_link_capacity_df):
    """Test error handling in PSP link creation."""
    from antares.datamanager.exceptions.exceptions import APIGenerationError, LinkGenerationError

    mock_get_maxpower.return_value = (100.0, 200.0)
    # Make the mock raise an APIGenerationError to trigger LinkGenerationError
    mock_link_capacity_df.side_effect = APIGenerationError("Test error")

    mock_study = MagicMock()
    mock_link = MagicMock()
    mock_study.create_link.return_value = mock_link

    used_files = set()
    with pytest.raises(LinkGenerationError):
        create_psp_link(mock_study, "AT", "w_hydro_open_at", used_files)


@patch("antares.datamanager.generator.generate_study_process.create_psp_link")
@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
@patch("antares.datamanager.generator.generate_study_process.generate_hydro")
def test_psp_link_created_during_area_hydro_generation(mock_generate_hydro, mock_load_dir, mock_create_psp_link):
    """Test that PSP link is created when PSP hydro is present."""
    from antares.datamanager.generator.generate_study_process import _generate_area_hydro

    mock_load_dir.return_value = Path("/mock/load/dir")
    mock_study = MagicMock()
    mock_real_area_obj = MagicMock(name="real_area")
    mock_virtual_area_obj = MagicMock(name="virtual_area")
    mock_study.create_area.return_value = mock_virtual_area_obj

    psp_data = {"properties": {"reservoir": True}, "series": ["AT_psp_mod.arrow"]}
    hydro_data = {"properties": {"follow_load": True}, "series": ["AT_mod.arrow"], "psp": psp_data}

    used_files = set()

    _generate_area_hydro(mock_study, mock_real_area_obj, "AT", hydro_data, used_files)

    # Verify PSP link creation was called
    mock_create_psp_link.assert_called_once_with(mock_study, "AT", "w_hydro_open_at", used_files)
