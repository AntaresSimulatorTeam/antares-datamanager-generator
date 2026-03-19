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

from antares.datamanager.exceptions.exceptions import MiscGenerationError
from antares.datamanager.generator.generate_misc_timeseries import (
    build_misc_timeseries_matrix,
    generate_misc_timeseries,
)


@patch("antares.datamanager.generator.generate_misc_timeseries.settings")
def test_generate_misc_timeseries_sets_zero_matrix_when_misc_is_empty(mock_settings):
    mock_settings.misc_ts_directory = Path("/unused")
    area = MagicMock()

    generate_misc_timeseries(area, "FR", {})

    assert area.set_misc_gen.call_count == 1
    df = area.set_misc_gen.call_args[0][0]
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (8760, 8)
    assert list(df.columns) == ["CHP", "BioMass", "Bi Gas", "Waste", "GeoThermal", "Other", "PSP", "Row balance"]
    assert float(df.sum().sum()) == 0.0


@patch("antares.datamanager.generator.generate_misc_timeseries.settings")
@patch("antares.datamanager.generator.generate_misc_timeseries.pd.read_feather")
def test_build_misc_timeseries_matrix_maps_group_with_single_series(mock_read_feather, mock_settings, tmp_path):
    mock_settings.misc_ts_directory = tmp_path

    # Create placeholders so path existence checks pass.
    (tmp_path / "f1.arrow").write_text("x", encoding="utf-8")

    df_ones = pd.DataFrame({"FR": [1.0] * 8760})
    mock_read_feather.return_value = df_ones

    misc = {
        "waste": {
            "properties": {"capacity": 100},
            "series": ["f1.arrow"],
        }
    }

    matrix = build_misc_timeseries_matrix("FR", misc)

    assert matrix.shape == (8760, 8)
    assert float(matrix["Waste"].iloc[0]) == 100.0
    assert float(matrix["Waste"].iloc[-1]) == 100.0


@patch("antares.datamanager.generator.generate_misc_timeseries.settings")
@patch("antares.datamanager.generator.generate_misc_timeseries.pd.read_feather")
def test_build_misc_timeseries_matrix_maps_hydrokinetic_and_wave_to_other(mock_read_feather, mock_settings, tmp_path):
    mock_settings.misc_ts_directory = tmp_path

    (tmp_path / "hk.arrow").write_text("x", encoding="utf-8")
    (tmp_path / "wave.arrow").write_text("x", encoding="utf-8")

    mock_read_feather.side_effect = [
        pd.DataFrame({"FR": [0.1] * 8760}),
        pd.DataFrame({"FR": [0.2] * 8760}),
    ]

    misc = {
        "hydrokinetic": {"properties": {"capacity": 100}, "series": ["hk.arrow"]},
        "wave": {"properties": {"capacity": 50}, "series": ["wave.arrow"]},
    }

    matrix = build_misc_timeseries_matrix("FR", misc)

    # Other = 0.1*100 + 0.2*50 = 20
    assert float(matrix["Other"].iloc[42]) == 20.0


@patch("antares.datamanager.generator.generate_misc_timeseries.settings")
def test_build_misc_timeseries_matrix_ignores_psp_chp_and_row_balance(mock_settings):
    mock_settings.misc_ts_directory = Path("/unused")

    misc = {
        "x_open_pump": {"properties": {"capacity": 999}, "series": []},
        "chp": {"properties": {"capacity": 999}, "series": []},
        "row": {"properties": {"capacity": 999}, "series": []},
    }

    matrix = build_misc_timeseries_matrix("FR", misc)

    assert matrix.shape == (8760, 8)
    assert float(matrix["CHP"].sum()) == 0.0
    assert float(matrix["PSP"].sum()) == 0.0
    assert float(matrix["Row balance"].sum()) == 0.0
    assert float(matrix.sum().sum()) == 0.0


@patch("antares.datamanager.generator.generate_misc_timeseries.settings")
def test_build_misc_timeseries_matrix_rejects_multiple_series_for_same_group(mock_settings, tmp_path):
    mock_settings.misc_ts_directory = tmp_path

    misc = {
        "waste": {
            "properties": {"capacity": 1},
            "series": ["a.arrow", "b.arrow"],
        }
    }

    with pytest.raises(MiscGenerationError, match="Expected one MISC series file"):
        build_misc_timeseries_matrix("FR", misc)


@patch("antares.datamanager.generator.generate_misc_timeseries.settings")
def test_build_misc_timeseries_matrix_rejects_unsafe_path(mock_settings, tmp_path):
    mock_settings.misc_ts_directory = tmp_path

    misc = {
        "waste": {
            "properties": {"capacity": 1},
            "series": ["../escape.arrow"],
        }
    }

    with pytest.raises(MiscGenerationError, match="MISC series path outside allowed directory"):
        build_misc_timeseries_matrix("FR", misc)


@patch("antares.datamanager.generator.generate_misc_timeseries.settings")
def test_build_misc_timeseries_matrix_raises_when_file_missing(mock_settings, tmp_path):
    mock_settings.misc_ts_directory = tmp_path

    misc = {
        "waste": {
            "properties": {"capacity": 1},
            "series": ["missing.arrow"],
        }
    }

    with pytest.raises(FileNotFoundError, match="MISC series file not found"):
        build_misc_timeseries_matrix("FR", misc)


@patch("antares.datamanager.generator.generate_misc_timeseries.settings")
def test_build_misc_timeseries_matrix_rejects_non_object_group_payload(mock_settings, tmp_path):
    mock_settings.misc_ts_directory = tmp_path

    misc = {
        "waste": "invalid",
    }

    with pytest.raises(MiscGenerationError, match="Invalid MISC group data"):
        build_misc_timeseries_matrix("FR", misc)


@patch("antares.datamanager.generator.generate_misc_timeseries.settings")
def test_build_misc_timeseries_matrix_rejects_non_string_series_entries(mock_settings, tmp_path):
    mock_settings.misc_ts_directory = tmp_path

    misc = {
        "waste": {
            "properties": {"capacity": 1},
            "series": ["a.arrow", 1],
        }
    }

    with pytest.raises(MiscGenerationError, match=r"expected string\[\]"):
        build_misc_timeseries_matrix("FR", misc)


@patch("antares.datamanager.generator.generate_misc_timeseries.settings")
@patch("antares.datamanager.generator.generate_misc_timeseries.pd.read_feather")
def test_build_misc_timeseries_matrix_rejects_non_numeric_arrow_values(mock_read_feather, mock_settings, tmp_path):
    mock_settings.misc_ts_directory = tmp_path
    (tmp_path / "waste.arrow").write_text("x", encoding="utf-8")

    mock_read_feather.return_value = pd.DataFrame({"FR": ["bad"] * 8760})

    misc = {
        "waste": {
            "properties": {"capacity": 1},
            "series": ["waste.arrow"],
        }
    }

    with pytest.raises(MiscGenerationError, match="contains invalid values"):
        build_misc_timeseries_matrix("FR", misc)
