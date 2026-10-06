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

import dataclasses

from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from antares.craft import Month, ThermalClusterProperties
from antares.datamanager.exceptions.exceptions import MEGenerationError
from antares.datamanager.generator.generate_thermal_clusters import (
    NPO_SUMMER_DIVISOR,
    NPO_WINTER_DIVISOR,
    create_modulation_me_matrix,
    create_prepro_data_matrix,
    create_thermal_me_cluster,
    generate_thermal_me_clusters,
    resolve_and_validate_res_arrow_path,
)


def test_npo_max_default_when_zero():
    """Verify default NPO max values when input is zero."""
    data = {
        "fo_duration": 1,
        "po_duration": 2,
        "fo_monthly_rate": [10] * 12,
        "po_monthly_rate": [20] * 12,
        "npo_max_winter": 0,
        "npo_max_summer": 0,
        "nb_unit": 1,
    }

    unit_count = 12
    df = create_prepro_data_matrix(data, unit_count, Month.JULY)

    npo_max = df.iloc[:, 5]
    # Starting July 1st
    # Jul (0-30), Aug (31-61), Sep (62-91) -> Summer (0-91)
    # Oct (92-122), Nov (123-152), Dec (153-183) -> Winter (92-183)
    # Jan (184-214), Feb (215-242), Mar (243-273) -> Winter (184-273)
    # Apr (274-303), May (304-334), Jun (335-364) -> Summer (274-364)

    expected_summer = unit_count / NPO_SUMMER_DIVISOR
    expected_winter = unit_count / NPO_WINTER_DIVISOR

    # Summer slices
    assert (npo_max[0:92] == expected_summer).all()
    assert (npo_max[274:365] == expected_summer).all()
    # Winter slice
    assert (npo_max[92:274] == expected_winter).all()


def test_prepro_basic_shape():
    """Matrix should contain 365 days and 6 columns."""
    data = {
        "fo_duration": 1,
        "po_duration": 2,
        "fo_monthly_rate": [10] * 12,
        "po_monthly_rate": [20] * 12,
        "npo_max_winter": 5,
        "npo_max_summer": 10,
        "nb_unit": 2,
    }

    df = create_prepro_data_matrix(data, unit_count=2, first_month=Month.JULY)

    assert df.shape == (365, 6)


def test_monthly_to_daily_expansion():
    """Verify that monthly values expand properly into daily vectors."""
    data = {
        "fo_duration": 1,
        "po_duration": 2,
        "fo_monthly_rate": list(range(12)),  # months 0..11
        "po_monthly_rate": [100] * 12,
        "npo_max_winter": 5,
        "npo_max_summer": 10,
        "nb_unit": 1,
    }

    df = create_prepro_data_matrix(data, unit_count=1, first_month=Month.JULY)

    fo_rate = df.iloc[:, 2]

    # Row 0 is July (index 6)
    assert (fo_rate[0:31] == 6).all()
    # Row 184 is January (index 0)
    assert (fo_rate[184 : 184 + 31] == 0).all()


def test_npo_min_is_zero():
    """npo_min must be 0 for all 365 days."""
    data = {
        "fo_duration": 1,
        "po_duration": 2,
        "fo_monthly_rate": [10] * 12,
        "po_monthly_rate": [20] * 12,
        "npo_max_winter": 5,
        "npo_max_summer": 10,
        "nb_unit": 1,
    }

    df = create_prepro_data_matrix(data, unit_count=1, first_month=Month.JULY)
    npo_min = df.iloc[:, 4]

    assert (npo_min == 0).all()


def test_npo_max_season_logic():
    """Check the correct assignment of summer vs. winter values at a daily level."""
    data = {
        "fo_duration": 1,
        "po_duration": 2,
        "fo_monthly_rate": [10] * 12,
        "po_monthly_rate": [20] * 12,
        "npo_max_winter": 8,
        "npo_max_summer": 4,
        "nb_unit": 2,
    }

    unit_count = 4
    df = create_prepro_data_matrix(data, unit_count, Month.JULY)

    npo_max = df.iloc[:, 5]
    factor = unit_count / data["nb_unit"]

    expected_winter = data["npo_max_winter"] * factor
    expected_summer = data["npo_max_summer"] * factor

    # July (Summer)
    assert np.allclose(npo_max[0:31], expected_summer)
    # October (Winter)
    assert np.allclose(npo_max[92:123], expected_winter)
    # January (Winter)
    assert np.allclose(npo_max[184:215], expected_winter)


def test_invalid_monthly_rate_length():
    """Should raise if monthly arrays are not length 12."""
    data = {
        "fo_duration": 1,
        "po_duration": 2,
        "fo_monthly_rate": [1] * 11,  # invalid
        "po_monthly_rate": [2] * 12,
        "npo_max_winter": 5,
        "npo_max_summer": 10,
        "nb_unit": 1,
    }

    with pytest.raises(ValueError):
        create_prepro_data_matrix(data, unit_count=1, first_month=Month.JULY)


def test_create_prepro_data_matrix_when_data_is_none_returns_365_default_rows():
    df = create_prepro_data_matrix(None, unit_count=5, first_month=Month.JULY)

    expected = pd.DataFrame([[1, 1, 0, 0, 0, 0]] * 365)

    pd.testing.assert_frame_equal(df, expected)


def test_season_boundaries():
    """Verify that winter and summer boundaries match exactly with the requirements.
    Row 0 = July 1st.
    """
    data = {
        "fo_duration": 1,
        "po_duration": 2,
        "fo_monthly_rate": [10] * 12,
        "po_monthly_rate": [20] * 12,
        "npo_max_winter": 8,
        "npo_max_summer": 4,
        "nb_unit": 1,
    }

    unit_count = 1
    df = create_prepro_data_matrix(data, unit_count, Month.JULY)
    npo_max = df.iloc[:, 5]

    # September 29th is Row 90
    assert npo_max[90] == 4
    # September 30th is Row 91
    assert npo_max[91] == 4
    # October 1st is Row 92
    assert npo_max[92] == 8
    # March 31st is Row 273
    assert npo_max[273] == 8
    # April 1st is Row 274
    assert npo_max[274] == 4


def test_flexibility_dynamic_parameter():
    """Verify that the first_month parameter works dynamically."""
    data = {
        "fo_duration": 1,
        "po_duration": 2,
        "fo_monthly_rate": list(range(12)),
        "po_monthly_rate": [20] * 12,
        "npo_max_winter": 0,
        "npo_max_summer": 0,
        "nb_unit": 1,
    }

    # Test January start via parameter
    df_jan = create_prepro_data_matrix(data, unit_count=1, first_month=Month.JANUARY)
    fo_rate_jan = df_jan.iloc[:, 2]
    # Row 0 is January (index 0)
    assert (fo_rate_jan[0:31] == 0).all()

    # Test July start via parameter
    df_jul = create_prepro_data_matrix(data, unit_count=1, first_month=Month.JULY)
    fo_rate_jul = df_jul.iloc[:, 2]
    # Row 0 is July (index 6)
    assert (fo_rate_jul[0:31] == 6).all()


def test_flexibility_january_start(monkeypatch):
    data = {
        "fo_duration": 1,
        "po_duration": 2,
        "fo_monthly_rate": list(range(1, 13)),  # 1..12 for months Jan..Dec
        "po_monthly_rate": [20] * 12,
        "npo_max_winter": 8,
        "npo_max_summer": 4,
        "nb_unit": 1,
    }

    df = create_prepro_data_matrix(data, unit_count=1, first_month=Month.JANUARY)

    # Row 0 should be January (if JANUARY start)
    # fo_monthly_rate[0] is 1
    assert (df.iloc[0:31, 2] == 1).all()

    # March 31st is Day 90 (0-indexed 89)
    # npo_max for March should be winter (8)
    assert df.iloc[89, 5] == 8

    # April 1st is Day 91 (0-indexed 90)
    # npo_max for April should be summer (4)
    assert df.iloc[90, 5] == 4


def test_resolve_and_validate_res_arrow_path(tmp_path):
    # Test valid path with Path and str
    arrow_file = tmp_path / "test.arrow"
    arrow_file.write_text("dummy")

    resolved_path = resolve_and_validate_res_arrow_path(tmp_path, "test.arrow")
    assert resolved_path == arrow_file.resolve()

    resolved_str_path = resolve_and_validate_res_arrow_path(str(tmp_path), "test.arrow")
    assert resolved_str_path == arrow_file.resolve()

    # Invalid filename
    with pytest.raises(MEGenerationError, match="must be a non-empty string"):
        resolve_and_validate_res_arrow_path(tmp_path, "")

    # Invalid extension
    with pytest.raises(MEGenerationError, match="Unexpected ME file extension for 'test.txt', expected .arrow"):
        resolve_and_validate_res_arrow_path(tmp_path, "test.txt")

    # Outside base directory
    with pytest.raises(MEGenerationError, match="outside allowed directory"):
        resolve_and_validate_res_arrow_path(tmp_path, "../test.arrow")

    # File not found
    with pytest.raises(FileNotFoundError, match="ME series file not found"):
        resolve_and_validate_res_arrow_path(
            tmp_path,
            "ME series file not found: /tmp/pytest-of-etiennemar/pytest-1/test_resolve_and_validate_res_4/nonexistent.arrow",
        )


def test_create_modulation_me_matrix_scalar():
    cluster_values = {
        "modulation": {
            "marginal_cost_modulation": 1.5,
            "market_bid_modulation": 2.0,
            "capacity_modulation": 0.8,
            "must_run_modulation": 0.5,
        }
    }
    df = create_modulation_me_matrix("cluster_1", cluster_values)
    assert df.shape == (8760, 4)
    assert (df.iloc[:, 0] == 1.5).all()
    assert (df.iloc[:, 1] == 2.0).all()
    assert (df.iloc[:, 2] == 0.8).all()
    assert (df.iloc[:, 3] == 0.5).all()


def test_create_modulation_me_matrix_feather(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "antares.datamanager.generator.generate_thermal_clusters.thermal_me_modulation_output_directory",
        lambda: tmp_path,
    )
    df_series = pd.DataFrame({"cluster_1": [1.0, 2.0, 3.0]})
    file_path = tmp_path / "capacity_modulation_test.arrow"
    df_series.to_feather(file_path)

    cluster_values = {
        "modulation": {
            "marginal_cost_modulation": 1.0,
            "market_bid_modulation": 1.0,
            "must_run_modulation": 0.0,
        },
        "series": ["capacity_modulation_test.arrow"],
    }
    df = create_modulation_me_matrix("cluster_1", cluster_values)
    assert df.shape == (3, 4)
    assert (df.iloc[:, 2] == [1.0, 2.0, 3.0]).all()
    assert (df.iloc[:, 0] == 1.0).all()
    assert (df.iloc[:, 1] == 1.0).all()
    assert (df.iloc[:, 3] == 0.0).all()


def test_create_modulation_me_matrix_empty_or_none():
    # If modulation is None
    df_none = create_modulation_me_matrix("cluster_1", {"modulation": None})
    assert df_none.shape == (8760, 4)
    assert (df_none.iloc[:, 0] == 1).all()
    assert (df_none.iloc[:, 1] == 1).all()
    assert (df_none.iloc[:, 2] == 1).all()
    assert (df_none.iloc[:, 3] == 0).all()


def test_create_modulation_me_matrix_missing_value_and_file_raises():
    cluster_values = {
        "modulation": {
            "marginal_cost_modulation": 1.0,
            # market_bid_modulation missing and not in series
            "capacity_modulation": 1.0,
            "must_run_modulation": 0.0,
        },
        "series": [],
    }
    with pytest.raises(ValueError, match="Aucune valeur ni fichier spécifié pour 'market_bid_modulation'"):
        create_modulation_me_matrix("cluster_1", cluster_values)


def test_create_modulation_me_matrix_column_not_found_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "antares.datamanager.generator.generate_thermal_clusters.thermal_me_modulation_output_directory",
        lambda: tmp_path,
    )
    df_series = pd.DataFrame({"other_cluster": [1.0, 2.0, 3.0]})
    file_path = tmp_path / "marginal_cost_modulation_test.arrow"
    df_series.to_feather(file_path)

    cluster_values = {
        "modulation": {
            "market_bid_modulation": 1.0,
            "capacity_modulation": 1.0,
            "must_run_modulation": 0.0,
        },
        "series": ["marginal_cost_modulation_test.arrow"],
    }
    with pytest.raises(ValueError, match="Colonne 'cluster_1' introuvable"):
        create_modulation_me_matrix("cluster_1", cluster_values)


def test_create_modulation_me_matrix_mismatched_series_lengths_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "antares.datamanager.generator.generate_thermal_clusters.thermal_me_modulation_output_directory",
        lambda: tmp_path,
    )
    df_series_1 = pd.DataFrame({"cluster_1": [1.0, 2.0, 3.0]})
    file_path_1 = tmp_path / "marginal_cost_modulation_test.arrow"
    df_series_1.to_feather(file_path_1)

    df_series_2 = pd.DataFrame({"cluster_1": [1.0, 2.0, 3.0, 4.0]})
    file_path_2 = tmp_path / "market_bid_modulation_test.arrow"
    df_series_2.to_feather(file_path_2)

    cluster_values = {
        "modulation": {
            "capacity_modulation": 1.0,
            "must_run_modulation": 0.0,
        },
        "series": ["marginal_cost_modulation_test.arrow", "market_bid_modulation_test.arrow"],
    }
    with pytest.raises(
        ValueError, match="Toutes les colonnes de modulation de fichiers doivent avoir le même nombre de lignes"
    ):
        create_modulation_me_matrix("cluster_1", cluster_values)


def test_create_modulation_me_matrix_tracks_used_files(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "antares.datamanager.generator.generate_thermal_clusters.thermal_me_modulation_output_directory",
        lambda: tmp_path,
    )
    df_series = pd.DataFrame({"cluster_1": [1.0, 2.0, 3.0]})
    file_path = tmp_path / "capacity_modulation_test.arrow"
    df_series.to_feather(file_path)

    cluster_values = {
        "modulation": {
            "marginal_cost_modulation": 1.0,
            "market_bid_modulation": 1.0,
            "must_run_modulation": 0.0,
        },
        "series": ["capacity_modulation_test.arrow"],
    }
    used_files = set()
    create_modulation_me_matrix("cluster_1", cluster_values, used_files=used_files)
    assert file_path in used_files


def test_create_thermal_me_cluster():
    area_mock = MagicMock()
    thermal_cluster_mock = MagicMock()
    area_mock.create_thermal_cluster.return_value = thermal_cluster_mock

    cluster_values = {
        "properties": {
            "nominal_capacity": 500.0,
            "unit_count": 2,
            "enabled": True,
            "must_run": True,
            "marginal_cost": 45.0,
            "market_bid_cost": 50.0,
            "group": "Other 1",
        },
        "modulation": {
            "marginal_cost_modulation": 1.0,
            "market_bid_modulation": 1.0,
            "capacity_modulation": 1.0,
            "must_run_modulation": 0.0,
        },
    }

    create_thermal_me_cluster(
        area_obj=area_mock,
        cluster_name="V_ME_H2_LONG_IBER_cluster",
        cluster_values=cluster_values,
    )

    area_mock.create_thermal_cluster.assert_called_once()
    cluster_name_arg, properties_arg = area_mock.create_thermal_cluster.call_args[0]
    assert cluster_name_arg == "V_ME_H2_LONG_IBER_cluster"
    assert isinstance(properties_arg, ThermalClusterProperties)
    assert dataclasses.is_dataclass(properties_arg)
    # Ensure asdict succeeds without raising TypeError
    props_dict = dataclasses.asdict(properties_arg)
    assert props_dict["nominal_capacity"] == 500.0
    assert props_dict["unit_count"] == 2
    assert props_dict["enabled"] is True
    assert props_dict["must_run"] is True
    assert props_dict["marginal_cost"] == 45.0
    assert props_dict["market_bid_cost"] == 50.0
    assert props_dict["group"] == "Other 1"

    thermal_cluster_mock.set_prepro_modulation.assert_called_once()


def test_generate_thermal_me_clusters(tmp_path):
    area_mock = MagicMock()
    thermal_cluster_mock1 = MagicMock()
    thermal_cluster_mock2 = MagicMock()
    area_mock.create_thermal_cluster.side_effect = [thermal_cluster_mock1, thermal_cluster_mock2]

    thermals = {
        "cluster_1": {
            "properties": {"nominal_capacity": 100.0},
            "modulation": {
                "marginal_cost_modulation": 1.0,
                "market_bid_modulation": 1.0,
                "capacity_modulation": 1.0,
                "must_run_modulation": 0.0,
            },
        },
        "cluster_2": {
            "properties": {"nominal_capacity": 200.0},
            "modulation": {
                "marginal_cost_modulation": 2.0,
                "market_bid_modulation": 2.0,
                "capacity_modulation": 2.0,
                "must_run_modulation": 1.0,
            },
        },
    }

    used_files = set()
    generate_thermal_me_clusters(area_mock, thermals, used_files=used_files)

    assert area_mock.create_thermal_cluster.call_count == 2
    thermal_cluster_mock1.set_prepro_modulation.assert_called_once()
    thermal_cluster_mock2.set_prepro_modulation.assert_called_once()
