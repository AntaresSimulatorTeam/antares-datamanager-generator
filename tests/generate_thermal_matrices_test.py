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

import numpy as np
import pandas as pd

from antares.craft import Month
from antares.datamanager.generator.generate_thermal_clusters import (
    NPO_SUMMER_DIVISOR,
    NPO_WINTER_DIVISOR,
    create_prepro_data_matrix,
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
    df = create_prepro_data_matrix(data, unit_count)

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

    df = create_prepro_data_matrix(data, unit_count=2)

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

    df = create_prepro_data_matrix(data, unit_count=1)

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

    df = create_prepro_data_matrix(data, unit_count=1)
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
    df = create_prepro_data_matrix(data, unit_count)

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
        create_prepro_data_matrix(data, unit_count=1)


def test_create_prepro_data_matrix_when_data_is_none_returns_365_default_rows():
    df = create_prepro_data_matrix(None, unit_count=5)

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
    df = create_prepro_data_matrix(data, unit_count)
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
    """Verify that if STUDY_SETTING_FIRST_MONTH is JANUARY, the matrix starts on Jan 1st."""
    monkeypatch.setenv("STUDY_SETTING_FIRST_MONTH", "JANUARY")
    # Reload settings to pick up the new env var if needed,
    # but here we rely on the fact that Settings.study_setting_first_month
    # calls os.getenv every time.

    data = {
        "fo_duration": 1,
        "po_duration": 2,
        "fo_monthly_rate": list(range(1, 13)),  # 1..12 for months Jan..Dec
        "po_monthly_rate": [20] * 12,
        "npo_max_winter": 8,
        "npo_max_summer": 4,
        "nb_unit": 1,
    }

    df = create_prepro_data_matrix(data, unit_count=1)

    # Row 0 should be January (if JANUARY start)
    # fo_monthly_rate[0] is 1
    assert (df.iloc[0:31, 2] == 1).all()

    # March 31st is Day 90 (0-indexed 89)
    # npo_max for March should be winter (8)
    assert df.iloc[89, 5] == 8

    # April 1st is Day 91 (0-indexed 90)
    # npo_max for April should be summer (4)
    assert df.iloc[90, 5] == 4
