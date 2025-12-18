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

from antares.datamanager.generator.generate_thermal_matrices_data import create_prepro_data_matrix


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

    jan_days = 31
    feb_days = 28

    # January block
    assert (fo_rate[:jan_days] == 0).all()
    # February block
    assert (fo_rate[jan_days : jan_days + feb_days] == 1).all()


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

    days = np.arange(365)
    day_of_year = days + 1

    winter_mask = (day_of_year <= 90) | (day_of_year >= 305)
    summer_mask = ~winter_mask

    expected_winter = data["npo_max_winter"] * factor
    expected_summer = data["npo_max_summer"] * factor

    assert np.allclose(npo_max[winter_mask], expected_winter)
    assert np.allclose(npo_max[summer_mask], expected_summer)


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


def test_create_prepro_data_matrix_when_critical_keys_missing_returns_365_default_rows():
    base = {
        "fo_duration": 1,
        "po_duration": 2,
        "fo_monthly_rate": [10] * 12,
        "po_monthly_rate": [20] * 12,
        "npo_max_winter": 5,
        "npo_max_summer": 10,
        "nb_unit": 2,
    }

    # Each missing critical key should trigger the default (365 x 6) matrix
    for missing_key in ["fo_duration", "po_duration", "npo_max_winter", "npo_max_summer"]:
        data = dict(base)
        del data[missing_key]

        df = create_prepro_data_matrix(data, unit_count=base["nb_unit"])
        expected = pd.DataFrame([[1, 1, 0, 0, 0, 0]] * 365)
        pd.testing.assert_frame_equal(df, expected)
