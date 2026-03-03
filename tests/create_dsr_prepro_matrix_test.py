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

from antares.datamanager.generator.generate_dsr_clusters import create_dsr_prepro_data_matrix


def test_create_dsr_prepro_data_matrix_empty_data():
    """Test with empty or no data should return default 365x6 matrix."""
    df = create_dsr_prepro_data_matrix({}, 1)
    assert df.shape == (365, 6)
    expected_row = [1, 1, 0, 0, 0, 0]
    for i in range(365):
        assert df.iloc[i].tolist() == expected_row


def test_create_dsr_prepro_data_matrix_valid_data():
    """Test with valid fo_duration and fo_monthly_rate."""
    fo_monthly_rate = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2]
    data = {"fo_duration": 5, "fo_monthly_rate": fo_monthly_rate}
    df = create_dsr_prepro_data_matrix(data, 1)

    assert df.shape == (365, 6)

    # Check fo_duration (first column)
    assert (df.iloc[:, 0] == 5).all()

    # Check po_duration (second column, default 1)
    assert (df.iloc[:, 1] == 1).all()

    # Check fo_rate_daily (third column)
    # January (31 days)
    assert (df.iloc[0:31, 2] == 0.1).all()
    # February (28 days)
    assert (df.iloc[31 : 31 + 28, 2] == 0.2).all()
    # December (31 days)
    assert (df.iloc[365 - 31 : 365, 2] == 0.2).all()

    # Check po_rate, npo_min, npo_max (columns 3, 4, 5, all 0)
    assert (df.iloc[:, 3] == 0).all()
    assert (df.iloc[:, 4] == 0).all()
    assert (df.iloc[:, 5] == 0).all()


def test_create_dsr_prepro_data_matrix_monthly_expansion():
    """Detailed check of the monthly expansion logic."""
    fo_monthly_rate = [i / 10.0 for i in range(12)]
    data = {"fo_duration": 1, "fo_monthly_rate": fo_monthly_rate}
    df = create_dsr_prepro_data_matrix(data, 1)

    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    current_day = 0
    for month in range(12):
        month_days = days_in_month[month]
        expected_rate = fo_monthly_rate[month]
        assert (df.iloc[current_day : current_day + month_days, 2] == expected_rate).all()
        current_day += month_days
    assert current_day == 365
