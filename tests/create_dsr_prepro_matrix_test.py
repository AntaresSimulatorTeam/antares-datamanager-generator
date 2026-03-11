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

import numpy as np
import pandas as pd

from antares.craft import Month
from antares.datamanager.generator.generate_dsr_clusters import (
    create_dsr_prepro_data_matrix,
    generate_dsr_binding_constraints,
)


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
    # Force JANUARY start to match legacy test expectations
    df = create_dsr_prepro_data_matrix(data, first_month=Month.JANUARY)

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
    # Force JANUARY start to match legacy test expectations
    df = create_dsr_prepro_data_matrix(data, first_month=Month.JANUARY)

    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    current_day = 0
    for month in range(12):
        month_days = days_in_month[month]
        expected_rate = fo_monthly_rate[month]
        assert (df.iloc[current_day : current_day + month_days, 2] == expected_rate).all()
        current_day += month_days
    assert current_day == 365


def test_generate_contraintes_couplantes_fr():
    # Setup data for FR (multiple sub-clusters)
    dsr_data = {
        "FR_DSR_tertiaire": {
            "properties": {
                "enabled": True,
                "nominal_capacity": 300,
            },
            "data": {"nb_hour_per_day": 13, "max_hour_per_day": 1},
        },
        "FR_DSR_industrie": {
            "properties": {
                "enabled": True,
                "nominal_capacity": 500,
            },
            "data": {"nb_hour_per_day": 10, "max_hour_per_day": 2},
        },
    }

    # Mock cluster_series (8760 hours)
    # For tertiaire: constant 100
    # For industrie: constant 200
    cluster_series = {"FR_DSR_tertiaire": pd.Series([100.0] * 8760), "FR_DSR_industrie": pd.Series([200.0] * 8760)}

    df_constraints = generate_dsr_binding_constraints(dsr_data, cluster_series)

    # Check shape: 365 days, 2 columns (because FR should keep columns separate)
    assert df_constraints.shape == (365, 2)
    assert "FR_DSR_tertiaire" in df_constraints.columns
    assert "FR_DSR_industrie" in df_constraints.columns

    # Coeff tertiaire = 24 * 1 / 13 = 1.84615...
    # Mean tertiaire = 100
    # Result tertiaire = 100 * 1.84615 = 184.615
    expected_tertiaire = 100 * (24 * 1 / 13)
    np.testing.assert_allclose(df_constraints["FR_DSR_tertiaire"].iloc[0], expected_tertiaire, rtol=1e-5)

    # Coeff industrie = 24 * 2 / 10 = 4.8
    # Mean industrie = 200
    # Result industrie = 200 * 4.8 = 960
    expected_industrie = 200 * (24 * 2 / 10)
    np.testing.assert_allclose(df_constraints["FR_DSR_industrie"].iloc[0], expected_industrie, rtol=1e-5)


def test_generate_contraintes_couplantes_non_fr():
    dsr_data = {"BE_DSR": {"properties": {"enabled": True}, "data": {"nb_hour_per_day": 12, "max_hour_per_day": 1}}}
    cluster_series = {"BE_DSR": pd.Series([100.0] * 8760)}

    df_constraints = generate_dsr_binding_constraints(dsr_data, cluster_series)
    assert df_constraints.shape == (365, 1)
    expected_be = 100 * (24 * 1 / 12)  # 100 * 2 = 200
    assert "BE_DSR" in df_constraints.columns
    np.testing.assert_allclose(df_constraints["BE_DSR"].iloc[0], expected_be, rtol=1e-5)
