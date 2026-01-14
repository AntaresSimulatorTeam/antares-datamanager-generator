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

from antares.datamanager.generator.generate_link_matrices import generate_link_capacity_df, generate_link_parameters_df


@pytest.fixture
def link_data_example() -> dict[str, int]:
    return {
        # Direct values
        "winterHcDirectMw": 1300,
        "winterHpDirectMw": 1200,
        "summerHcDirectMw": 1100,
        "summerHpDirectMw": 1300,
        # Indirect values
        "winterHcIndirectMw": 1400,
        "winterHpIndirectMw": 1350,
        "summerHcIndirectMw": 1050,
        "summerHpIndirectMw": 1250,
    }


def get_season_period(index: int) -> tuple[str, str]:
    hour = index % 24
    day_of_year = (index // 24) + 1
    season = "winter" if (day_of_year <= 90 or day_of_year >= 305) else "summer"
    period = "HP" if 9 <= hour <= 20 else "HC"
    return season, period


@pytest.mark.parametrize("index", [0, 100, 2500, 8500])
@pytest.mark.parametrize("mode", ["direct", "indirect"])
def test_generate_link_capacity_data_by_index_auto_keys(
    link_data_example: dict[str, int],
    index: int,
    mode: str,
) -> None:
    """
    Check that for a given index and mode, the value in the generated DataFrame
    matches the expected value computed from the season and period.
    """

    season, period = get_season_period(index)
    key = f"{season}{period.capitalize()}{mode.capitalize()}Mw"
    expected_value = link_data_example[key]

    df = generate_link_capacity_df(link_data_example, mode=mode)
    actual_value = df.iloc[index, 0]  # First column, unnamed

    assert actual_value == expected_value, (
        f"{mode.title()} mode: At index={index} (season={season}, period={period}), "
        f"expected {expected_value} from key '{key}', but got {actual_value}"
    )


def test_array_length(link_data_example: dict[str, int]) -> None:
    """
    Confirm that the capacity DataFrame has 8760 rows for both direct and indirect modes.
    """
    df_direct = generate_link_capacity_df(link_data_example, mode="direct")
    df_indirect = generate_link_capacity_df(link_data_example, mode="indirect")

    assert len(df_direct) == 8760, "Direct DataFrame should have 8760 hours."
    assert len(df_indirect) == 8760, "Indirect DataFrame should have 8760 hours."


def test_generate_link_parameters_df_with_value() -> None:
    import pandas as pd

    hurdle = 5.5
    df = generate_link_parameters_df(hurdle)

    # Shape
    assert df.shape == (8760, 6)

    # First two columns = hurdle, last four = 0
    first_two_unique = set(pd.unique(df.iloc[:, 0:2].values.ravel()))
    last_four_unique = set(pd.unique(df.iloc[:, 2:6].values.ravel()))

    assert first_two_unique == {float(hurdle)}
    assert last_four_unique == {0.0}


def test_generate_link_parameters_df_with_none() -> None:
    df = generate_link_parameters_df(None)  # type: ignore[arg-type]

    assert df.shape == (8760, 6)
    assert float(df.values.max()) == 0.0
    assert float(df.values.min()) == 0.0


def test_generate_link_parameters_df_with_nan() -> None:
    df = generate_link_parameters_df(float("nan"))

    assert df.shape == (8760, 6)
    assert float(df.values.max()) == 0.0
    assert float(df.values.min()) == 0.0
