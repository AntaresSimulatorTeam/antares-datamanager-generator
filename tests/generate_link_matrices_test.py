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
    season = "winter" if (day_of_year <= 90 or day_of_year >= 274) else "summer"
    period = "HP" if 8 <= hour <= 19 else "HC"
    return season, period


@pytest.mark.parametrize("index", [0, 100, 2500, 7000, 8500])
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


def test_generate_link_capacity_df_hvdc() -> None:
    """
    Test link capacity generation with HVDC data.
    """
    global_seed = 1234
    link_name = "area1-area2"
    # 1. Full HVDC case: capacity equals HVDC value -> random TS generation (60 columns)
    link_data_full_hvdc = {
        "winterHcDirectMw": 1000,
        "winterHpDirectMw": 1000,
        "summerHcDirectMw": 1000,
        "summerHpDirectMw": 1000,
        "hvdcMwDirect": 1000,
        "hvdcNbDirect": 2,
        "hvdcFoRateDirect": 0.1,
        "winterHcIndirectMw": 2000,
        "winterHpIndirectMw": 2000,
        "summerHcIndirectMw": 2000,
        "summerHpIndirectMw": 2000,
        "hvdcMwIndirect": 2000,
        "hvdcNbIndirect": 1,
        "hvdcFoRateIndirect": 0.05,
    }
    df_direct = generate_link_capacity_df(
        link_data_full_hvdc, "direct", seed_tsgen_link=global_seed, link_name=link_name
    )
    assert df_direct.shape == (8760, 60)
    # Since fo_rate is 0.1, we expect some values to be less than 1000
    assert (df_direct.values <= 1000).all()
    assert (df_direct.values >= 0).all()

    df_indirect = generate_link_capacity_df(
        link_data_full_hvdc, "indirect", seed_tsgen_link=global_seed, link_name=link_name
    )
    assert df_indirect.shape == (8760, 60)
    assert (df_indirect.values <= 2000).all()

    # 2. Hybrid case: hvac + random hvdc TS (60 columns)
    link_data_hybrid = {
        "winterHcDirectMw": 1500,
        "winterHpDirectMw": 1500,
        "summerHcDirectMw": 1500,
        "summerHpDirectMw": 1500,
        "hvdcMwDirect": 1000,
        "hvdcNbDirect": 1,
        "hvdcFoRateDirect": 0.0,  # 0.0 fo_rate means hvdc TS will be constant 1000
        "winterHcIndirectMw": 2500,
        "winterHpIndirectMw": 2500,
        "summerHcIndirectMw": 2500,
        "summerHpIndirectMw": 2500,
        "hvdcMwIndirect": 1000,
        "hvdcNbIndirect": 1,
        "hvdcFoRateIndirect": 0.0,
    }
    df_direct_hybrid = generate_link_capacity_df(
        link_data_hybrid, "direct", seed_tsgen_link=global_seed, link_name=link_name
    )
    assert df_direct_hybrid.shape == (8760, 60)
    # 1500 (total) - 1000 (hvdc) = 500 (hvac)
    # 500 (hvac) + 1000 (hvdc TS) = 1500
    assert (df_direct_hybrid.values == 1500).all()

    df_indirect_hybrid = generate_link_capacity_df(
        link_data_hybrid, "indirect", seed_tsgen_link=global_seed, link_name=link_name
    )
    assert df_indirect_hybrid.shape == (8760, 60)
    # 2500 (total) - 1000 (hvdc) = 1500 (hvac)
    # 1500 (hvac) + 1000 (hvdc TS) = 2500
    assert (df_indirect_hybrid.values == 2500).all()


def test_generate_link_capacity_df_case_insensitivity() -> None:
    """
    Test that keys in link_data can be provided in any case.
    """
    global_seed = 1234
    link_name = "area1-area2"
    link_data = {
        "WINTERHCDIRECTMW": 1000,
        "winterhpdirectmw": 1100,
        "SummerHcDirectMw": 1200,
        "SUMMERHPDIRECTMW": 1300,
        "HvdcMwDirect": 500,
        "HvdcNbDirect": 1,
        "HvdcFoRateDirect": 0.0,
    }
    df = generate_link_capacity_df(link_data, "direct", seed_tsgen_link=global_seed, link_name=link_name)

    # Check some values
    # Winter HC: (1000 - 500) + 500 = 1000
    # Winter HP: (1100 - 500) + 500 = 1100
    # Summer HC: (1200 - 500) + 500 = 1200
    # Summer HP: (1300 - 500) + 500 = 1300

    assert df.iloc[0, 0] == 1000  # Jan 1st, 00:00 -> Winter HC
    assert df.iloc[8, 0] == 1100  # Jan 1st, 08:00 -> Winter HP

    # Summer: Day 100 (April)
    assert df.iloc[100 * 24, 0] == 1200  # April, 00:00 -> Summer HC
    assert df.iloc[100 * 24 + 8, 0] == 1300  # April, 08:00 -> Summer HP
