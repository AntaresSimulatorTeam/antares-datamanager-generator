import pytest

from antares.datamanager.generator.generate_link_capacity_data import generate_link_capacity_df


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
