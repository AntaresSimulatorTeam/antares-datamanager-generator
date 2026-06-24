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

from typing import Any

import numpy as np
import pandas as pd

from antares.datamanager.core.settings import settings
from antares.datamanager.utils.seed_factory import SeedFactory
from antares.tsgen.duration_generator import ProbabilityLaw
from antares.tsgen.random_generator import MersenneTwisterRNG
from antares.tsgen.ts_generator import LinkCapacity, OutageGenerationParameters, TimeseriesGenerator


def _generate_hvdc_ts(link_data_lower: dict[str, Any], mode: str, seed_tsgen_link: int, link_name: str) -> pd.DataFrame:
    """
    Generate random time series for 100% HVDC links.
    """
    prefix = mode.lower()
    mw_key = f"hvdcmw{prefix}"
    nb_key = f"hvdcnb{prefix}"
    fo_rate_key = f"hvdcforate{prefix}"

    hvdc_mw = link_data_lower.get(mw_key, 0)
    hvdc_nb = link_data_lower.get(nb_key, 1)
    hvdc_fo_rate = link_data_lower.get(fo_rate_key, 0)

    # outage generation parameters
    # fo_rate, po_rate, fo_duration, po_duration, npo_min, npo_max are indexed by day of year (365)
    days = 365
    fo_rate_array = np.full(days, float(hvdc_fo_rate))
    zeros_float = np.zeros(days, dtype=float)
    ones_int = np.ones(days, dtype=int)
    zeros_int = np.zeros(days, dtype=int)

    outage_params = OutageGenerationParameters(
        unit_count=int(hvdc_nb),
        fo_duration=ones_int,  # must be > 0
        fo_rate=fo_rate_array,
        po_duration=ones_int,  # must be > 0
        po_rate=zeros_float,
        npo_min=zeros_int,
        npo_max=zeros_int,
        fo_law=ProbabilityLaw.UNIFORM,
        fo_volatility=0.0,
        po_law=ProbabilityLaw.UNIFORM,
        po_volatility=0.0,
    )

    # nominal capacity is total capacity / number of units
    nominal_cap = float(hvdc_mw) / float(hvdc_nb) if hvdc_nb > 0 else 0.0

    # modulation is a matrix of 1 (hourly: 8760)
    modulation = np.ones(8760, dtype=float)

    link_capacity = LinkCapacity(
        outage_gen_params=outage_params,
        nominal_capacity=nominal_cap,
        modulation_direct=modulation,
        modulation_indirect=modulation,
    )

    seed_int = SeedFactory.for_timeseries(seed_tsgen_link, link_name)
    rng = MersenneTwisterRNG(seed_int)

    ts_generator = TimeseriesGenerator(rng=rng)

    link_output = ts_generator.generate_time_series_for_links(
        link_capacity, number_of_timeseries=settings.number_of_timeseries
    )

    if prefix == "direct":
        data = link_output.direct_available_power
    else:
        data = link_output.indirect_available_power

    return pd.DataFrame(data)


def generate_link_capacity_df(
    link_data: dict[str, int], mode: str, seed_tsgen_link: int = 0, link_name: str = ""
) -> pd.DataFrame:
    """
    Generate a DataFrame representing link capacity based on input parameters.

    This function generates a time-series DataFrame that defines the capacity
    of a link over a year (8760 hours) based on the input dictionary `link_data`,
    the operation mode (`direct` or `indirect`), and other optional parameters.
    It distinguishes between summer and winter, as well as low consumption (HC - Heures Creuses)
    and high-consumption (HP - Heures Pleines) periods, to assign appropriate capacity values to
    each hour. Additionally, it supports handling HVDC (High Voltage Direct Current)
    links, either independently or in combination with HVAC (High Voltage Alternating Current).

    Parameters:
        link_data (dict[str, int]): A case-insensitive dictionary containing power
            values for different seasons, periods, and optionally HVDC. Example keys
            include "winterhcdirectmw", "summerhpindirectmw".
        mode (str): The operation mode of the link can be either "direct" or "indirect".
        seed_tsgen_link (int, optional): Seed value for generating HVDC time-series if
            applicable. Defaults to 0.
        link_name (str, optional): An identifier for the link, used for generating
            HVDC time-series if applicable. Defaults to an empty string.

    Returns:
        pd.DataFrame: A DataFrame representing the link capacity over 8760 hours
            (columns indicate periods if HVDC is not fully utilized). For mixed
            HVAC and HVDC setups, the returned DataFrame incorporates both.

    Raises:
        ValueError: If the `mode` argument is not "direct" or "indirect".
    """
    is_full_hvdc = False
    hvdc_ts = None
    total_hours = 8760
    indices = np.arange(total_hours)
    hours = indices % 24
    day_of_year = (indices // 24) + 1
    seasons = np.where((day_of_year <= 90) | (day_of_year >= 274), "winter", "summer")
    periods = np.where((hours >= 8) & (hours <= 19), "HP", "HC")

    # Make link_data case-insensitive by creating a lowercase copy
    link_data_lower = {k.lower(): v for k, v in link_data.items()}

    if mode.lower() == "direct":
        winter_hc_value = link_data_lower["winterhcdirectmw"]
        winter_hp_value = link_data_lower["winterhpdirectmw"]
        summer_hc_value = link_data_lower["summerhcdirectmw"]
        summer_hp_value = link_data_lower["summerhpdirectmw"]
        hvdc_mw = link_data_lower.get("hvdcmwdirect")
    elif mode.lower() == "indirect":
        winter_hc_value = link_data_lower["winterhcindirectmw"]
        winter_hp_value = link_data_lower["winterhpindirectmw"]
        summer_hc_value = link_data_lower["summerhcindirectmw"]
        summer_hp_value = link_data_lower["summerhpindirectmw"]
        hvdc_mw = link_data_lower.get("hvdcmwindirect")
    else:
        raise ValueError("Mode must be either 'direct' or 'indirect'")

    if hvdc_mw is not None:
        is_full_hvdc = (
            winter_hc_value == hvdc_mw
            and winter_hp_value == hvdc_mw
            and summer_hc_value == hvdc_mw
            and summer_hp_value == hvdc_mw
        )
        if is_full_hvdc:
            return _generate_hvdc_ts(link_data_lower, mode, seed_tsgen_link, link_name)
        else:
            hvdc_ts = _generate_hvdc_ts(link_data_lower, mode, seed_tsgen_link, link_name)
            winter_hc_value -= hvdc_mw
            winter_hp_value -= hvdc_mw
            summer_hc_value -= hvdc_mw
            summer_hp_value -= hvdc_mw

    capacity = np.empty(total_hours, dtype=int)
    winter_hc_mask = (seasons == "winter") & (periods == "HC")
    winter_hp_mask = (seasons == "winter") & (periods == "HP")
    summer_hc_mask = (seasons == "summer") & (periods == "HC")
    summer_hp_mask = (seasons == "summer") & (periods == "HP")

    capacity[winter_hc_mask] = winter_hc_value
    capacity[winter_hp_mask] = winter_hp_value
    capacity[summer_hc_mask] = summer_hc_value
    capacity[summer_hp_mask] = summer_hp_value

    hvac_ts = pd.DataFrame(capacity)

    if hvdc_mw is not None and not is_full_hvdc:
        # Sum hvac_ts (1 column) to each column of hvdc_ts (60 columns)
        # Using .values[0] or broadcasting
        assert hvdc_ts is not None
        return hvdc_ts.add(hvac_ts.values, axis=0)

    return hvac_ts


def generate_link_parameters_df(hurdle_cost: float) -> pd.DataFrame:
    """
    Generate a DataFrame for link parameters.

    When hurdleCost is provided (not None/NaN), return a DataFrame with 8760 rows
    and 6 unnamed columns where:
      - the first two columns are filled with hurdleCost value
      - the remaining four columns are filled with 0

    If hurdleCost is None or NaN, a DataFrame of the same shape filled with zeros is returned.

    Note: Although the signature expects a float, callers may pass a dict containing
    the key "hurdleCost". This function supports that pattern for robustness.
    """
    total_hours = 8760

    # Handle None/NaN as missing value → use zeros
    if hurdle_cost is None or pd.isna(hurdle_cost):
        first_two = np.zeros((total_hours, 2), dtype=float)
    else:
        first_two = np.full((total_hours, 2), float(hurdle_cost))

    last_four = np.zeros((total_hours, 4), dtype=float)

    data = np.concatenate([first_two, last_four], axis=1)
    df = pd.DataFrame(data)
    return df
