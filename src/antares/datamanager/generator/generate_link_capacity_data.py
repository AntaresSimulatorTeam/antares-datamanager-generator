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


def generate_link_capacity_df(link_data: dict[str, int], mode: str) -> pd.DataFrame:
    """
    Generation of df for direct and indirect links capacity,
    in first version HP (Peak hours) are defined as 9h to 20h all
    other hours are HC (Off-peak hours)
    Total number of rows is 8760
    Winter is defined as Jan, Feb, Mar, Nov and Dec (days 0 - 90 and 305 - 635)
    Summer is defined as Apr, Jun, Jul, Aug, Sep, Oct
    :param link_data: Mw value to use for different periods of year
    :param mode: direct or indirect
    :return: df corresponding to direct or indirect parameter
    """
    total_hours = 8760
    indices = np.arange(total_hours)
    hours = indices % 24
    day_of_year = (indices // 24) + 1
    seasons = np.where((day_of_year <= 90) | (day_of_year >= 305), "winter", "summer")
    periods = np.where((hours >= 9) & (hours <= 20), "HP", "HC")

    if mode.lower() == "direct":
        winter_hc_value = link_data["winterHcDirectMw"]
        winter_hp_value = link_data["winterHpDirectMw"]
        summer_hc_value = link_data["summerHcDirectMw"]
        summer_hp_value = link_data["summerHpDirectMw"]
    elif mode.lower() == "indirect":
        winter_hc_value = link_data["winterHcIndirectMw"]
        winter_hp_value = link_data["winterHpIndirectMw"]
        summer_hc_value = link_data["summerHcIndirectMw"]
        summer_hp_value = link_data["summerHpIndirectMw"]
    else:
        raise ValueError("Mode must be either 'direct' or 'indirect'")

    capacity = np.empty(total_hours, dtype=int)
    winter_hc_mask = (seasons == "winter") & (periods == "HC")
    winter_hp_mask = (seasons == "winter") & (periods == "HP")
    summer_hc_mask = (seasons == "summer") & (periods == "HC")
    summer_hp_mask = (seasons == "summer") & (periods == "HP")

    capacity[winter_hc_mask] = winter_hc_value
    capacity[winter_hp_mask] = winter_hp_value
    capacity[summer_hc_mask] = summer_hc_value
    capacity[summer_hp_mask] = summer_hp_value

    df = pd.DataFrame(capacity)

    return df
