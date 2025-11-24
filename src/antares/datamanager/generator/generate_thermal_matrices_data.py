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
from typing import Any, Dict

import numpy as np
import pandas as pd


def create_prepro_data_matrix(data: Dict[str, Any], unit_count: int) -> pd.DataFrame:
    fo_duration_const = data.get("fo_duration", 0)
    po_duration_const = data.get("po_duration", 0)
    npo_max_winter = data.get("npo_max_winter", 0)
    npo_max_summer = data.get("npo_max_summer", 0)

    nb_unit_raw = data.get("nb_unit", 1)

    # Avoid division by zero → if nb_unit = 0, NPO_max = 0
    factor = (unit_count / nb_unit_raw) if nb_unit_raw > 0 else 0.0

    fo_monthly_rate = data.get("fo_monthly_rate", [])
    po_monthly_rate = data.get("po_monthly_rate", [])

    if len(fo_monthly_rate) != 12 or len(po_monthly_rate) != 12:
        raise ValueError("fo_monthly_rate and po_monthly_rate must have 12 values")

    # Days per month
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    # Build 365-day arrays directly
    fo_rate_daily = []
    po_rate_daily = []

    for month in range(12):
        for _ in range(days_in_month[month]):
            fo_rate_daily.append(fo_monthly_rate[month])
            po_rate_daily.append(po_monthly_rate[month])

    days = np.arange(1, 366)

    # Determine season
    season_is_winter = (days <= 90) | (days >= 305)
    season_is_summer = ~season_is_winter

    # Compute NPO_max daily safely
    npo_max_daily = np.zeros(365)
    npo_max_daily[season_is_summer] = npo_max_summer * factor
    npo_max_daily[season_is_winter] = npo_max_winter * factor

    # NPO_min always zero
    npo_min_daily = np.zeros(365)

    # Constant daily durations
    fo_duration_daily = np.full(365, fo_duration_const)
    po_duration_daily = np.full(365, po_duration_const)

    df = pd.DataFrame(
        list(
            zip(
                fo_duration_daily,
                po_duration_daily,
                fo_rate_daily,
                po_rate_daily,
                npo_min_daily,
                npo_max_daily,
            )
        )
    )

    return df
