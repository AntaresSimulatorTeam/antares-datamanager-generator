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
    nb_unit = data.get("nb_unit", 1)

    fo_monthly_rate = data.get("fo_monthly_rate", [])
    po_monthly_rate = data.get("po_monthly_rate", [])

    if len(fo_monthly_rate) != 12 or len(po_monthly_rate) != 12:
        raise ValueError("fo_monthly_rate and po_monthly_rate must have 12 values")

    # Days per month
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    fo_rate = []
    po_rate = []

    # Expand the monthly rate to HOURS (month_days × 24)
    for m in range(12):
        hours = days_in_month[m] * 24
        fo_rate.extend([fo_monthly_rate[m]] * hours)
        po_rate.extend([po_monthly_rate[m]] * hours)

    # ensure exactly 8760 hours (normal year)
    fo_rate = fo_rate[:8760]
    po_rate = po_rate[:8760]

    # Constant columns
    fo_duration = [fo_duration_const] * 8760
    po_duration = [po_duration_const] * 8760
    total_hours = 8760
    indices = np.arange(total_hours)

    day_of_year = (indices // 24) + 1

    # Determine season
    season_is_winter = (day_of_year <= 90) | (day_of_year >= 305)
    season_is_summer = ~season_is_winter

    # Allocate vector
    npo_max = np.zeros(total_hours)

    # Summer
    npo_max[season_is_summer] = npo_max_summer * unit_count / nb_unit

    # Winter
    npo_max[season_is_winter] = npo_max_winter * unit_count / nb_unit
    npo_min = [0] * 8760
    # Build DataFrame WITHOUT column names
    df = pd.DataFrame(list(zip(fo_duration, po_duration, fo_rate, po_rate, npo_min, npo_max)))

    return df
