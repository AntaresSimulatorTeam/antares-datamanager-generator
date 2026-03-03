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
from typing import List

import numpy as np

from antares.craft import Month


class SeasonManager:
    """
    A utility class to manage seasonal and monthly mapping for a 365-day year,
    starting from a configurable first month.
    """

    DAYS_IN_MONTH_JAN_TO_DEC = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    def __init__(self, first_month: Month):
        self.first_month = first_month
        self.months_list = list(first_month.__class__)
        self.first_month_idx = self.months_list.index(first_month) + 1  # 1-indexed

        self.month_order: List[int] = []
        self.days_per_month: List[int] = []
        self.month_of_day: np.ndarray = np.array([], dtype=int)

        self._initialize_mappings()

    def _initialize_mappings(self) -> None:
        """Initialize month order and days per month mappings based on first_month."""
        month_order = []
        days_per_month = []
        for i in range(12):
            m = (self.first_month_idx + i - 1) % 12 + 1
            month_order.append(m)
            days_per_month.append(self.DAYS_IN_MONTH_JAN_TO_DEC[m - 1])

        self.month_order = month_order
        self.days_per_month = days_per_month

        # Precompute the month mapping for each of the 365 days
        month_of_day_list = []
        for i in range(12):
            for _ in range(self.days_per_month[i]):
                month_of_day_list.append(self.month_order[i])
        self.month_of_day = np.array(month_of_day_list)

    def is_winter(self) -> np.ndarray:
        """
        Identify winter days.
        Winter is defined as months Jan, Feb, Mar, Oct, Nov, Dec.
        Returns a boolean array of length 365.
        """
        return (self.month_of_day <= 3) | (self.month_of_day >= 10)

    def is_summer(self) -> np.ndarray:
        """
        Identify summer days.
        Summer is defined as months Apr, May, Jun, Jul, Aug, Sep.
        Returns a boolean array of length 365.
        """
        return ~self.is_winter()

    def get_month_order(self) -> List[int]:
        """Returns the order of months (1-12) starting from first_month."""
        return self.month_order

    def get_days_per_month(self) -> List[int]:
        """Returns the number of days in each month in the order starting from first_month."""
        return self.days_per_month

    def get_month_of_day(self) -> np.ndarray:
        """Returns an array of length 365 mapping each day to its month (1-12)."""
        return self.month_of_day
