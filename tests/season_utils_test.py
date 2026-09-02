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

import numpy as np

from antares.craft import Month
from antares.datamanager.utils.season_utils import SeasonManager


def test_season_manager_default_initialization():
    """Verify that SeasonManager defaults to Month.JULY when no month is specified."""
    manager_none = SeasonManager(None)
    manager_default = SeasonManager()

    assert manager_none.first_month == Month.JULY
    assert manager_default.first_month == Month.JULY
    assert manager_none.first_month_idx == 7
    assert manager_default.first_month_idx == 7

    # Check month order starting in July
    expected_month_order = [7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6]
    assert manager_default.get_month_order() == expected_month_order

    # Check days per month starting in July
    expected_days = [31, 31, 30, 31, 30, 31, 31, 28, 31, 30, 31, 30]
    assert manager_default.get_days_per_month() == expected_days
    assert sum(manager_default.get_days_per_month()) == 365


def test_season_manager_january_initialization():
    """Verify SeasonManager behavior when initialized with Month.JANUARY."""
    manager = SeasonManager(Month.JANUARY)

    assert manager.first_month == Month.JANUARY
    assert manager.first_month_idx == 1

    expected_month_order = list(range(1, 13))
    assert manager.get_month_order() == expected_month_order

    expected_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    assert manager.get_days_per_month() == expected_days
    assert sum(manager.get_days_per_month()) == 365


def test_season_manager_december_initialization():
    """Verify SeasonManager behavior when initialized with Month.DECEMBER."""
    manager = SeasonManager(Month.DECEMBER)

    assert manager.first_month == Month.DECEMBER
    assert manager.first_month_idx == 12

    expected_month_order = [12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    assert manager.get_month_order() == expected_month_order

    expected_days = [31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30]
    assert manager.get_days_per_month() == expected_days
    assert sum(manager.get_days_per_month()) == 365


@pytest.mark.parametrize("month_enum", list(Month))
def test_season_manager_all_months(month_enum):
    """Verify SeasonManager for every possible start month."""
    manager = SeasonManager(month_enum)

    # 1. Month order properties
    month_order = manager.get_month_order()
    assert len(month_order) == 12
    assert set(month_order) == set(range(1, 13))
    assert month_order[0] == manager.first_month_idx

    # 2. Days per month properties
    days_per_month = manager.get_days_per_month()
    assert len(days_per_month) == 12
    assert sum(days_per_month) == 365
    for m, d in zip(month_order, days_per_month):
        assert d == SeasonManager.DAYS_IN_MONTH_JAN_TO_DEC[m - 1]

    # 3. Month of day array
    month_of_day = manager.get_month_of_day()
    assert isinstance(month_of_day, np.ndarray)
    assert len(month_of_day) == 365

    # Check day counts per month match
    for m in range(1, 13):
        assert np.sum(month_of_day == m) == SeasonManager.DAYS_IN_MONTH_JAN_TO_DEC[m - 1]

    # 4. Winter / Summer masks
    is_winter = manager.is_winter()
    is_summer = manager.is_summer()

    assert isinstance(is_winter, np.ndarray)
    assert isinstance(is_summer, np.ndarray)
    assert is_winter.shape == (365,)
    assert is_summer.shape == (365,)
    assert is_winter.dtype == bool
    assert is_summer.dtype == bool

    # Check exact complementarity
    assert np.array_equal(is_summer, ~is_winter)

    # Check winter months definition: Jan (1), Feb (2), Mar (3), Oct (10), Nov (11), Dec (12)
    winter_months = {1, 2, 3, 10, 11, 12}
    expected_winter_mask = np.isin(month_of_day, list(winter_months))
    assert np.array_equal(is_winter, expected_winter_mask)

    # Total winter days: 31 + 28 + 31 + 31 + 30 + 31 = 182 days
    assert np.sum(is_winter) == 182
    # Total summer days: 30 + 31 + 30 + 31 + 31 + 30 = 183 days
    assert np.sum(is_summer) == 183


def test_season_manager_july_boundaries():
    """Verify specific day boundaries for July start (default business setting)."""
    manager = SeasonManager(Month.JULY)
    month_of_day = manager.get_month_of_day()
    is_winter = manager.is_winter()
    is_summer = manager.is_summer()

    # July 1st (index 0): Month 7, Summer
    assert month_of_day[0] == 7
    assert not is_winter[0]
    assert is_summer[0]

    # September 30th (index 91 = 31 + 31 + 30 - 1): Month 9, Summer
    assert month_of_day[91] == 9
    assert not is_winter[91]
    assert is_summer[91]

    # October 1st (index 92): Month 10, Winter
    assert month_of_day[92] == 10
    assert is_winter[92]
    assert not is_summer[92]

    # March 31st (index 273 = 92 + 31 + 30 + 31 + 31 + 28 + 31 - 1): Month 3, Winter
    assert month_of_day[273] == 3
    assert is_winter[273]
    assert not is_summer[273]

    # April 1st (index 274): Month 4, Summer
    assert month_of_day[274] == 4
    assert not is_winter[274]
    assert is_summer[274]

    # June 30th (index 364): Month 6, Summer
    assert month_of_day[364] == 6
    assert not is_winter[364]
    assert is_summer[364]


def test_season_manager_january_boundaries():
    """Verify specific day boundaries for January start."""
    manager = SeasonManager(Month.JANUARY)
    month_of_day = manager.get_month_of_day()
    is_winter = manager.is_winter()
    is_summer = manager.is_summer()

    # January 1st (index 0): Month 1, Winter
    assert month_of_day[0] == 1
    assert is_winter[0]
    assert not is_summer[0]

    # March 31st (index 89 = 31 + 28 + 31 - 1): Month 3, Winter
    assert month_of_day[89] == 3
    assert is_winter[89]
    assert not is_summer[89]

    # April 1st (index 90): Month 4, Summer
    assert month_of_day[90] == 4
    assert not is_winter[90]
    assert is_summer[90]

    # September 30th (index 272 = 90 + 30 + 31 + 30 + 31 + 31 + 30 - 1): Month 9, Summer
    assert month_of_day[272] == 9
    assert not is_winter[272]
    assert is_summer[272]

    # October 1st (index 273): Month 10, Winter
    assert month_of_day[273] == 10
    assert is_winter[273]
    assert not is_summer[273]

    # December 31st (index 364): Month 12, Winter
    assert month_of_day[364] == 12
    assert is_winter[364]
    assert not is_summer[364]
