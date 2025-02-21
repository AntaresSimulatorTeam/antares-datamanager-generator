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

import random

from typing import List, Tuple

DEFAULT_X_RANGE = (-250, 600)
DEFAULT_Y_RANGE = (-350, 300)
DEFAULT_COLOR_RANGE = (0, 255)


def generate_random_color(color_range: Tuple[int, int] = DEFAULT_COLOR_RANGE) -> List[int]:
    """Generate a random RGB color with default range [0, 255]"""
    return [random.randint(color_range[0], color_range[1]) for _ in range(3)]


def generate_random_coordinate(
    x_range: Tuple[int, int] = DEFAULT_X_RANGE, y_range: Tuple[int, int] = DEFAULT_Y_RANGE
) -> Tuple[int, int]:
    """Generate random x, y coordinates within the specified ranges"""
    x = random.randint(*x_range)
    y = random.randint(*y_range)
    return x, y
