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

import secrets

from typing import List, Tuple

DEFAULT_X_RANGE = (-250, 600)
DEFAULT_Y_RANGE = (-350, 300)
DEFAULT_COLOR_RANGE = (0, 255)


def generate_random_color(color_range: Tuple[int, int] = DEFAULT_COLOR_RANGE) -> List[int]:
    """Generate a random RGB color with the default range [0, 255]"""
    return [color_range[0] + secrets.randbelow(color_range[1] - color_range[0] + 1) for _ in range(3)]


def generate_random_coordinate(
    x_range: Tuple[int, int] = DEFAULT_X_RANGE, y_range: Tuple[int, int] = DEFAULT_Y_RANGE
) -> Tuple[int, int]:
    """Generate random x, y coordinates within the specified ranges"""
    x = x_range[0] + secrets.randbelow(x_range[1] - x_range[0] + 1)
    y = y_range[0] + secrets.randbelow(y_range[1] - y_range[0] + 1)
    return x, y
