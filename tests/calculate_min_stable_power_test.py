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

from pathlib import Path
from unittest.mock import patch

import pandas as pd

from antares.datamanager.generator.generate_thermal_clusters import calculate_min_stable_power


def test_calculate_min_stable_power_no_cm():
    min_stable_power = 100
    cluster_modulation = ["MR_file.arrow"]
    result = calculate_min_stable_power(min_stable_power, cluster_modulation)
    assert result == 100


@patch("antares.datamanager.generator.generate_thermal_clusters.generator_param_modulation_directory")
@patch("antares.datamanager.generator.generate_thermal_clusters.pd.read_feather")
def test_calculate_min_stable_power_with_cm(mock_read_feather, mock_mod_dir):
    mock_mod_dir.return_value = Path("/fake/mod")
    mock_read_feather.return_value = pd.DataFrame({"val": [0.5, 0.2, 0.8]})

    min_stable_power = 100
    cluster_modulation = ["CM_file.arrow"]

    result = calculate_min_stable_power(min_stable_power, cluster_modulation)

    # min value is 0.2, so 100 * 0.2 = 20.0
    assert result == 20.0
    mock_read_feather.assert_called_once_with(Path("/fake/mod/CM_file.arrow"))


def test_calculate_min_stable_power_empty_modulation():
    min_stable_power = 50
    result = calculate_min_stable_power(min_stable_power, [])
    assert result == 50
