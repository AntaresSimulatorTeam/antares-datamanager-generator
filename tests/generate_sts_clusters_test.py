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
from unittest.mock import MagicMock

from antares.craft import STStorageProperties
from antares.datamanager.generator.generate_sts_clusters import generate_sts_clusters


def test_generate_sts_clusters_calls_create_st_storage():
    mock_area = MagicMock()
    sts_data = {
        "cluster1": {
            "properties": {
                "group": "battery",
                "injection_nominal_capacity": 100,
                "withdrawal_nominal_capacity": 100,
                "reservoir_capacity": 200,
                "efficiency": 0.9,
                "enabled": True,
            }
        }
    }

    generate_sts_clusters(mock_area, sts_data)

    assert mock_area.create_st_storage.call_count == 1
    call_args = mock_area.create_st_storage.call_args
    assert call_args[0][0] == "cluster1"
    props = call_args[0][1]
    assert isinstance(props, STStorageProperties)
    assert props.group == "battery"
    assert props.injection_nominal_capacity == 100
    assert props.reservoir_capacity == 200
    assert props.efficiency == 0.9
    assert props.enabled is True


def test_generate_sts_clusters_multiple_clusters():
    mock_area = MagicMock()
    sts_data = {"c1": {"properties": {"group": "battery"}}, "c2": {"properties": {"group": "pumped_storage"}}}

    generate_sts_clusters(mock_area, sts_data)

    assert mock_area.create_st_storage.call_count == 2
    names = [call[0][0] for call in mock_area.create_st_storage.call_args_list]
    assert "c1" in names
    assert "c2" in names
