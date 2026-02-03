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

from antares.craft import Area, STStorageProperties


def generate_sts_clusters(area_obj: Area, sts: Dict[str, Any]) -> None:
    # Short-term storage clusters
    for cluster_name, values in sts.items():
        print(f"Creating sts cluster: {cluster_name}")
        properties = values.get("properties", {})
        st_storage_properties = STStorageProperties(**properties)
        area_obj.create_st_storage(cluster_name, st_storage_properties)
