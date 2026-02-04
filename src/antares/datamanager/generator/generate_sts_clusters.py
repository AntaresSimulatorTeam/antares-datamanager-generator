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
from typing import Any, Dict

import pandas as pd

from antares.craft import Area, STStorageProperties
from antares.datamanager.core.settings import settings


def generate_sts_clusters(area_obj: Area, sts: Dict[str, Any]) -> None:
    # Short-term storage clusters
    for cluster_name, values in sts.items():
        print(f"Creating sts cluster: {cluster_name}")
        properties = values.get("properties", {})
        st_storage_properties = STStorageProperties(**properties)

        sts_series = values.get("series", [])

        storage = area_obj.create_st_storage(cluster_name, st_storage_properties)

        # Mapping of arrow file prefixes to their corresponding setter methods in antares-craft
        # Filename example: inflows.xlsx.f185b056-c144-445f-9dd1-34f92c6138c9.arrow
        matrix_setter_map = {
            "inflows": storage.set_storage_inflows,
            "lower_curve": storage.set_lower_rule_curve,
            "Pmax_injection": storage.update_pmax_injection,
            "Pmax_soutirage": storage.set_pmax_withdrawal,
            "upper_curve": storage.set_upper_rule_curve,
        }

        base_dir = settings.sts_ts_directory
        for filename in sts_series:
            prefix = filename.split(".")[0]
            setter = matrix_setter_map.get(prefix)
            if setter:
                file_path = base_dir / filename
                if file_path.exists():
                    df = pd.read_feather(file_path)
                    # Use the first column of the matrix (it corresponds to TS1)
                    if not df.empty:
                        df = df.iloc[:, [0]]
                    setter(df)
                else:
                    print(f"Warning: STS matrix file not found: {file_path}")