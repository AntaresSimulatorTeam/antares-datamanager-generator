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

import pandas as pd

from antares.craft import Area, STStorageProperties
from antares.datamanager.core.settings import settings

from antares.datamanager.logs.logging_setup import configure_ecs_logger, get_logger

# Configurer le logger au démarrage du module (ou appeler configure_ecs_logger() dans le main)
configure_ecs_logger()
logger = get_logger(__name__)


def generate_sts_clusters(area_obj: Area, sts: Dict[str, Any]) -> None:
    # Short-term storage clusters
    for cluster_name, values in sts.items():
        logger.info("Creating sts cluster : ", cluster_name)
        properties = values.get("properties", {})
        st_storage_properties = STStorageProperties(**properties)

        sts_series = values.get("series", [])

        storage = area_obj.create_st_storage(cluster_name, st_storage_properties)

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
            if not setter:
                continue

            file_path = base_dir / filename
            if not file_path.exists():
                raise FileNotFoundError(f"STS matrix file not found for cluster '{cluster_name}': {file_path}")

            df = pd.read_feather(file_path)

            # Column 0 = timestamps, column 1 = TS1
            if not df.empty:
                df = df.iloc[:, [1]]

            setter(df)
