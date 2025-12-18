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

import numpy as np
import pandas as pd

from antares.datamanager.utils.resolve_directory import resolve_directory


def create_prepro_data_matrix(data: Dict[str, Any], unit_count: int) -> pd.DataFrame:
    # If no data is provided OR if critical keys are missing, return the default 365x6 matrix
    # Critical keys: fo_duration, po_duration, npo_max_winter, npo_max_summer
    if not data or any(k not in data for k in ["fo_duration", "po_duration", "npo_max_winter", "npo_max_summer"]):
        # fo_duration, po_duration, fo_rate, po_rate, npo_min, npo_max
        return pd.DataFrame([[1, 1, 0, 0, 0, 0]] * 365)

    fo_duration_const = data.get("fo_duration", 0)
    po_duration_const = data.get("po_duration", 0)
    npo_max_winter = data.get("npo_max_winter", 0)
    npo_max_summer = data.get("npo_max_summer", 0)

    nb_unit_raw = data.get("nb_unit", 1)

    # Avoid division by zero → if nb_unit = 0, NPO_max = 0
    factor = (unit_count / nb_unit_raw) if nb_unit_raw > 0 else 0.0

    fo_monthly_rate = data.get("fo_monthly_rate", [])
    po_monthly_rate = data.get("po_monthly_rate", [])

    if not fo_monthly_rate or not po_monthly_rate:
        print("fo_monthly_rate or po_monthly_rate area empty skipping modulation matrix generation.")
        return pd.DataFrame()  # empty DF

    if len(fo_monthly_rate) != 12 or len(po_monthly_rate) != 12:
        raise ValueError("fo_monthly_rate and po_monthly_rate must have 12 values")

    # Days per month
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    # Build 365-day arrays directly
    fo_rate_daily = []
    po_rate_daily = []

    for month in range(12):
        for _ in range(days_in_month[month]):
            fo_rate_daily.append(fo_monthly_rate[month])
            po_rate_daily.append(po_monthly_rate[month])

    days = np.arange(1, 366)

    # Determine season
    season_is_winter = (days <= 90) | (days >= 305)
    season_is_summer = ~season_is_winter

    # Compute NPO_max daily safely
    npo_max_daily = np.zeros(365)
    npo_max_daily[season_is_summer] = npo_max_summer * factor
    npo_max_daily[season_is_winter] = npo_max_winter * factor

    # NPO_min always zero
    npo_min_daily = np.zeros(365)

    # Constant daily durations
    fo_duration_daily = np.full(365, fo_duration_const)
    po_duration_daily = np.full(365, po_duration_const)

    df = pd.DataFrame(
        list(
            zip(
                fo_duration_daily,
                po_duration_daily,
                fo_rate_daily,
                po_rate_daily,
                npo_min_daily,
                npo_max_daily,
            )
        )
    )

    return df


def generator_param_modulation_directory() -> Path:
    return resolve_directory("PEGASE_PARAM_MODULATION_OUTPUT_DIRECTORY")


def create_modulation_matrix(cluster_modulation: list[str]) -> pd.DataFrame:
    """
    cluster_modulation: list of filenames
    Returns a 4-column DataFrame without column names:
        [1, 1, CM_value, MR_value]

    If cluster_modulation is empty:
        returns 8760 rows of [1, 1, 1, 0]
    """
    if not cluster_modulation:
        print("cluster_modulation is empty, skipping modulation matrix generation.")
        data = np.tile([1, 1, 1, 0], (8760, 1))
        return pd.DataFrame(data)

    base_dir = generator_param_modulation_directory()

    # Detect CM and MR filenames
    cm_file = next((f for f in cluster_modulation if "CM_" in f), None)
    mr_file = next((f for f in cluster_modulation if "MR_" in f), None)

    # If both are missing, reuse existing fallback behavior
    if cm_file is None and mr_file is None:
        print("No CM or MR file found, using default modulation matrix.")
        data = np.tile([1, 1, 1, 0], (8760, 1))
        return pd.DataFrame(data)

    cm_values = None
    mr_values = None

    # Read CM if present
    if cm_file is not None:
        cm_path = base_dir / cm_file
        df_cm = pd.read_feather(cm_path)
        cm_values = df_cm.iloc[:, 0]
        print(f"CM file '{cm_file}' size: {len(cm_values)}")

    # Read MR if present
    if mr_file is not None:
        mr_path = base_dir / mr_file
        df_mr = pd.read_feather(mr_path)
        mr_values = df_mr.iloc[:, 0]
        print(f"MR file '{mr_file}' size: {len(mr_values)}")

    # If both exist, row counts must match
    if cm_values is not None and mr_values is not None:
        if len(cm_values) != len(mr_values):
            raise ValueError(
                f"CM and MR files must have the same number of rows. Got {len(cm_values)} vs {len(mr_values)}"
            )
        df = pd.DataFrame([[1, 1, cm, mr] for cm, mr in zip(cm_values, mr_values)])

    # CM missing → CM = 1
    elif cm_values is None:
        assert mr_values is not None
        df = pd.DataFrame([[1, 1, 1, mr] for mr in mr_values])

    # MR missing → MR = 0
    else:  # mr_values is None
        assert cm_values is not None
        df = pd.DataFrame([[1, 1, cm, 0] for cm in cm_values])

    print(f"Final DataFrame shape: {df.shape}")
    return df
