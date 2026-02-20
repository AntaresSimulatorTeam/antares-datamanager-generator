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

from antares.craft.model.area import Area
from antares.datamanager.core.settings import settings
from antares.datamanager.generator.generate_thermal_clusters import (
    create_thermal_cluster_with_prepro,
)
from antares.datamanager.logs.logging_setup import configure_ecs_logger, get_logger

configure_ecs_logger()
logger = get_logger(__name__)


def generate_dsr_clusters(area_obj: Area, dsr: Dict[str, Any]) -> None:
    """
    Generates thermal clusters for DSR (Demand Side Response) based on provided area and DSR data.
    """

    # DSR as Thermals
    base_dir = generator_dsr_modulation_directory()
    cluster_series = {}

    # 1. First pass: Collect all series to find the global max for this zone
    for cluster_name, values in dsr.items():
        cluster_modulation = values.get("modulation", [])
        if not cluster_modulation:
            continue

        cm_file = next((f for f in cluster_modulation if "CM_" in f), None)
        if cm_file:
            cm_path = base_dir / cm_file
            if cm_path.exists():
                df_cm = pd.read_feather(cm_path)
                series = df_cm.iloc[:, 0]
                cluster_series[cluster_name] = series
            else:
                logger.warning(f"DSR CM file '{cm_file}' not found at {cm_path}")

    # Global max of the SUM of all DSR capacities in this zone
    global_max = 0
    if cluster_series:
        # Sum all series element-wise to get the total DSR capacity at each hour
        total_series = pd.concat(cluster_series.values(), axis=1).sum(axis=1)
        global_max = total_series.max()

    # 2. Second pass: Create clusters with normalized modulation
    for cluster_name, values in dsr.items():
        logger.info(f"Creating dsr cluster: {cluster_name}")

        cluster_series_data: "pd.Series[Any] | None" = cluster_series.get(cluster_name)
        modulation_matrix = create_dsr_modulation_matrix_from_series(cluster_series_data, global_max)

        create_thermal_cluster_with_prepro(
            area_obj, cluster_name, values, create_dsr_prepro_data_matrix, modulation_matrix
        )


def generator_dsr_modulation_directory() -> Path:
    return settings.dsr_modulation_directory


def create_dsr_modulation_matrix_from_series(series: "pd.Series[Any] | None", global_max: float) -> pd.DataFrame:
    """
    Returns a 4-column DataFrame without column names:
        [1, 1, capacity_modulation, 0]

    If series is None:
        returns 8760 rows of [1, 1, 1, 0]
    """
    if series is None:
        logger.info("DSR modulation series is None, skipping dsr modulation matrix generation.")
        data = np.tile([1, 1, 1, 0], (8760, 1))
        return pd.DataFrame(data)

    # cm_values = np.ones(len(series))
    if global_max > 0:
        cm_values = (series / global_max).round(3)
    else:
        cm_values = series.round(3)

    df = pd.DataFrame([[1, 1, cm, 0] for cm in cm_values])
    logger.info(f"Final DataFrame shape: {df.shape}")
    return df


def create_dsr_prepro_data_matrix(data: Dict[str, Any], unit_count: int) -> pd.DataFrame:
    """
    Generates a data matrix for preprocessed DSR values.

    This function computes a 365x6 pandas DataFrame based on provided data and default
    values for missing data. If no data input is provided,
    it returns a default 365x6 matrix initialized with predefined constant values.

    Parameters:
    data: Dict[str, Any]
        A dictionary of input data containing parameters like durations and monthly rates.
        Mandatory keys include:
            - "fo_duration" (int): The constant duration for the first operation.
            - "fo_monthly_rate" (List[float]): Per-month rate for the first operation (12 values).
            If these keys are not provided, default values are used.

    Returns:
    pd.DataFrame
        A pandas DataFrame with shape (365, 6). Columns represent the following:
            - fo_duration_daily: Daily duration for the first operation.
            - po_duration: default: 1.
            - fo_rate_daily: Daily rate for the first operation, derived from monthly rates.
            - po_rate: default: 0
            - npo_min: default: 0.
            - npo_max: default: 0.
    """
    # If no data is provided  return the default 365x6 matrix
    if not data:
        # fo_duration, po_duration, fo_rate, po_rate, npo_min, npo_max
        return pd.DataFrame([[1, 1, 0, 0, 0, 0]] * 365)

    fo_duration_const = data.get("fo_duration", 1)
    fo_monthly_rate = data.get("fo_monthly_rate", [])

    # Days per month
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    # Build 365-day arrays directly
    fo_rate_daily = []

    for month in range(12):
        for _ in range(days_in_month[month]):
            fo_rate_daily.append(fo_monthly_rate[month])

    # Constant daily durations
    fo_duration_daily = np.full(365, fo_duration_const)
    po_duration = np.full(365, 1)
    po_rate = np.zeros(365)
    npo_min = np.zeros(365)
    npo_max = np.zeros(365)

    df = pd.DataFrame(
        list(
            zip(
                fo_duration_daily,
                po_duration,
                fo_rate_daily,
                po_rate,
                npo_min,
                npo_max,
            )
        )
    )

    return df
