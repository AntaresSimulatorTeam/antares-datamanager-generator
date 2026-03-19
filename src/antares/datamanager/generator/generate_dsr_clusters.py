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
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from antares.craft import Month, ThermalClusterProperties
from antares.craft.model.area import Area
from antares.datamanager.core.settings import settings
from antares.datamanager.logs.logging_setup import configure_ecs_logger, get_logger
from antares.datamanager.utils.season_utils import SeasonManager

configure_ecs_logger()
logger = get_logger(__name__)


def generate_dsr_clusters(area_obj: Area, dsr: Dict[str, Any], first_month: Optional[Month] = None) -> pd.DataFrame:
    """
    Generates thermal clusters for DSR (Demand Side Response) based on provided area and DSR data.
    """

    # DSR as Thermals
    base_dir = generator_dsr_modulation_directory()
    cluster_series = {}

    # 1. Collect all series to find the global max for this zone
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

    # 2. Create clusters with normalized modulation
    for cluster_name, values in dsr.items():
        logger.info(f"Creating dsr cluster: {cluster_name}")

        cluster_series_data: Optional[pd.Series[Any]] = cluster_series.get(cluster_name)
        modulation_matrix = create_dsr_modulation_matrix_from_series(cluster_series_data, global_max)

        create_dsr_cluster(area_obj, cluster_name, values, modulation_matrix, first_month)

    # 3. Generate coupling constraints
    return generate_dsr_binding_constraints(dsr, cluster_series)


def generate_dsr_binding_constraints(dsr_data: Dict[str, Any], cluster_series: Dict[str, pd.Series]) -> pd.DataFrame:
    """
    Calculates coupling constraints for DSR.

    Rules:
    1. For each day, calculate the mean of hourly values per column (capacity modulation).
    2. Coefficient per DSR node: coefficient = 24 * max_hour_per_day / nb_hour_per_day.
    3. Multiply daily mean by the coefficient.
    4. The result is a 366-day matrix.

    FR Case: Do not sum sub-clusters. Keep FR_* columns separate.
    """
    if not cluster_series:
        return pd.DataFrame()

    results = {}
    for cluster_name, series in cluster_series.items():
        data = dsr_data.get(cluster_name, {}).get("data", {})
        max_hour_per_day = data.get("max_hour_per_day", 1)
        nb_hour_per_day = data.get("nb_hour_per_day", 1)

        if nb_hour_per_day == 0:
            logger.warning(f"nb_hour_per_day is 0 for {cluster_name}, using 1 to avoid division by zero.")
            nb_hour_per_day = 1

        coefficient = 24 * max_hour_per_day / nb_hour_per_day

        daily_mean = series.groupby(series.index // 24).mean()

        # 3. Multiply by coeff
        results[cluster_name] = daily_mean * coefficient

    df_results = pd.DataFrame(results)

    fr_columns = [col for col in df_results.columns if col.startswith("FR_")]
    non_fr_columns = [col for col in df_results.columns if not col.startswith("FR_")]

    final_df = df_results[fr_columns].copy()

    if non_fr_columns:
        # If it's not a FR area, we sum all columns.
        # The column name will be used to identify the area in the constraint generation
        final_df[cluster_name] = df_results[non_fr_columns].sum(axis=1)

    logger.info(f"Generated coupling constraints matrix with shape {final_df.shape}")
    # Antares always expects 366 rows for bc_daily
    if len(final_df) == 365:
        final_df = pd.concat(
            [final_df, pd.DataFrame([[0] * final_df.shape[1]], columns=final_df.columns)], ignore_index=True
        )
    return final_df


def create_dsr_cluster(
    area_obj: Area,
    cluster_name: str,
    cluster_values: Dict[str, Any],
    modulation_matrix: pd.DataFrame,
    first_month: Optional[Month] = None,
) -> None:
    """
    Creates a DSR cluster, generates its prepro matrix, and sets it.
    """
    cluster_properties = ThermalClusterProperties(**cluster_values.get("properties", {}))

    # If cluster_properties doesn't expose attributes (e.g., patched as dict in tests),
    if not hasattr(cluster_properties, "unit_count"):
        area_obj.create_thermal_cluster(cluster_name, cluster_properties)
        return

    cluster_data = cluster_values.get("data", {})
    prepro_matrix = create_dsr_prepro_data_matrix(cluster_data, first_month=first_month)

    thermal_cluster = area_obj.create_thermal_cluster(cluster_name, cluster_properties)
    thermal_cluster.set_prepro_data(prepro_matrix)
    thermal_cluster.set_prepro_modulation(modulation_matrix)


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
    logger.info(f"Final dsr modulation matrix shape: {df.shape}")
    return df


def create_dsr_prepro_data_matrix(data: Dict[str, Any], first_month: Optional[Month] = None) -> pd.DataFrame:
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
    unit_count: int
        The number of units in the cluster.
    first_month: Optional[Month]
        The first month of the study.

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

    if not fo_monthly_rate:
        logger.info("fo_monthly_rate area empty skipping modulation matrix generation.")
        return pd.DataFrame()

    if len(fo_monthly_rate) != 12:
        raise ValueError("fo_monthly_rate must have 12 values")

    if first_month is None:
        first_month = settings.study_setting_first_month

    season_manager = SeasonManager(first_month)
    month_order = season_manager.get_month_order()
    days_in_month = season_manager.get_days_per_month()

    # Build 365-day arrays directly
    fo_rate_daily = []

    for i in range(12):
        month_idx_in_data = month_order[i] - 1
        for _ in range(days_in_month[i]):
            fo_rate_daily.append(fo_monthly_rate[month_idx_in_data])

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
