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
from typing import Any, Dict

import numpy as np
import pandas as pd

from antares.craft import Month, ThermalClusterProperties, ThermalClusterPropertiesUpdate
from antares.craft.model.area import Area
from antares.datamanager.core.settings import settings
from antares.datamanager.exceptions.exceptions import MEGenerationError
from antares.datamanager.logs.logging_setup import get_logger
from antares.datamanager.utils.season_utils import SeasonManager

logger = get_logger(__name__)

# Constants for NPO (Number of Planned Outages) calculations
# In summer, we divide by 3 to indicate there are fewer planned outages.
# In winter, we divide by 4.
NPO_SUMMER_DIVISOR = 3
NPO_WINTER_DIVISOR = 4
MODULATION_DEF = ["marginal_cost_modulation", "market_bid_modulation", "capacity_modulation", "must_run_modulation"]


def calculate_min_stable_power(
    min_stable_power: float,
    cluster_modulation: list[str],
    base_dir: Path | None = None,
    used_files: set[Path] | None = None,
) -> Any:
    cm_file = next((f for f in cluster_modulation if "CM_" in f), None)
    if cm_file is not None:
        if base_dir is None:
            base_dir = generator_param_modulation_directory()
        cm_path = base_dir / cm_file
        if used_files is not None:
            used_files.add(cm_path)
        df_cm = pd.read_feather(cm_path)
        cm_values = df_cm.iloc[:, 0]
        logger.info(f"CM file '{cm_file}' size: {len(cm_values)}")
        min_cm_value = cm_values.min()
        return round(min_stable_power * min_cm_value, 2)
    return round(min_stable_power, 2)


def generate_thermal_clusters(
    area_obj: Area,
    thermals: dict[str, Any],
    first_month: Month,
    used_files: set[Path] | None = None,
) -> None:
    # Thermals
    for cluster_name, values in thermals.items():
        logger.info(f"Creating thermal cluster: {cluster_name}")

        cluster_modulation = values.get("modulation", {})
        modulation_matrix = create_modulation_matrix(cluster_modulation, used_files=used_files)

        create_thermal_cluster_with_prepro(
            area_obj,
            cluster_name,
            values,
            create_prepro_data_matrix,
            modulation_matrix,
            first_month,
            used_files=used_files,
        )


def create_thermal_cluster_with_prepro(
    area_obj: Area,
    cluster_name: str,
    cluster_values: dict[str, Any],
    prepro_matrix_func: Any,
    modulation_matrix: pd.DataFrame | None = None,
    first_month: Month | None = None,
    base_dir: Path | None = None,
    used_files: set[Path] | None = None,
) -> None:
    """
    Creates a thermal cluster, generates its prepro matrix, and sets it.
    """
    cluster_properties = ThermalClusterProperties(**cluster_values.get("properties", {}))

    # If cluster_properties doesn't expose attributes (e.g., patched as dict in tests),
    if not hasattr(cluster_properties, "unit_count"):
        area_obj.create_thermal_cluster(cluster_name, cluster_properties)
        return

    cluster_modulation = cluster_values.get("modulation", {})
    min_stable_power_final = calculate_min_stable_power(
        cluster_properties.min_stable_power, cluster_modulation, base_dir=base_dir, used_files=used_files
    )

    if modulation_matrix is None:
        modulation_matrix = create_modulation_matrix(cluster_modulation, base_dir=base_dir, used_files=used_files)

    cluster_data = cluster_values.get("data", {})
    unit_count = cluster_properties.unit_count
    prepro_matrix = prepro_matrix_func(cluster_data, unit_count, first_month=first_month)

    thermal_cluster = area_obj.create_thermal_cluster(cluster_name, cluster_properties)
    thermal_cluster.update_properties(ThermalClusterPropertiesUpdate(min_stable_power=min_stable_power_final))
    thermal_cluster.set_prepro_data(prepro_matrix)
    thermal_cluster.set_prepro_modulation(modulation_matrix)


def _build_npo_max_daily(
    season_manager: SeasonManager,
    unit_count: int,
    npo_max_summer: float,
    npo_max_winter: float,
    factor: float,
) -> np.ndarray[Any, np.dtype[np.float64]]:
    # Determine season using SeasonManager
    season_is_winter = season_manager.is_winter()
    season_is_summer = season_manager.is_summer()
    npo_max_daily = np.zeros(365)

    # In summer, division by NPO_SUMMER_DIVISOR and in winter by NPO_WINTER_DIVISOR
    # to indicate that there are fewer NPO (Number of Planned Outages) in summer.
    if npo_max_summer == 0:
        npo_max_daily[season_is_summer] = int(unit_count / NPO_SUMMER_DIVISOR)
    else:
        npo_max_daily[season_is_summer] = npo_max_summer * factor

    if npo_max_winter == 0:
        npo_max_daily[season_is_winter] = int(unit_count / NPO_WINTER_DIVISOR)
    else:
        npo_max_daily[season_is_winter] = npo_max_winter * factor

    return npo_max_daily


def create_prepro_data_matrix(data: Dict[str, Any], unit_count: int, first_month: Month | None = None) -> pd.DataFrame:
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
        logger.info("fo_monthly_rate or po_monthly_rate area empty skipping modulation matrix generation.")
        return pd.DataFrame()  # empty DF

    if len(fo_monthly_rate) != 12 or len(po_monthly_rate) != 12:
        raise ValueError("fo_monthly_rate and po_monthly_rate must have 12 values")

    season_manager = SeasonManager(first_month)
    month_order = season_manager.get_month_order()
    days_in_month = season_manager.get_days_per_month()

    # Build 365-day arrays directly
    fo_rate_daily = []
    po_rate_daily = []

    for i in range(12):
        month_idx_in_data = month_order[i] - 1
        for _ in range(days_in_month[i]):
            fo_rate_daily.append(fo_monthly_rate[month_idx_in_data])
            po_rate_daily.append(po_monthly_rate[month_idx_in_data])

    npo_max_daily = _build_npo_max_daily(
        season_manager=season_manager,
        unit_count=unit_count,
        npo_max_summer=npo_max_summer,
        npo_max_winter=npo_max_winter,
        factor=factor,
    )

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
    return settings.param_modulation_directory


def create_modulation_matrix(
    cluster_modulation: list[str], base_dir: Path | None = None, used_files: set[Path] | None = None
) -> pd.DataFrame:
    """
    cluster_modulation: list of filenames
    Returns a 4-column DataFrame without column names:
        [1, 1, CM_value, MR_value]

    If cluster_modulation is empty:
        returns 8760 rows of [1, 1, 1, 0]
    """
    if not cluster_modulation:
        logger.info("cluster_modulation is empty, skipping thermal modulation matrix generation.")
        data = np.tile([1, 1, 1, 0], (8760, 1))
        return pd.DataFrame(data)

    if base_dir is None:
        base_dir = generator_param_modulation_directory()

    # Detect CM and MR filenames
    cm_file = next((f for f in cluster_modulation if "CM_" in f), None)
    mr_file = next((f for f in cluster_modulation if "MR_" in f), None)

    # If both are missing, reuse existing fallback behavior
    if cm_file is None and mr_file is None:
        logger.info("No CM or MR file found, using default modulation matrix.")
        data = np.tile([1, 1, 1, 0], (8760, 1))
        return pd.DataFrame(data)

    cm_values = None
    mr_values = None

    # Read CM if present
    if cm_file is not None:
        cm_path = base_dir / cm_file
        if used_files is not None:
            used_files.add(cm_path)
        df_cm = pd.read_feather(cm_path)
        cm_values = df_cm.iloc[:, 0]
        logger.info(f"CM file '{cm_file}' size: {len(cm_values)}")

    # Read MR if present
    if mr_file is not None:
        mr_path = base_dir / mr_file
        if used_files is not None:
            used_files.add(mr_path)
        df_mr = pd.read_feather(mr_path)
        mr_values = df_mr.iloc[:, 0]
        logger.info(f"MR file '{mr_file}' size: {len(mr_values)}")

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

    logger.info(f"Final DataFrame shape: {df.shape}")
    return df


def thermal_me_directory() -> Path:
    return settings.thermal_me_directory


def thermal_me_modulation_output_directory() -> Path:
    return settings.thermal_me_modulation_output_directory


def resolve_and_validate_res_arrow_path(
    base_dir: Path | str,
    filename: str,
    allowed_extensions: tuple[str, ...] = (".arrow",),
) -> Path:
    if not isinstance(filename, str) or not filename:
        raise MEGenerationError("ME series filename must be a non-empty string")

    if not filename.endswith(".arrow"):
        raise MEGenerationError(f"Unexpected ME file extension for '{filename}', expected .arrow")

    base_resolved = Path(base_dir).resolve()
    file_path = (base_resolved / filename).resolve()

    if base_resolved != file_path and base_resolved not in file_path.parents:
        raise MEGenerationError(f"ME series path outside allowed directory: '{filename}'")

    if not file_path.exists():
        raise FileNotFoundError(f"ME series file not found: {file_path}")

    return file_path


def create_modulation_me_matrix(
    cluster_name: str,
    cluster_values: Any,
    used_files: set[Path] | None = None,
) -> pd.DataFrame:
    """
    cluster_modulation: list of modulation properties :
    {
        "marginal_cost_modulation": 1,
        "market_bid_cost_modulation": 1,
        "must_run_modulation": 1,
        "capacity_modulation": 1
    }
    Returns a 4-column DataFrame without column names:
        [MC_value, MBC_value, CM_value, MR_value]

    If cluster_modulation.prop is empty:
        # use : must_run_modulation_FE_prod_h2_central_v1.xlsx.e4a2a6a8-9c5c-4948-b20e-a24d2d6f3bf2.arrow
        returns 8760 rows of [prop.modulation.column, 1, 1, 0]
    """
    cluster_modulation = cluster_values.get("modulation", {})

    if cluster_modulation is None:
        logger.info("cluster_modulation is empty, skipping thermal modulation matrix generation.")
        data = np.tile([1, 1, 1, 0], (8760, 1))
        return pd.DataFrame(data)

    column_name = cluster_name.lower()

    cluster_series = cluster_values.get("series") or []
    mod_dir = thermal_me_modulation_output_directory()

    resolved_values = []
    for val_key in MODULATION_DEF:
        val = cluster_modulation.get(val_key)
        if val is None:
            file_name = next((s for s in cluster_series if s.startswith(val_key)), None)
            if not file_name:
                raise ValueError(f"Aucune valeur ni fichier spécifié pour '{val_key}' / '{file_name}'")

            file_path = resolve_and_validate_res_arrow_path(mod_dir, file_name)
            if used_files is not None:
                used_files.add(file_path)

            df = pd.read_feather(file_path)
            if column_name not in df.columns:
                raise ValueError(f"Colonne '{column_name}' introuvable dans '{file_path}'")

            val = df[column_name]
            logger.info(f"{val_key} file '{file_path}' size: {len(val)}")

        resolved_values.append(val)

    mc_values, mbc_values, cm_values, mr_values = resolved_values

    modulations = {
        "MC": mc_values,
        "MBC": mbc_values,
        "CM": cm_values,
        "MR": mr_values,
    }

    # 1. Identifier la longueur cible (série de fichier ou 8760 par défaut si que des scalaires)
    series_lengths = {
        name: len(val)
        for name, val in modulations.items()
        if hasattr(val, "__len__") and not isinstance(val, (str, bytes))
    }

    if series_lengths:
        # Vérifier que toutes les colonnes/fichiers fournis ont la même taille
        unique_lengths = set(series_lengths.values())
        if len(unique_lengths) > 1:
            details = ", ".join(f"{k}: {v}" for k, v in series_lengths.items())
            raise ValueError(
                f"Toutes les colonnes de modulation de fichiers doivent avoir le même nombre de lignes. Reçu : {details}"
            )
        num_rows = next(iter(unique_lengths))
    else:
        num_rows = 8760  # Valeur par défaut (nombre d'heures dans une année)

    # 2. Remplir chaque colonne (en étendant les scalaires sur num_rows)
    cols = []
    for val in [mc_values, mbc_values, cm_values, mr_values]:
        if hasattr(val, "__len__") and not isinstance(val, (str, bytes)):
            cols.append(np.asarray(val))
        else:
            cols.append(np.full(num_rows, val))

    # 3. Création du DataFrame final (matrice num_rows x 4)
    df = pd.DataFrame(np.column_stack(cols))

    logger.info(f"Final DataFrame shape: {df.shape}")
    return df


def generate_thermal_me_clusters(
    area_obj: Area,
    thermals: dict[str, Any],
    used_files: set[Path] | None = None,
) -> None:
    # Thermals
    for cluster_name, values in thermals.items():
        logger.info(f"Creating thermal ME cluster: {cluster_name}")

        modulation_matrix = create_modulation_me_matrix(cluster_name, values, used_files=used_files)

        create_thermal_me_cluster(
            area_obj,
            cluster_name,
            values,
            modulation_matrix=modulation_matrix,
            used_files=used_files,
        )


def create_thermal_me_cluster(
    area_obj: Area,
    cluster_name: str,
    cluster_values: dict[str, Any],
    modulation_matrix: pd.DataFrame | None = None,
    used_files: set[Path] | None = None,
) -> None:
    """
    Creates a thermal cluster, generates its prepro matrix, and sets it.
    """

    properties = ThermalClusterProperties(**cluster_values.get("properties", {}))

    if modulation_matrix is None:
        modulation_matrix = create_modulation_me_matrix(cluster_name, cluster_values, used_files=used_files)

    thermal_cluster = area_obj.create_thermal_cluster(cluster_name, properties)
    thermal_cluster.set_prepro_modulation(modulation_matrix)
