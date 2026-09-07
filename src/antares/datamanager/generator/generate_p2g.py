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
from typing import Any

import numpy as np
import pandas as pd

from antares.craft import (
    AreaProperties,
    BindingConstraintFrequency,
    BindingConstraintOperator,
    BindingConstraintProperties,
    ConstraintTerm,
    LinkData,
    ThermalClusterProperties,
)
from antares.craft.model.study import Study
from antares.datamanager.core.settings import settings
from antares.datamanager.exceptions.exceptions import P2GGenerationError
from antares.datamanager.logs.logging_setup import get_logger

logger = get_logger(__name__)

AREA_PREFIX = "z_P2G_"
P2G_TYPES = {"base", "marg", "methanation", "asservi"}
P2G_FATAL_BAND_PREFIX = "P2G_fatalband_"
BINDING_CONSTRAINT_HOURLY_ROWS = 8784
EXPECTED_HOURS = 8760


def _build_area_properties(properties_json: dict[str, Any]) -> AreaProperties | None:
    if not isinstance(properties_json, dict):
        return None

    props = {}
    for key in ["energy_cost_unsupplied", "energy_cost_spilled", "adequacy_patch_mode"]:
        val = properties_json.get(key)
        if val is not None:
            props[key] = val

    return AreaProperties(**props)


def get_mean_load_factor(res_cluster: Any) -> float:
    if res_cluster is None:
        return 0.0

    time_series = res_cluster.get_timeseries()
    if time_series is None or time_series.empty:
        return 0.0

    return float(time_series.to_numpy().mean())


def generate_h2_profile_time_series(
    cluster_solar_pv: Any,
    cluster_wind_onshore: Any,
    capacity_pv_virtual: float,
    capacity_onshore_virtual: float,
    capacity_p2g: float,
    expected_hours: int = EXPECTED_HOURS,
) -> pd.DataFrame:
    """
    Génère la série temporelle Profil_H2(t) :
        Production_ENR(t) = FC_PV(t) * Capacité_PV_virtuelle + FC_Eol(t) * Capacité_Eol_virtuelle
        Profil_H2(t) = min(Production_ENR(t), Capacité_P2G)

    Retourne un DataFrame ayant les mêmes dimensions (8760 lignes) et colonnes
    que cluster_wind_onshore.
    """
    if cluster_solar_pv is None or cluster_wind_onshore is None:
        raise P2GGenerationError("Les séries temporelles de l'un des clusters EnR sont manquantes.")

    ts_pv = cluster_solar_pv.get_timeseries()
    ts_wind = cluster_wind_onshore.get_timeseries()

    if ts_pv is None or ts_wind is None:
        raise P2GGenerationError("Les séries temporelles de l'un des clusters EnR sont manquantes.")

    if len(ts_wind) != expected_hours or len(ts_pv) != expected_hours:
        raise P2GGenerationError(
            f"Les séries doivent comporter {expected_hours} lignes (reçu PV: {len(ts_pv)}, Wind: {len(ts_wind)})."
        )

    # Conversion en tableaux numpy pour s'affranchir des différences éventuelles de noms de colonnes
    pv_values = ts_pv.to_numpy()
    wind_values = ts_wind.to_numpy()

    if pv_values.shape != wind_values.shape:
        raise P2GGenerationError(
            f"Incohérence du nombre de colonnes/scénarios entre PV ({pv_values.shape}) et Éolien ({wind_values.shape})."
        )

    # 1. Calcul de Production_ENR(t)
    production_enr = (pv_values * float(capacity_pv_virtual)) + (wind_values * float(capacity_onshore_virtual))

    # 2. Application du plafonnement Profil_H2(t) = min(Production_ENR(t), Capacité_P2G)
    profile_h2_values = np.minimum(production_enr, capacity_p2g)

    # 3. Reconstruction du DataFrame avec les colonnes et l'index de cluster_wind_onshore
    return pd.DataFrame(
        profile_h2_values.round(),
        columns=ts_wind.columns,
        index=ts_wind.index,
    )


def generate_modulation_df_from_csv(
    trajectory_path: str,
    modulation_name: str,
    expected_hours: int = EXPECTED_HOURS,
) -> pd.DataFrame:
    csv_path = (settings.nas_path / trajectory_path).resolve()

    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Fichier de modulation introuvable: {path}")

    # Lecture avec détection automatique du séparateur (tabulation, virgule, point-virgule ou espaces)
    df = pd.read_csv(path, sep=r"[\t,;\s]+", engine="python")

    # Nettoyage des noms de colonnes (suppression des espaces superflus)
    df.columns = df.columns.str.strip()
    target_col = modulation_name.strip()

    if target_col not in df.columns:
        raise KeyError(
            f"La colonne '{target_col}' est introuvable dans le fichier {path.name}. "
            f"Colonnes disponibles : {list(df.columns)}"
        )

    # Extraction et conversion des valeurs en float
    values = pd.to_numeric(df[target_col], errors="coerce").to_numpy(dtype=np.float64)

    if np.isnan(values).any():
        raise ValueError(f"La colonne '{target_col}' contient des valeurs non numériques ou manquantes.")

    if len(values) != expected_hours:
        raise ValueError(
            f"Nombre de lignes incorrect pour '{target_col}' dans {path.name} : "
            f"attendu {expected_hours}, obtenu {len(values)}"
        )

    # Construction vectorisée des 4 colonnes : [valeur, valeur, 1, 0]
    ones = np.ones(expected_hours, dtype=int)
    zeros = np.zeros(expected_hours, dtype=int)

    data_4cols = np.column_stack([values, values, ones, zeros])

    return pd.DataFrame(data_4cols)


def generate_profile_hydro(res_clusters: Any, area_link: Any, parameters: Any) -> pd.DataFrame:
    if not isinstance(parameters, dict):
        parameters = {}
    if not isinstance(area_link, dict):
        area_link = {}

    fc_elec_raw = parameters.get("FC_electrolyseur")
    fc_enr_raw = parameters.get("Facteur_surdimension_ENR")
    part_pv_mix_raw = parameters.get("Part_PV_mix")
    capacity_p2g_raw = area_link.get("capacity", 0.0)

    try:
        fc_elec = float(fc_elec_raw) if fc_elec_raw is not None else 0.0
    except (ValueError, TypeError):
        fc_elec = 0.0

    try:
        fc_enr = float(fc_enr_raw) if fc_enr_raw is not None else 1.0
    except (ValueError, TypeError):
        fc_enr = 1.0

    try:
        part_pv_mix = float(part_pv_mix_raw) if part_pv_mix_raw is not None else 0.0
    except (ValueError, TypeError):
        part_pv_mix = 0.0

    try:
        capacity_p2g = float(capacity_p2g_raw) if capacity_p2g_raw is not None else 0.0
    except (ValueError, TypeError):
        capacity_p2g = 0.0

    # Calcul des besoins en EnR
    yearly_h2_production = capacity_p2g * fc_elec * EXPECTED_HOURS
    enr_supply = yearly_h2_production * fc_enr

    cluster_solar_pv = res_clusters.get("solar_pv")
    cluster_wind_onshore = res_clusters.get("wind_onshore")

    # solar pv
    solar_pv_supply = part_pv_mix * enr_supply
    # moyenne du facteur de charge 1GW sur l'ensemble des années Monte Carlo.
    fc_solar_pv_mean = get_mean_load_factor(cluster_solar_pv)

    # wind onshore
    wind_onshore_supply = (1.0 - part_pv_mix) * enr_supply
    # moyenne du facteur de charge 1GW sur l'ensemble des années Monte Carlo.
    fc_wind_onshore_mean = get_mean_load_factor(cluster_wind_onshore)

    # calcul des capacités ENR virtuelles
    if fc_solar_pv_mean > 0:
        capacity_pv_virtual = solar_pv_supply / (fc_solar_pv_mean * EXPECTED_HOURS)
    else:
        capacity_pv_virtual = 0.0

    if fc_wind_onshore_mean > 0:
        capacity_onshore_virtual = wind_onshore_supply / (fc_wind_onshore_mean * EXPECTED_HOURS)
    else:
        capacity_onshore_virtual = 0.0

    profile_h2 = generate_h2_profile_time_series(
        cluster_solar_pv=cluster_solar_pv,
        cluster_wind_onshore=cluster_wind_onshore,
        capacity_pv_virtual=capacity_pv_virtual,
        capacity_onshore_virtual=capacity_onshore_virtual,
        capacity_p2g=capacity_p2g,
    )
    return profile_h2


def compute_total_links_capacity(links_data: dict[str, Any] | None) -> float:
    if not isinstance(links_data, dict):
        return 0.0

    total = 0.0
    for country, link_info in links_data.items():
        if isinstance(link_info, dict):
            raw_capacity = link_info.get("capacity", 0.0)
            try:
                total += float(raw_capacity)
            except (ValueError, TypeError):
                # Valeur invalide ou non convertible
                continue
    return total


def build_binding_constraint(study: Study, area_name: str, capacity: float) -> None:
    properties = BindingConstraintProperties(
        enabled=True, time_step=BindingConstraintFrequency.HOURLY, operator=BindingConstraintOperator.GREATER
    )

    constraint_name = P2G_FATAL_BAND_PREFIX + area_name
    terms = [ConstraintTerm(data=LinkData(area1=area_name, area2="z_p2g_base"), weight=1)]
    rhs = pd.DataFrame(np.full((BINDING_CONSTRAINT_HOURLY_ROWS, 1), capacity, dtype=np.float64))
    study.create_binding_constraint(
        name=str(constraint_name), properties=properties, terms=terms, greater_term_matrix=rhs
    )
    logger.info(f"Created P2G base binding constraint {constraint_name}")


def generate_p2g(study: Study, data_p2g: dict[str, Any]) -> None:
    """
    Expected P2G JSON
    data = {"p2g": {
      "market_modulation": "thermal/economic parameters/market_bid_marg_cost_modulation/FE60_liv1_saME/MB_MC_modulation_FE60_liv1_saME_2027.csv",
      "base": {
        "properties": {
          "adequacy_patch_mode": AdequacyPatchMode.VIRTUAL,
          "nominal_capacity": 4000,
          "cost": 78.00
        },
        "modulation": "H2",
        "links": {
          "FR": { "capacity": 1500, "fatal_band": 300 },
          "BE": { "capacity": 3456, "fatal_band": 200 }
        }
      },
      "marg": {
        "properties": {
          "adequacy_patch_mode": AdequacyPatchMode.VIRTUAL,
          "nominal_capacity": 5000,
          "cost": 78.00
        },
        "modulation": "Gas",
        "links": {
          "FR": { "capacity": 250 },
          "BE": { "capacity": 356 }
        }
      },
      "methanation": {
        "properties": {
          "adequacy_patch_mode": AdequacyPatchMode.VIRTUAL,
          "nominal_capacity": 3890,
          "cost": 78.00
        },
        "modulation": "Gas",
        "links": {
          "FR": { "capacity": 300 },
          "BE": { "capacity": 400 }
        }
      },
      "asservi": {
        "properties": {
          "adequacy_patch_mode": AdequacyPatchMode.OUTSIDE,
          "nominal_capacity": 2500,
          "cost": 78.00
        },
        "modulation": "H2",
        "links": {
          "FR": { "capacity": 140 },
          "BE": { "capacity": 300 }
        },
        "parameters": {
          "FC_electrolyseur": 0.5,
          "Facteur_surdemension_ENR": 1.2,
          "Part_PV_mix": 0.9
        }
      }
    }}
    """

    for p2g_type in P2G_TYPES:
        type_data = data_p2g.get(p2g_type)
        if not isinstance(type_data, dict):
            continue

        virtual_area = f"{AREA_PREFIX}{p2g_type}"
        area_props = type_data.get("properties")
        nominal_capacity = area_props.get("nominal_capacity", 0.0)
        cost = area_props.get("cost", 0.0)
        area_properties = _build_area_properties(area_props)
        area = study.create_area(area_name=virtual_area, properties=area_properties)

        if p2g_type == "asservi":
            # Création des liens et récupération de la somme des profils H2
            load_series = create_p2g_asservi_links(study=study, virtual_area=virtual_area, type_data=type_data)
        else:
            # Profil de charge constant basé sur nominal_capacity (8760 x 1)
            create_p2g_links(
                study=study,
                virtual_area=virtual_area,
                p2g_type=p2g_type,
                type_data=type_data,
            )
            if p2g_type == "base":
                link_data = type_data.get("links")
                load_capacity = compute_total_links_capacity(link_data)
            else:
                load_capacity = nominal_capacity
            load_capacity_round = round(load_capacity, 0)
            load_series = pd.DataFrame(np.full((EXPECTED_HOURS, 1), load_capacity_round, dtype=np.float64))

        if load_series is not None:
            area.set_load(load_series)

        cluster_thermal = area.create_thermal_cluster(
            thermal_name=virtual_area + "_" + p2g_type,
            properties=ThermalClusterProperties(
                nominal_capacity=nominal_capacity,
                unit_count=1,
                enabled=True,
                marginal_cost=cost,
                market_bid_cost=cost,
                group="other",
            ),
        )
        modulation_type = type_data.get("modulation")
        if modulation_type is not None:
            trajectory_path = str(data_p2g.get("market_modulation", ""))
            modulation_df = generate_modulation_df_from_csv(
                trajectory_path=trajectory_path, modulation_name=modulation_type
            )
            cluster_thermal.set_prepro_modulation(modulation_df)
        logger.info(f"Created P2G virtual area {virtual_area}")


def create_p2g_links(study: Study, virtual_area: str, p2g_type: str, type_data: dict[str, Any]) -> None:
    links_data = type_data.get("links", {})
    if not isinstance(links_data, dict) or not links_data:
        return None
    for area_name, area_link in links_data.items():
        if not isinstance(area_link, dict):
            continue
        link_name = f"{area_name}-{virtual_area}"
        link = study.create_link(area_from=area_name, area_to=virtual_area)

        # Multiplier le tableau de 1 par la valeur souhaitée
        capacity = round(area_link.get("capacity", 0.0), 0)
        link_time_series = pd.DataFrame(np.full((EXPECTED_HOURS, 1), capacity, dtype=np.float64))

        if p2g_type == "base":
            build_binding_constraint(study, area_name, round(area_link.get("fatal_band", 0.0), 0))
        link.set_capacity_direct(link_time_series)
        logger.info(f"Created P2G link {link_name}")
    return None


def _combine_h2_profiles(total_profile: pd.DataFrame | None, new_profile: pd.DataFrame) -> pd.DataFrame:
    if total_profile is None:
        return new_profile.copy()
    if total_profile.columns.equals(new_profile.columns):
        return total_profile + new_profile
    return pd.DataFrame(
        total_profile.to_numpy() + new_profile.to_numpy(),
        index=total_profile.index,
        columns=total_profile.columns,
    )


def _process_single_asservi_link(
    study: Study,
    area_name: str,
    virtual_area: str,
    area_link: dict[str, Any],
    area_list: Any,
    parameters: dict[str, Any],
) -> pd.DataFrame | None:
    area_data = area_list[area_name.lower()]
    if area_data is None:
        logger.warning(f"Area {area_name} not found in study")
        return None

    res_clusters = area_data.get_renewables()
    if not res_clusters:
        logger.warning(f"No renewables found for area {area_name}")
        return None

    try:
        link_time_series = generate_profile_hydro(
            res_clusters=res_clusters,
            area_link=area_link,
            parameters=parameters,
        )
    except P2GGenerationError as e:
        logger.warning(f"Could not generate H2 profile for area {area_name}: {e}")
        return None

    if not isinstance(link_time_series, pd.DataFrame):
        link_time_series = pd.DataFrame(link_time_series)

    link = study.create_link(area_from=area_name, area_to=virtual_area)
    link.set_capacity_direct(link_time_series)
    link_name = f"{area_name}-{virtual_area}"
    logger.info(f"Created P2G link {link_name}")
    return link_time_series


def create_p2g_asservi_links(study: Study, virtual_area: str, type_data: dict[str, Any]) -> pd.DataFrame | None:
    links_data = type_data.get("links", {})
    if not isinstance(links_data, dict) or not links_data:
        return None

    area_list = study.get_areas()
    parameters = type_data.get("parameters", {})
    if not isinstance(parameters, dict):
        parameters = {}

    total_profile_h2: pd.DataFrame | None = None
    for area_name, area_link in links_data.items():
        if not isinstance(area_link, dict):
            continue

        link_profile = _process_single_asservi_link(
            study=study,
            area_name=str(area_name),
            virtual_area=virtual_area,
            area_link=area_link,
            area_list=area_list,
            parameters=parameters,
        )
        if link_profile is not None:
            total_profile_h2 = _combine_h2_profiles(total_profile_h2, link_profile)

    return total_profile_h2
