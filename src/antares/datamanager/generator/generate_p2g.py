from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from antares.craft import (
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

AREA_PREFIX = "z_p2g_"
P2G_TYPES = {"base", "marg", "methanation", "asservi"}
P2G_FATAL_BAND_PREFIX = "P2G_fatalband_"
MARKET_MODULATION_PREFIX = "MB_MC_modulation_"
BINDING_CONSTRAINT_HOURLY_ROWS = 8784
EXPECTED_HOURS = 8760


def get_mean_load_factor(res_clusters: Any) -> float:
    time_series = res_clusters.get_time_series()
    if time_series is None or time_series.empty:
        return 0.0

    return float(time_series.to_numpy().mean())


def _pad_to_binding_constraint_hourly_rows(matrix: pd.DataFrame) -> pd.DataFrame:
    if len(matrix) != EXPECTED_HOURS:
        raise P2GGenerationError(f"Expected {EXPECTED_HOURS} rows before padding, got {len(matrix)}")
    padding = pd.DataFrame(0.0, index=range(BINDING_CONSTRAINT_HOURLY_ROWS), columns=matrix.columns)
    return pd.concat([matrix, padding], ignore_index=True)


def generate_h2_profile_time_series(
    cluster_solar_pv: Any,
    cluster_wind_onshore: Any,
    capacite_pv_virtuelle: float,
    capacite_eol_virtuelle: float,
    capacite_p2g: float,
    expected_hours: int = EXPECTED_HOURS,
) -> pd.DataFrame:
    """
    Génère la série temporelle Profil_H2(t) :
        Production_ENR(t) = FC_PV(t) * Capacité_PV_virtuelle + FC_Eol(t) * Capacité_Eol_virtuelle
        Profil_H2(t) = min(Production_ENR(t), Capacité_P2G)

    Retourne un DataFrame ayant les mêmes dimensions (8760 lignes) et colonnes
    que cluster_wind_onshore.
    """
    ts_pv = cluster_solar_pv.get_time_series()
    ts_wind = cluster_wind_onshore.get_time_series()

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
    # Production_ENR(t) = FC_PV(t) * Capacité_PV_virtuelle + FC_Eol(t) * Capacité_Eol_virtuelle
    production_enr = (pv_values * float(capacite_pv_virtuelle)) + (wind_values * float(capacite_eol_virtuelle))

    # 2. Application du plafonnement Profil_H2(t) = min(Production_ENR(t), Capacité_P2G)
    profil_h2_values = np.minimum(production_enr, float(capacite_p2g))

    # 3. Reconstruction du DataFrame avec les colonnes et l'index de cluster_wind_onshore
    return pd.DataFrame(
        profil_h2_values,
        columns=ts_wind.columns,
        index=ts_wind.index,
    )


def generate_modulation_df_from_csv(
    trajectory_path: str,
    modulation_name: str,
    expected_hours: int = EXPECTED_HOURS,
) -> pd.DataFrame:
    csv_path = (settings.market_bid_modulation_directory / trajectory_path).resolve()

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
    ones = np.ones(expected_hours, dtype=np.int)
    zeros = np.zeros(expected_hours, dtype=np.int)

    data_4cols = np.column_stack([values, values, ones, zeros])

    return pd.DataFrame(data_4cols)


def generate_profil_H2(res_clusters: Any, area_link: Any, parameters: Any) -> pd.DataFrame:
    fc_electrolyseur = parameters.get("FC_electrolyseur")
    facteur_surdemension_Enr = parameters.get("Facteur_surdemension_ENR")
    part_PV_mix = parameters.get("Part_PV_mix")
    # Calcul des besoins en EnR
    capacity_p2g = area_link.get("capacity")
    production_H2_annuelle = capacity_p2g * fc_electrolyseur * EXPECTED_HOURS
    approvisionnement_ENR = production_H2_annuelle * facteur_surdemension_Enr

    # Construction d'un dictionnaire {nom: cluster}
    clusters_by_name = {c.name: c for c in res_clusters}

    cluster_solar_pv = clusters_by_name.get("solar_pv")
    cluster_wind_onshore = clusters_by_name.get("wind_onshore")

    if cluster_solar_pv is not None:
        # Cluster trouvé
        print(cluster_solar_pv.name, cluster_solar_pv.properties)
    if cluster_wind_onshore is not None:
        # Cluster trouvé
        print(cluster_wind_onshore.name, cluster_wind_onshore.properties)
    else:
        # Cluster non trouvé
        print("Cluster introuvable")

    # solar pv
    approvisionnement_PV = part_PV_mix * approvisionnement_ENR
    # moyenne du facteur de charge 1GW sur l'ensemble des années Monte Carlo.
    FC_PV_moyen = get_mean_load_factor(cluster_solar_pv)

    # wind onshore
    approvisionnement_Eol = (1 - part_PV_mix) * approvisionnement_ENR
    # moyenne du facteur de charge 1GW sur l'ensemble des années Monte Carlo.
    FC_Eol_moyen = get_mean_load_factor(cluster_wind_onshore)

    # calcul des capacités ENR virtuelles
    capacité_PV_virtuelle = approvisionnement_PV / (FC_PV_moyen * EXPECTED_HOURS)
    capacité_Eol_virtuelle = approvisionnement_Eol / (FC_Eol_moyen * EXPECTED_HOURS)
    # time serie avec production horaire
    profil_H2 = generate_h2_profile_time_series(
        cluster_solar_pv, cluster_wind_onshore, capacité_PV_virtuelle, capacité_Eol_virtuelle, capacity_p2g
    )
    return profil_H2


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
    rhs = pd.DataFrame(np.full((EXPECTED_HOURS, 1), capacity, dtype=np.float64))
    study.create_binding_constraint(
        name=str(constraint_name), properties=properties, terms=terms, greater_term_matrix=rhs
    )
    logger.info(f"Created P2G base binding constraint {constraint_name}")


def generate_p2g(study: Study, data: dict[str, Any]) -> None:
    #"""
    #Expected P2G JSON
    data = {"p2g": {
      "market_modulation": "FE60_liv1_saME/MB_MC_modulation_FE60_liv1_saME_2027.csv",
      "base": {
        "properties": {
          "nominal_capacity": 4000,
          "cost": 78.00
        },
        "modulation": "H2",
        "links": {
          "AT": { "capacity": 1500, "fatal_band": 300 },
          "BE": { "capacity": 3456, "fatal_band": 200 }
        }
      },
      "marg": {
        "properties": {
          "nominal_capacity": 5000,
          "cost": 78.00
        },
        "modulation": "Gaz",
        "links": {
          "AT": { "capacity": 250 },
          "BE": { "capacity": 356 }
        }
      },
      "methanation": {
        "properties": {
          "nominal_capacity": 3890,
          "cost": 78.00
        },
        "modulation": "Gaz",
        "links": {
          "AT": { "capacity": 300 },
          "BE": { "capacity": 400 }
        }
      },
      "asservi": {
        "properties": {
          "nominal_capacity": 2500,
          "cost": 78.00
        },
        "modulation": "H2",
        "links": {
          "AT": { "capacity": 140 },
          "BE": { "capacity": 300 }
        },
        "parameters": {
          "FC_electrolyseur": 0.5,
          "Facteur_surdemension_ENR": 1.2,
          "Part_PV_mix": 0.9
        }
      }
    }}
    #"""
    data_p2g = data.get("p2g")
    global nominal_capacity, load_series, load_capacity
    for p2g_type in P2G_TYPES or []:
        virtual_area = f"{AREA_PREFIX}{p2g_type}"
        area = study.create_area(area_name=virtual_area)

        type_data = data_p2g.get(p2g_type)
        
        if p2g_type == "asservi":
            # Création des liens et récupération de la somme des profils H2
            load_series = create_p2g_asservi_links(
                study=study,
                virtual_area=virtual_area,
                type_data=type_data,
            )
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
                nominal_capacity = float(type_data.get("properties", {}).get("nominal_capacity", 0.0))
            else:
                load_capacity = float(type_data.get("properties", {}).get("nominal_capacity", 0.0))
                nominal_capacity = load_capacity
                load_series = pd.DataFrame(np.full((EXPECTED_HOURS, 1), load_capacity, dtype=np.float64))
        area.set_load(load_series)
        cost = float(type_data.get("properties", {}).get("cost", 0.0))
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
        modulation_type = type_data.get("modulation", {})
        if modulation_type is not None:
            modulation_name = type_data.get("modulation")
            trajectory_path = data_p2g.get("market_modulation", {})
            modulation_df = generate_modulation_df_from_csv(trajectory_path=trajectory_path, modulation_name=modulation_name)
            cluster_thermal.set_prepro_modulation(modulation_df)
        logger.info(f"Created P2G virtual area {virtual_area}")


def create_p2g_links(study: Study, virtual_area: str, p2g_type: str, type_data: dict[str, Any]) -> None:
    global link_time_series
    links_data = type_data.get("links", {})
    if not links_data:
        return None
    for area_name, area_link in links_data.items():
        link_name = f"{area_name}-{virtual_area}"
        study.create_link(area_from=area_name, area_to=virtual_area)

        # Multiplier le tableau de 1 par la valeur souhaitée
        link_time_series = np.ones((EXPECTED_HOURS, 1), dtype=np.float64) * area_link.get("capacity")

        if p2g_type == "base":
            build_binding_constraint(study, area_name, area_link.get("fatal_band"))
        logger.info(f"Created P2G link {link_name}")

def create_p2g_asservi_links(study: Study, virtual_area: str, type_data: dict[str, Any]) -> pd.DataFrame:
    global link_time_series, total_profil_h2
    links_data = type_data.get("links", {})
    if not links_data:
        return None
    for area_name, area_link in links_data.items():
        link_name = f"{area_name}-{virtual_area}"
        study.create_link(area_from=area_name, area_to=virtual_area)

        # Profil H2 par lien
        # solar_pv et wind_onshore
        # Génération du profil H2 (8760 x N colonnes) pour ce pays
        area_list = study.get_areas()
        area_data = area_list[area_name.lower()]
        if area_data is None:
            continue
        res_clusters = area_data.get_renewables()
        if res_clusters is None:
            continue
        link_time_series = generate_profil_H2(
            res_clusters=res_clusters,
            area_link=area_link,
            parameters=type_data.get("parameters", {}),
        )

        # Somme matricielle des profils H2 de chaque pays
        if total_profil_h2 is None:
            total_profil_h2 = link_time_series.copy()
        else:
            total_profil_h2 = total_profil_h2 + link_time_series
        logger.info(f"Created P2G link {link_name}")
        return total_profil_h2
    return None
