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

import pytest

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from antares.craft import (
    AdequacyPatchMode,
    AreaProperties,
    BindingConstraintFrequency,
    BindingConstraintOperator,
    LinkData,
    Study,
)
from antares.craft.model.renewable import RenewableCluster
from antares.datamanager.exceptions.exceptions import P2GGenerationError
from antares.datamanager.generator.generate_p2g import (
    AREA_PREFIX,
    BINDING_CONSTRAINT_HOURLY_ROWS,
    EXPECTED_HOURS,
    P2G_FATAL_BAND_PREFIX,
    P2G_TYPES,
    _build_area_properties,
    build_binding_constraint,
    compute_total_links_capacity,
    create_p2g_asservi_links,
    create_p2g_links,
    generate_h2_profile_time_series,
    generate_modulation_df_from_csv,
    generate_p2g,
    generate_profile_hydro,
    get_mean_load_factor,
)


def _make_mock_res_cluster(time_series: pd.DataFrame | None, name: str = "cluster") -> MagicMock:
    cluster = MagicMock(spec=RenewableCluster)
    cluster.name = name
    cluster.properties = {}
    cluster.get_timeseries.return_value = time_series
    return cluster


# ---------------------------------------------------------------------------
# Tests: _build_area_properties
# ---------------------------------------------------------------------------


def test_build_area_properties():
    assert _build_area_properties(None) is None
    assert _build_area_properties("not_a_dict") is None

    empty_props = _build_area_properties({})
    assert isinstance(empty_props, AreaProperties)

    props_dict = {
        "energy_cost_unsupplied": 3000.0,
        "energy_cost_spilled": 0.0,
        "adequacy_patch_mode": AdequacyPatchMode.VIRTUAL,
        "nominal_capacity": 4000,
        "cost": 78.0,
    }
    props = _build_area_properties(props_dict)
    assert isinstance(props, AreaProperties)
    assert props.energy_cost_unsupplied == 3000.0
    assert props.energy_cost_spilled == 0.0
    assert props.adequacy_patch_mode == AdequacyPatchMode.VIRTUAL


# ---------------------------------------------------------------------------
# Tests: get_mean_load_factor
# ---------------------------------------------------------------------------


def test_get_mean_load_factor_none_or_empty():
    assert get_mean_load_factor(None) == 0.0

    cluster_none = _make_mock_res_cluster(None)
    assert get_mean_load_factor(cluster_none) == 0.0

    cluster_empty = _make_mock_res_cluster(pd.DataFrame())
    assert get_mean_load_factor(cluster_empty) == 0.0


def test_get_mean_load_factor_valid():
    df = pd.DataFrame({"col1": [0.2, 0.4], "col2": [0.6, 0.8]})
    cluster = _make_mock_res_cluster(df)
    assert pytest.approx(get_mean_load_factor(cluster)) == 0.5


# ---------------------------------------------------------------------------
# Tests: generate_h2_profile_time_series
# ---------------------------------------------------------------------------


def test_generate_h2_profile_time_series_missing_ts():
    cluster_pv_valid = _make_mock_res_cluster(pd.DataFrame(np.ones((EXPECTED_HOURS, 1))))
    cluster_wind_valid = _make_mock_res_cluster(pd.DataFrame(np.ones((EXPECTED_HOURS, 1))))

    with pytest.raises(P2GGenerationError, match="manquantes"):
        generate_h2_profile_time_series(None, cluster_wind_valid, 100.0, 100.0, 50.0)

    with pytest.raises(P2GGenerationError, match="manquantes"):
        generate_h2_profile_time_series(cluster_pv_valid, None, 100.0, 100.0, 50.0)

    cluster_pv = _make_mock_res_cluster(None)
    cluster_wind = _make_mock_res_cluster(pd.DataFrame(np.ones((EXPECTED_HOURS, 1))))

    with pytest.raises(P2GGenerationError, match="manquantes"):
        generate_h2_profile_time_series(cluster_pv, cluster_wind, 100.0, 100.0, 50.0)

    cluster_wind_none = _make_mock_res_cluster(None)

    with pytest.raises(P2GGenerationError, match="manquantes"):
        generate_h2_profile_time_series(cluster_pv_valid, cluster_wind_none, 100.0, 100.0, 50.0)


def test_generate_h2_profile_time_series_invalid_hours():
    df_short = pd.DataFrame(np.ones((100, 1)))
    cluster_pv = _make_mock_res_cluster(df_short)
    cluster_wind = _make_mock_res_cluster(df_short)

    with pytest.raises(P2GGenerationError, match=f"doivent comporter {EXPECTED_HOURS} lignes"):
        generate_h2_profile_time_series(cluster_pv, cluster_wind, 100.0, 100.0, 50.0)


def test_generate_h2_profile_time_series_shape_mismatch():
    df_pv = pd.DataFrame(np.ones((EXPECTED_HOURS, 1)))
    df_wind = pd.DataFrame(np.ones((EXPECTED_HOURS, 2)))
    cluster_pv = _make_mock_res_cluster(df_pv)
    cluster_wind = _make_mock_res_cluster(df_wind)

    with pytest.raises(P2GGenerationError, match="Incohérence du nombre de colonnes/scénarios"):
        generate_h2_profile_time_series(cluster_pv, cluster_wind, 100.0, 100.0, 50.0)


def test_generate_h2_profile_time_series_success():
    # PV: 0.5, Wind: 0.5, Cap PV: 100, Cap Wind: 200 -> Prod = 50 + 100 = 150
    # Capacity P2G = 120 -> Plafonnement à 120
    df_pv = pd.DataFrame({"sc_1": [0.5] * EXPECTED_HOURS, "sc_2": [0.1] * EXPECTED_HOURS})
    df_wind = pd.DataFrame({"sc_1": [0.5] * EXPECTED_HOURS, "sc_2": [0.2] * EXPECTED_HOURS})
    cluster_pv = _make_mock_res_cluster(df_pv)
    cluster_wind = _make_mock_res_cluster(df_wind)

    result = generate_h2_profile_time_series(
        cluster_solar_pv=cluster_pv,
        cluster_wind_onshore=cluster_wind,
        capacity_pv_virtual=100.0,
        capacity_onshore_virtual=200.0,
        capacity_p2g=120.0,
    )

    assert result.shape == (EXPECTED_HOURS, 2)
    assert list(result.columns) == ["sc_1", "sc_2"]
    # sc_1: min(0.5*100 + 0.5*200, 120) = min(150, 120) = 120
    assert np.allclose(result["sc_1"].to_numpy(), 120.0)
    # sc_2: min(0.1*100 + 0.2*200, 120) = min(50, 120) = 50
    assert np.allclose(result["sc_2"].to_numpy(), 50.0)


# ---------------------------------------------------------------------------
# Tests: generate_modulation_df_from_csv
# ---------------------------------------------------------------------------


def test_generate_modulation_df_from_csv_file_not_found(tmp_path):
    with patch("antares.datamanager.generator.generate_p2g.settings") as mock_settings:
        mock_settings.trajectory_input_path = tmp_path
        with pytest.raises(FileNotFoundError, match="Fichier de modulation introuvable"):
            generate_modulation_df_from_csv("missing_file.csv", "H2")


def test_generate_modulation_df_from_csv_missing_column(tmp_path):
    csv_file = tmp_path / "mod.csv"
    csv_file.write_text("Gas\tOther\n" + "1.0\t2.0\n" * EXPECTED_HOURS)

    with patch("antares.datamanager.generator.generate_p2g.settings") as mock_settings:
        mock_settings.trajectory_input_path = tmp_path
        with pytest.raises(KeyError, match="introuvable dans le fichier"):
            generate_modulation_df_from_csv("mod.csv", "H2")


def test_generate_modulation_df_from_csv_non_numeric(tmp_path):
    csv_file = tmp_path / "mod.csv"
    csv_file.write_text("H2\n" + "invalid\n" * EXPECTED_HOURS)

    with patch("antares.datamanager.generator.generate_p2g.settings") as mock_settings:
        mock_settings.trajectory_input_path = tmp_path
        with pytest.raises(ValueError, match="contient des valeurs non numériques"):
            generate_modulation_df_from_csv("mod.csv", "H2")


def test_generate_modulation_df_from_csv_invalid_length(tmp_path):
    csv_file = tmp_path / "mod.csv"
    csv_file.write_text("H2\n" + "1.0\n" * 100)

    with patch("antares.datamanager.generator.generate_p2g.settings") as mock_settings:
        mock_settings.trajectory_input_path = tmp_path
        with pytest.raises(ValueError, match="Nombre de lignes incorrect"):
            generate_modulation_df_from_csv("mod.csv", "H2")


def test_generate_modulation_df_from_csv_success(tmp_path):
    csv_file = tmp_path / "mod.csv"
    lines = ["H2\tGas"] + [f"{float(i % 10)}\t0.5" for i in range(EXPECTED_HOURS)]
    csv_file.write_text("\n".join(lines))

    with patch("antares.datamanager.generator.generate_p2g.settings") as mock_settings:
        mock_settings.trajectory_input_path = tmp_path
        df = generate_modulation_df_from_csv("mod.csv", "H2")

        assert df.shape == (EXPECTED_HOURS, 4)
        # Column 0: values, Column 1: values, Column 2: 1s, Column 3: 0s
        expected_values = np.array([float(i % 10) for i in range(EXPECTED_HOURS)])
        assert np.allclose(df.iloc[:, 0].to_numpy(), expected_values)
        assert np.allclose(df.iloc[:, 1].to_numpy(), expected_values)
        assert np.all(df.iloc[:, 2].to_numpy() == 1)
        assert np.all(df.iloc[:, 3].to_numpy() == 0)


# ---------------------------------------------------------------------------
# Tests: generate_profile_hydro
# ---------------------------------------------------------------------------


def test_generate_profile_H2_with_zero_load_factors():
    df_zeros = pd.DataFrame(np.zeros((EXPECTED_HOURS, 1)))
    cluster_pv = _make_mock_res_cluster(df_zeros, name="solar_pv")
    cluster_wind = _make_mock_res_cluster(df_zeros, name="wind_onshore")

    res_clusters = {"solar_pv": cluster_pv, "wind_onshore": cluster_wind}
    area_link = {"capacity": 100.0}
    parameters = {
        "FC_electrolyseur": 0.6,
        "Facteur_surdimension_ENR": 1.2,
        "Part_PV_mix": 0.4,
    }

    result = generate_profile_hydro(res_clusters, area_link, parameters)
    assert result.shape == (EXPECTED_HOURS, 1)
    assert np.all(result.to_numpy() == 0.0)


def test_generate_profile_H2_nominal():
    # PV average = 0.2, Wind average = 0.4
    df_pv = pd.DataFrame({"s1": [0.2] * EXPECTED_HOURS})
    df_wind = pd.DataFrame({"s1": [0.4] * EXPECTED_HOURS})
    cluster_pv = _make_mock_res_cluster(df_pv, name="solar_pv")
    cluster_wind = _make_mock_res_cluster(df_wind, name="wind_onshore")

    res_clusters = {"solar_pv": cluster_pv, "wind_onshore": cluster_wind}
    area_link = {"capacity": 1000.0}
    parameters = {
        "FC_electrolyseur": 0.5,
        "Facteur_surdimension_ENR": 1.0,
        "Part_PV_mix": 0.5,
    }

    result = generate_profile_hydro(res_clusters, area_link, parameters)
    assert result.shape == (EXPECTED_HOURS, 1)
    assert not result.empty
    assert (result.to_numpy() <= 1000.0).all()


def test_generate_profile_h2_default_parameters():
    df_pv = pd.DataFrame({"s1": [0.2] * EXPECTED_HOURS})
    df_wind = pd.DataFrame({"s1": [0.4] * EXPECTED_HOURS})
    cluster_pv = _make_mock_res_cluster(df_pv, name="solar_pv")
    cluster_wind = _make_mock_res_cluster(df_wind, name="wind_onshore")

    res_clusters = {"solar_pv": cluster_pv, "wind_onshore": cluster_wind}
    area_link = {"capacity": 500.0}

    # Parameters with missing or non-float values use default values (fc_elec=0.0 -> profile is 0)
    parameters_missing = {
        "Facteur_surdimension_ENR": 1.2,
        "Part_PV_mix": 0.5,
    }
    result = generate_profile_hydro(res_clusters, area_link, parameters_missing)
    assert result.shape == (EXPECTED_HOURS, 1)
    assert np.all(result.to_numpy() == 0.0)

    parameters_invalid = {
        "FC_electrolyseur": "not_a_number",
        "Facteur_surdimension_ENR": "invalid",
        "Part_PV_mix": "invalid",
    }
    result_invalid = generate_profile_hydro(res_clusters, area_link, parameters_invalid)
    assert result_invalid.shape == (EXPECTED_HOURS, 1)
    assert np.all(result_invalid.to_numpy() == 0.0)


def test_generate_profile_h2_cluster_discovery_by_group():
    df_pv = pd.DataFrame({"s1": [0.2] * EXPECTED_HOURS})
    df_wind = pd.DataFrame({"s1": [0.4] * EXPECTED_HOURS})

    cluster_pv = MagicMock()
    cluster_pv.name = "fr_pv_cluster_1"
    mock_prop_pv = MagicMock()
    mock_prop_pv.group = "Solar PV"
    cluster_pv.properties = mock_prop_pv
    cluster_pv.get_timeseries.return_value = df_pv

    cluster_wind = MagicMock()
    cluster_wind.name = "fr_wind_cluster_1"
    mock_prop_wind = MagicMock()
    mock_prop_wind.group = "Wind Onshore"
    cluster_wind.properties = mock_prop_wind
    cluster_wind.get_timeseries.return_value = df_wind

    res_clusters = {"solar_pv": cluster_pv, "wind_onshore": cluster_wind}
    area_link = {"capacity": 1000.0}
    parameters = {
        "FC_electrolyseur": 0.5,
        "Facteur_surdimension_ENR": 1.0,
        "Part_PV_mix": 0.5,
    }

    result = generate_profile_hydro(res_clusters, area_link, parameters)
    assert result.shape == (EXPECTED_HOURS, 1)
    assert not result.empty


# ---------------------------------------------------------------------------
# Tests: compute_total_links_capacity
# ---------------------------------------------------------------------------


def test_compute_total_links_capacity_none_or_invalid():
    assert compute_total_links_capacity(None) == 0.0
    assert compute_total_links_capacity("invalid") == 0.0
    assert compute_total_links_capacity([]) == 0.0


def test_compute_total_links_capacity_valid_and_mixed():
    links_data = {
        "FR": {"capacity": 1500},
        "BE": {"capacity": "3456"},
        "DE": {"capacity": "not_a_number"},
        "NL": {"invalid_key": 100},
        "ES": "not_a_dict",
    }
    assert compute_total_links_capacity(links_data) == 4956.0


# ---------------------------------------------------------------------------
# Tests: build_binding_constraint
# ---------------------------------------------------------------------------


def test_build_binding_constraint():
    study = MagicMock(spec=Study)
    build_binding_constraint(study, "FR", 300.0)

    study.create_binding_constraint.assert_called_once()
    call_kwargs = study.create_binding_constraint.call_args.kwargs

    assert call_kwargs["name"] == f"{P2G_FATAL_BAND_PREFIX}FR"
    assert call_kwargs["properties"].enabled is True
    assert call_kwargs["properties"].time_step == BindingConstraintFrequency.HOURLY
    assert call_kwargs["properties"].operator == BindingConstraintOperator.GREATER

    terms = call_kwargs["terms"]
    assert len(terms) == 1
    assert terms[0].weight == 1
    assert terms[0].data == LinkData(area1="FR", area2="z_p2g_base")

    rhs = call_kwargs["greater_term_matrix"]
    assert rhs.shape == (BINDING_CONSTRAINT_HOURLY_ROWS, 1)
    assert np.all(rhs.to_numpy() == 300.0)


# ---------------------------------------------------------------------------
# Tests: create_p2g_links
# ---------------------------------------------------------------------------


def test_create_p2g_links_empty():
    study = MagicMock(spec=Study)
    assert create_p2g_links(study, f"{AREA_PREFIX}base", "base", {}) is None
    study.create_link.assert_not_called()


def test_create_p2g_links_base():
    study = MagicMock(spec=Study)
    mock_link = MagicMock()
    study.create_link.return_value = mock_link

    type_data = {
        "links": {
            "FR": {"capacity": 1500, "fatal_band": 300},
            "BE": {"capacity": 3456, "fatal_band": 200},
        }
    }

    create_p2g_links(study, f"{AREA_PREFIX}base", "base", type_data)

    assert study.create_link.call_count == 2
    assert study.create_binding_constraint.call_count == 2
    assert mock_link.set_capacity_direct.call_count == 2


def test_create_p2g_links_non_base():
    study = MagicMock(spec=Study)
    mock_link = MagicMock()
    study.create_link.return_value = mock_link

    type_data = {
        "links": {
            "FR": {"capacity": 250},
        }
    }

    create_p2g_links(study, f"{AREA_PREFIX}marg", "marg", type_data)

    study.create_link.assert_called_once_with(area_from="FR", area_to=f"{AREA_PREFIX}marg")
    mock_link.set_capacity_direct.assert_called_once()
    study.create_binding_constraint.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: create_p2g_asservi_links
# ---------------------------------------------------------------------------


def test_create_p2g_asservi_links_empty():
    study = MagicMock(spec=Study)
    assert create_p2g_asservi_links(study, f"{AREA_PREFIX}asservi", {}) is None


def test_create_p2g_asservi_links_success():
    study = MagicMock(spec=Study)
    mock_link = MagicMock()
    study.create_link.return_value = mock_link

    # Area mock
    area_fr = MagicMock()
    area_be = MagicMock()

    df_pv = pd.DataFrame({"s1": [0.2] * EXPECTED_HOURS})
    df_wind = pd.DataFrame({"s1": [0.4] * EXPECTED_HOURS})
    cluster_pv = _make_mock_res_cluster(df_pv, "solar_pv")
    cluster_wind = _make_mock_res_cluster(df_wind, "wind_onshore")

    area_fr.get_renewables.return_value = {"solar_pv": cluster_pv, "wind_onshore": cluster_wind}
    area_be.get_renewables.return_value = {"solar_pv": cluster_pv, "wind_onshore": cluster_wind}

    study.get_areas.return_value = {"fr": area_fr, "be": area_be}

    type_data = {
        "links": {
            "FR": {"capacity": 140},
            "BE": {"capacity": 300},
        },
        "parameters": {
            "FC_electrolyseur": 0.5,
            "Facteur_surdimension_ENR": 1.2,
            "Part_PV_mix": 0.5,
        },
    }

    total_profile = create_p2g_asservi_links(study, f"{AREA_PREFIX}asservi", type_data)

    assert study.create_link.call_count == 2
    assert mock_link.set_capacity_direct.call_count == 2
    assert total_profile is not None
    assert total_profile.shape == (EXPECTED_HOURS, 1)


def test_create_p2g_asservi_links_case_insensitive_areas():
    study = MagicMock(spec=Study)
    mock_link = MagicMock()
    study.create_link.return_value = mock_link

    area_fr = MagicMock()
    df_pv = pd.DataFrame({"s1": [0.2] * EXPECTED_HOURS})
    df_wind = pd.DataFrame({"s1": [0.4] * EXPECTED_HOURS})
    cluster_pv = _make_mock_res_cluster(df_pv, "solar_pv")
    cluster_wind = _make_mock_res_cluster(df_wind, "wind_onshore")
    area_fr.get_renewables.return_value = {"solar_pv": cluster_pv, "wind_onshore": cluster_wind}

    # Study get_areas() returns dict keyed by lowercase area names
    study.get_areas.return_value = {"fr": area_fr}

    type_data = {
        "links": {
            "FR": {"capacity": 140},
        },
        "parameters": {
            "FC_electrolyseur": 0.5,
            "Facteur_surdimension_ENR": 1.2,
            "Part_PV_mix": 0.5,
        },
    }

    total_profile = create_p2g_asservi_links(study, f"{AREA_PREFIX}asservi", type_data)
    assert study.create_link.call_count == 1
    assert mock_link.set_capacity_direct.call_count == 1
    assert total_profile is not None


def test_create_p2g_asservi_links_missing_area_or_renewables():
    study = MagicMock(spec=Study)
    area_be = MagicMock()
    area_be.get_renewables.return_value = {}  # Empty renewables
    study.get_areas.return_value = {"fr": None, "be": area_be}

    type_data = {
        "links": {
            "FR": {"capacity": 140},  # FR is None in study areas
            "BE": {"capacity": 300},  # BE has empty renewables
        },
        "parameters": {
            "FC_electrolyseur": 0.5,
            "Facteur_surdimension_ENR": 1.2,
            "Part_PV_mix": 0.5,
        },
    }

    total_profile = create_p2g_asservi_links(study, f"{AREA_PREFIX}asservi", type_data)
    assert total_profile is None
    study.create_link.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: generate_p2g full workflow
# ---------------------------------------------------------------------------


@patch("antares.datamanager.generator.generate_p2g.generate_modulation_df_from_csv")
def test_generate_p2g_complete(mock_gen_modulation):
    mock_gen_modulation.return_value = pd.DataFrame(np.ones((EXPECTED_HOURS, 4)))

    study = MagicMock(spec=Study)
    mock_area = MagicMock()
    mock_thermal_cluster = MagicMock()
    mock_area.create_thermal_cluster.return_value = mock_thermal_cluster
    study.create_area.return_value = mock_area

    # Renewables for asservi
    area_fr = MagicMock()
    df_pv = pd.DataFrame({"s1": [0.2] * EXPECTED_HOURS})
    df_wind = pd.DataFrame({"s1": [0.4] * EXPECTED_HOURS})
    cluster_pv = _make_mock_res_cluster(df_pv, "solar_pv")
    cluster_wind = _make_mock_res_cluster(df_wind, "wind_onshore")
    area_fr.get_renewables.return_value = {"solar_pv": cluster_pv, "wind_onshore": cluster_wind}
    study.get_areas.return_value = {"fr": area_fr}

    data_p2g = {
        "market_modulation": "modulation.csv",
        "base": {
            "properties": {
                "adequacy_patch_mode": AdequacyPatchMode.VIRTUAL,
                "nominal_capacity": 4000,
                "cost": 78.0,
            },
            "modulation": "H2",
            "links": {"FR": {"capacity": 1500, "fatal_band": 300}},
        },
        "marg": {
            "properties": {
                "adequacy_patch_mode": AdequacyPatchMode.VIRTUAL,
                "nominal_capacity": 5000,
                "cost": 78.0,
            },
            "modulation": "Gas",
            "links": {"FR": {"capacity": 250}},
        },
        "methanation": {
            "properties": {
                "adequacy_patch_mode": AdequacyPatchMode.VIRTUAL,
                "nominal_capacity": 3890,
                "cost": 78.0,
            },
            "links": {"FR": {"capacity": 300}},
        },
        "asservi": {
            "properties": {
                "adequacy_patch_mode": AdequacyPatchMode.OUTSIDE,
                "nominal_capacity": 2500,
                "cost": 78.0,
            },
            "modulation": "H2",
            "links": {"FR": {"capacity": 140}},
            "parameters": {
                "FC_electrolyseur": 0.5,
                "Facteur_surdimension_ENR": 1.2,
                "Part_PV_mix": 0.9,
            },
        },
    }

    generate_p2g(study=study, data_p2g=data_p2g)

    # 4 virtual areas should be created
    assert study.create_area.call_count == len(P2G_TYPES)
    # Verify properties passed to create_area
    created_area_names = [call.kwargs.get("area_name") for call in study.create_area.call_args_list]
    for p2g_type in P2G_TYPES:
        assert f"{AREA_PREFIX}{p2g_type}" in created_area_names

    # Check thermal clusters
    assert mock_area.create_thermal_cluster.call_count == len(P2G_TYPES)
    # Check set_load called for each virtual area
    assert mock_area.set_load.call_count == len(P2G_TYPES)
    # Modulation called for base, marg, asservi (3 times)
    assert mock_gen_modulation.call_count == 3
