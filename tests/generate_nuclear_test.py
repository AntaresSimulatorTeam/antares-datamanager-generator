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

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from antares.craft import BindingConstraintFrequency, BindingConstraintOperator
from antares.datamanager.exceptions.exceptions import NuclearGenerationError
from antares.datamanager.generator.generate_nuclear import (
    generate_nuclear_availability,
    generate_nuclear_modulation_binding_constraints,
    generate_nuclear_talon_binding_constraint,
    generate_y_nuc_modulation_misc,
)


def test_generate_y_nuc_modulation_misc_hardcodes_psp_column():
    area = MagicMock()

    generate_y_nuc_modulation_misc(area)

    area.set_misc_gen.assert_called_once()
    matrix = area.set_misc_gen.call_args[0][0]
    assert matrix.shape == (8760, 8)
    assert (matrix["PSP"] == -999999).all()
    other_columns = matrix.drop(columns=["PSP"])
    assert float(other_columns.to_numpy().sum()) == 0.0


@patch("antares.datamanager.generator.generate_nuclear.settings")
@patch("antares.datamanager.generator.generate_nuclear.pd.read_feather")
def test_generate_nuclear_modulation_binding_constraints_builds_expected_terms(
    mock_read_feather, mock_settings, tmp_path
):
    mock_settings.nuclear_modulation_ts_directory = tmp_path
    mock_read_feather.return_value = pd.DataFrame({"0": [1.0] * 8760})

    study = MagicMock()
    used_files: set[Path] = set()

    nuclear_modulation_binding_constraints = {
        "group": "scenarised200",
        "nbTsColumns": 200,
        "frStandardClusters": ["fr_nuclear_cp0_cp1_cp2", "fr_nuclear_epr"],
        "frPeakClusters": ["fr_nuclear_peak1"],
        "yNucModulationClusters": ["y_nuc_modulation_nuclear_cp0_cp1_cp2", "y_nuc_modulation_nuclear_epr"],
        "constraints": [
            {
                "name": "nuc_modulation_limit",
                "type": "hourly",
                "coeff": 1.00,
                "includesPeak": True,
                "series": "limit.arrow",
            },
            {
                "name": "nuc_modulation_weekly",
                "type": "weekly",
                "coeff": 0.93,
                "includesPeak": False,
                "series": "weekly.arrow",
            },
        ],
    }

    generate_nuclear_modulation_binding_constraints(study, nuclear_modulation_binding_constraints, used_files)

    assert study.create_binding_constraint.call_count == 2
    assert used_files == {tmp_path / "limit.arrow", tmp_path / "weekly.arrow"}

    limit_kwargs = study.create_binding_constraint.call_args_list[0].kwargs
    assert limit_kwargs["name"] == "nuc_modulation_limit"
    assert limit_kwargs["properties"].time_step == BindingConstraintFrequency.HOURLY
    assert limit_kwargs["properties"].operator == BindingConstraintOperator.LESS
    assert limit_kwargs["properties"].group == "scenarised200"

    fr_terms = [t for t in limit_kwargs["terms"] if t.data.area == "fr"]
    y_terms = [t for t in limit_kwargs["terms"] if t.data.area == "y_nuc_modulation"]
    assert {t.data.cluster for t in fr_terms} == {"fr_nuclear_cp0_cp1_cp2", "fr_nuclear_epr", "fr_nuclear_peak1"}
    assert all(t.weight == 1 for t in fr_terms)
    assert {t.data.cluster for t in y_terms} == {
        "y_nuc_modulation_nuclear_cp0_cp1_cp2",
        "y_nuc_modulation_nuclear_epr",
    }
    assert all(t.weight == -1.00 for t in y_terms)

    weekly_kwargs = study.create_binding_constraint.call_args_list[1].kwargs
    assert weekly_kwargs["properties"].time_step == BindingConstraintFrequency.WEEKLY
    weekly_fr_clusters = {t.data.cluster for t in weekly_kwargs["terms"] if t.data.area == "fr"}
    assert weekly_fr_clusters == {"fr_nuclear_cp0_cp1_cp2", "fr_nuclear_epr"}
    weekly_y_terms = [t for t in weekly_kwargs["terms"] if t.data.area == "y_nuc_modulation"]
    assert all(t.weight == -0.93 for t in weekly_y_terms)


@patch("antares.datamanager.generator.generate_nuclear.settings")
@patch("antares.datamanager.generator.generate_nuclear.pd.read_feather")
def test_generate_nuclear_talon_binding_constraint_builds_expected_terms(mock_read_feather, mock_settings, tmp_path):
    mock_settings.nuclear_talon_ts_directory = tmp_path
    mock_read_feather.return_value = pd.DataFrame({"0": [1.0] * 8760})

    study = MagicMock()
    used_files: set[Path] = set()

    nuclear_talon_binding_constraint = {
        "group": "scenarised200",
        "nbTsColumns": 200,
        "frStandardClusters": ["fr_nuclear_cp0_cp1_cp2", "fr_nuclear_epr"],
        "series": "talon.arrow",
    }

    generate_nuclear_talon_binding_constraint(study, nuclear_talon_binding_constraint, used_files)

    study.create_binding_constraint.assert_called_once()
    assert used_files == {tmp_path / "talon.arrow"}

    kwargs = study.create_binding_constraint.call_args.kwargs
    assert kwargs["name"] == "talon_nuc"
    assert kwargs["properties"].time_step == BindingConstraintFrequency.HOURLY
    assert kwargs["properties"].operator == BindingConstraintOperator.GREATER
    assert kwargs["properties"].group == "scenarised200"
    assert "less_term_matrix" not in kwargs or kwargs.get("less_term_matrix") is None
    assert kwargs["greater_term_matrix"] is not None

    terms = kwargs["terms"]
    assert {t.data.area for t in terms} == {"fr"}
    assert {t.data.cluster for t in terms} == {"fr_nuclear_cp0_cp1_cp2", "fr_nuclear_epr"}
    assert all(t.weight == 1 for t in terms)


@patch("antares.datamanager.generator.generate_nuclear.settings")
def test_generate_nuclear_modulation_binding_constraints_empty_constraints_creates_nothing(mock_settings, tmp_path):
    """A modulation trajectory linked with no constraints entries must not create anything."""
    mock_settings.nuclear_modulation_ts_directory = tmp_path
    study = MagicMock()

    generate_nuclear_modulation_binding_constraints(
        study,
        {
            "group": "scenarised200",
            "frStandardClusters": [],
            "frPeakClusters": [],
            "yNucModulationClusters": [],
            "constraints": [],
        },
    )

    study.create_binding_constraint.assert_not_called()


def test_generate_nuclear_modulation_binding_constraints_raises_on_incomplete_payload():
    """A linked nuclear_modulation trajectory always has all of its fields.
    Something missing means the contract was broken somwhere, and must fail."""
    study = MagicMock()

    with pytest.raises(NuclearGenerationError, match="frStandardClusters"):
        generate_nuclear_modulation_binding_constraints(study, {"group": "scenarised200", "constraints": []})

    study.create_binding_constraint.assert_not_called()


@patch("antares.datamanager.generator.generate_nuclear.settings")
@patch("antares.datamanager.generator.generate_nuclear.pd.read_feather")
def test_generate_nuclear_modulation_binding_constraints_raises_on_incomplete_constraint_entry(
    mock_read_feather, mock_settings, tmp_path
):
    """Same as above: an individual constraint entry missing a field must fail"""
    mock_settings.nuclear_modulation_ts_directory = tmp_path
    mock_read_feather.return_value = pd.DataFrame({"0": [1.0] * 8760})
    study = MagicMock()

    with pytest.raises(NuclearGenerationError, match="coeff"):
        generate_nuclear_modulation_binding_constraints(
            study,
            {
                "group": "scenarised200",
                "frStandardClusters": ["fr_nuclear_epr"],
                "frPeakClusters": [],
                "yNucModulationClusters": ["y_nuc_modulation_nuclear_epr"],
                "constraints": [
                    {"name": "nuc_modulation_limit", "type": "hourly", "includesPeak": True, "series": "s.arrow"}
                ],
            },
        )


def test_generate_nuclear_talon_binding_constraint_raises_on_incomplete_payload():
    """Same as modulation: a linked nuclear_talon trajectory always has all of its fields together"""
    study = MagicMock()

    with pytest.raises(NuclearGenerationError, match="series"):
        generate_nuclear_talon_binding_constraint(
            study, {"group": "scenarised200", "frStandardClusters": ["fr_nuclear_epr"]}
        )

    study.create_binding_constraint.assert_not_called()


def _area_with_thermals(cluster_ids: list[str]) -> MagicMock:
    area = MagicMock()
    thermals = {cluster_id: MagicMock() for cluster_id in cluster_ids}
    area.get_thermals.return_value = thermals
    return area


@patch("antares.datamanager.generator.generate_nuclear.settings")
@patch("antares.datamanager.generator.generate_nuclear.pd.read_feather")
def test_generate_nuclear_availability_passthrough_for_lt_and_epr(mock_read_feather, mock_settings, tmp_path):
    mock_settings.nuclear_availability_ts_directory = tmp_path
    epr_df = pd.DataFrame({"0": [1.0, 2.0], "1": [3.0, 4.0]})
    mock_read_feather.return_value = epr_df

    area = _area_with_thermals(["fr_nuclear_epr"])
    used_files: set[Path] = set()

    generate_nuclear_availability(area, {"fr_nuclear_epr": {"series": "epr.arrow"}}, used_files)

    thermal_cluster = area.get_thermals()["fr_nuclear_epr"]
    thermal_cluster.set_series.assert_called_once()
    pd.testing.assert_frame_equal(thermal_cluster.set_series.call_args[0][0], epr_df)
    assert used_files == {tmp_path / "epr.arrow"}


@patch("antares.datamanager.generator.generate_nuclear.settings")
@patch("antares.datamanager.generator.generate_nuclear.pd.read_feather")
def test_generate_nuclear_availability_skips_clusters_without_series(mock_read_feather, mock_settings, tmp_path):
    mock_settings.nuclear_availability_ts_directory = tmp_path

    area = _area_with_thermals(["fr_nuclear_n4"])

    generate_nuclear_availability(area, {"fr_nuclear_n4": {"properties": {}}}, set())

    mock_read_feather.assert_not_called()
    area.get_thermals()["fr_nuclear_n4"].set_series.assert_not_called()


@patch("antares.datamanager.generator.generate_nuclear.settings")
@patch("antares.datamanager.generator.generate_nuclear.pd.read_feather")
def test_generate_nuclear_availability_lt_series_shared_across_clusters_is_read_once(
    mock_read_feather, mock_settings, tmp_path
):
    """cp0_cp1_cp2/n4/p4 all reference the same LT arrow file: reused, not re-read."""
    mock_settings.nuclear_availability_ts_directory = tmp_path
    lt_df = pd.DataFrame({"0": [1.0] * 24})
    mock_read_feather.return_value = lt_df

    cluster_ids = ["fr_nuclear_cp0_cp1_cp2", "fr_nuclear_n4", "fr_nuclear_p4"]
    area = _area_with_thermals(cluster_ids)
    nuclear_clusters = {cluster_id: {"series": "lt.arrow"} for cluster_id in cluster_ids}

    generate_nuclear_availability(area, nuclear_clusters, set())

    assert mock_read_feather.call_count == 1
    for cluster_id in cluster_ids:
        thermal_cluster = area.get_thermals()[cluster_id]
        thermal_cluster.set_series.assert_called_once()
        pd.testing.assert_frame_equal(thermal_cluster.set_series.call_args[0][0], lt_df)


@patch("antares.datamanager.generator.generate_nuclear.settings")
@patch("antares.datamanager.generator.generate_nuclear.pd.read_feather")
def test_generate_nuclear_availability_smr_single_unit_returns_pool_unchanged(
    mock_read_feather, mock_settings, tmp_path
):
    mock_settings.nuclear_availability_ts_directory = tmp_path
    pool_df = pd.DataFrame({"0": [1.0, 2.0], "1": [3.0, 4.0]})
    mock_read_feather.return_value = pool_df

    area = _area_with_thermals(["fr_nuclear_smr"])
    smr_mixage = {"unit_count": 1, "seed": "frnuclear_smrseed-tsgen-thermal"}

    generate_nuclear_availability(
        area, {"fr_nuclear_smr": {"series": "smr_pool.arrow", "smr_mixage": smr_mixage}}, set()
    )

    thermal_cluster = area.get_thermals()["fr_nuclear_smr"]
    pd.testing.assert_frame_equal(thermal_cluster.set_series.call_args[0][0], pool_df)


@patch("antares.datamanager.generator.generate_nuclear.settings")
@patch("antares.datamanager.generator.generate_nuclear.pd.read_feather")
def test_generate_nuclear_availability_smr_multiple_units_is_deterministic_and_preserves_shape(
    mock_read_feather, mock_settings, tmp_path
):
    mock_settings.nuclear_availability_ts_directory = tmp_path
    pool_df = pd.DataFrame(np.arange(20, dtype=float).reshape(4, 5), columns=[str(i) for i in range(5)])
    mock_read_feather.return_value = pool_df

    smr_mixage = {"unit_count": 3, "seed": "frnuclear_smrseed-tsgen-thermal"}
    nuclear_clusters = {"fr_nuclear_smr": {"series": "smr_pool.arrow", "smr_mixage": smr_mixage}}

    area_1 = _area_with_thermals(["fr_nuclear_smr"])
    generate_nuclear_availability(area_1, nuclear_clusters, set())
    result_1 = area_1.get_thermals()["fr_nuclear_smr"].set_series.call_args[0][0]

    area_2 = _area_with_thermals(["fr_nuclear_smr"])
    generate_nuclear_availability(area_2, nuclear_clusters, set())
    result_2 = area_2.get_thermals()["fr_nuclear_smr"].set_series.call_args[0][0]

    pd.testing.assert_frame_equal(result_1, result_2)
    assert result_1.shape == pool_df.shape
    assert list(result_1.columns) == list(pool_df.columns)
    # First unit contributes the pool unchanged, so the mixed result must be >= the raw pool
    # wherever additional units' draws add non-negative values.
    assert (result_1.to_numpy() >= pool_df.to_numpy()).all()


@patch("antares.datamanager.generator.generate_nuclear.settings")
@patch("antares.datamanager.generator.generate_nuclear.pd.read_feather")
def test_generate_nuclear_availability_smr_invalid_unit_count_raises(mock_read_feather, mock_settings, tmp_path):
    mock_settings.nuclear_availability_ts_directory = tmp_path
    mock_read_feather.return_value = pd.DataFrame({"0": [1.0]})

    area = _area_with_thermals(["fr_nuclear_smr"])
    smr_mixage = {"unit_count": 0, "seed": "seed"}

    with pytest.raises(NuclearGenerationError, match="unit_count"):
        generate_nuclear_availability(
            area, {"fr_nuclear_smr": {"series": "smr_pool.arrow", "smr_mixage": smr_mixage}}, set()
        )


@patch("antares.datamanager.generator.generate_nuclear.settings")
@patch("antares.datamanager.generator.generate_nuclear.pd.read_feather")
def test_generate_nuclear_availability_smr_missing_seed_raises(mock_read_feather, mock_settings, tmp_path):
    mock_settings.nuclear_availability_ts_directory = tmp_path
    mock_read_feather.return_value = pd.DataFrame({"0": [1.0]})

    area = _area_with_thermals(["fr_nuclear_smr"])
    smr_mixage = {"unit_count": 2}

    with pytest.raises(NuclearGenerationError, match="seed"):
        generate_nuclear_availability(
            area, {"fr_nuclear_smr": {"series": "smr_pool.arrow", "smr_mixage": smr_mixage}}, set()
        )


@patch("antares.datamanager.generator.generate_nuclear.settings")
@patch("antares.datamanager.generator.generate_nuclear.pd.read_feather")
def test_generate_nuclear_availability_missing_cluster_on_area_raises(mock_read_feather, mock_settings, tmp_path):
    mock_settings.nuclear_availability_ts_directory = tmp_path
    mock_read_feather.return_value = pd.DataFrame({"0": [1.0]})

    area = _area_with_thermals([])  # cluster wasn't created beforehand

    with pytest.raises(NuclearGenerationError, match="fr_nuclear_epr"):
        generate_nuclear_availability(area, {"fr_nuclear_epr": {"series": "epr.arrow"}}, set())
