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

import pandas as pd

from antares.craft import BindingConstraintFrequency, BindingConstraintOperator
from antares.datamanager.exceptions.exceptions import NuclearGenerationError
from antares.datamanager.generator.generate_nuclear import (
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
