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
from unittest.mock import MagicMock, patch

import pandas as pd

from antares.craft import BindingConstraintFrequency, BindingConstraintOperator
from antares.datamanager.generator.generate_study_process import (
    _build_dsr_constraint_names,
    add_areas_to_study,
)
from antares.datamanager.models.study_data_json_model import StudyData


def test_build_dsr_constraint_names():
    """Verify generated constraint names, cluster names, and area IDs for FR and Non-FR cases."""
    # FR Case
    bc_name, cluster_name, area_id = _build_dsr_constraint_names("FR_DSR_tertiaire")
    assert bc_name == "FR_DSR_tertiaire_stock"
    assert cluster_name == "FR_DSR_tertiaire"
    assert area_id == "fr"

    bc_name_ind, cluster_name_ind, area_id_ind = _build_dsr_constraint_names("FR_DSR_industriel")
    assert bc_name_ind == "FR_DSR_industriel_stock"
    assert cluster_name_ind == "FR_DSR_industriel"
    assert area_id_ind == "fr"

    # Non-FR Case
    bc_name_be, cluster_name_be, area_id_be = _build_dsr_constraint_names("BE_DSR")
    assert bc_name_be == "DSR_BE_stock"
    assert cluster_name_be == "be_dsr 0"
    assert area_id_be == "be"

    bc_name_de, cluster_name_de, area_id_de = _build_dsr_constraint_names("DE_DSR_0")
    assert bc_name_de == "DSR_DE_stock"
    assert cluster_name_de == "de_dsr 0"
    assert area_id_de == "de"


@patch("antares.datamanager.generator.generate_study_process.generator_load_directory")
@patch("antares.datamanager.generator.generate_study_process.generate_thermal_clusters")
@patch("antares.datamanager.generator.generate_study_process.generate_sts_clusters")
@patch("antares.datamanager.generator.generate_study_process.generate_dsr_clusters")
def test_add_areas_to_study_creates_binding_constraints(
    mock_generate_dsr, mock_generate_sts, mock_generate_thermal, mock_load_dir
):
    mock_load_dir.return_value = Path("/tmp")
    mock_study = MagicMock()
    mock_area_obj = MagicMock()
    mock_study.create_area.return_value = mock_area_obj
    used_files = set()

    # 1. Test Non-FR area
    study_data_non_fr = StudyData(
        name="test_study",
        areas={
            "BE": {
                "dsr": {
                    "BE_DSR": {"properties": {"enabled": True}, "data": {"nb_hour_per_day": 12, "max_hour_per_day": 1}}
                }
            }
        },
    )
    # Mock Step 1 result for BE
    mock_generate_dsr.return_value = pd.DataFrame({"BE_DSR": [100.0] * 365})

    add_areas_to_study(mock_study, study_data_non_fr, used_files)

    # Check if create_binding_constraint was called for BE and verify the constraint name
    mock_study.create_binding_constraint.assert_called_once()
    non_fr_constraint_names = [call.kwargs["name"] for call in mock_study.create_binding_constraint.call_args_list]
    assert non_fr_constraint_names == ["DSR_BE_stock"]

    args, kwargs = mock_study.create_binding_constraint.call_args
    assert kwargs["name"] == "DSR_BE_stock"
    assert kwargs["properties"].time_step == BindingConstraintFrequency.DAILY
    assert kwargs["properties"].operator == BindingConstraintOperator.LESS
    assert kwargs["terms"][0].data.area == "be"
    assert kwargs["terms"][0].data.cluster == "be_dsr 0"
    assert kwargs["less_term_matrix"].shape == (365, 1)
    assert kwargs["less_term_matrix"].iloc[0, 0] == 100.0

    mock_study.create_binding_constraint.reset_mock()
    mock_generate_dsr.reset_mock()

    # 2. Test FR area (with multiple DSR sub-clusters)
    study_data_fr = StudyData(
        name="test_study",
        areas={
            "FR": {
                "dsr": {
                    "FR_DSR_tertiaire": {
                        "properties": {"enabled": True},
                        "data": {"nb_hour_per_day": 13, "max_hour_per_day": 1},
                    },
                    "FR_DSR_industriel": {
                        "properties": {"enabled": True},
                        "data": {"nb_hour_per_day": 10, "max_hour_per_day": 2},
                    },
                }
            }
        },
    )
    # Mock Step 1 result for FR with multiple columns
    mock_generate_dsr.return_value = pd.DataFrame(
        {
            "FR_DSR_tertiaire": [184.6] * 365,
            "FR_DSR_industriel": [960.0] * 365,
        }
    )

    add_areas_to_study(mock_study, study_data_fr, used_files)

    # Check if create_binding_constraint was called for each FR DSR cluster and verify all constraint names
    assert mock_study.create_binding_constraint.call_count == 2
    fr_constraint_names = [call.kwargs["name"] for call in mock_study.create_binding_constraint.call_args_list]
    assert fr_constraint_names == ["FR_DSR_tertiaire_stock", "FR_DSR_industriel_stock"]

    # First constraint: FR_DSR_tertiaire
    call_ter_kwargs = mock_study.create_binding_constraint.call_args_list[0].kwargs
    assert call_ter_kwargs["name"] == "FR_DSR_tertiaire_stock"
    assert call_ter_kwargs["properties"].time_step == BindingConstraintFrequency.DAILY
    assert call_ter_kwargs["properties"].operator == BindingConstraintOperator.LESS
    assert call_ter_kwargs["terms"][0].data.area == "fr"
    assert call_ter_kwargs["terms"][0].data.cluster == "FR_DSR_tertiaire"
    assert call_ter_kwargs["less_term_matrix"].shape == (365, 1)
    assert call_ter_kwargs["less_term_matrix"].iloc[0, 0] == 184.6

    # Second constraint: FR_DSR_industriel
    call_ind_kwargs = mock_study.create_binding_constraint.call_args_list[1].kwargs
    assert call_ind_kwargs["name"] == "FR_DSR_industriel_stock"
    assert call_ind_kwargs["properties"].time_step == BindingConstraintFrequency.DAILY
    assert call_ind_kwargs["properties"].operator == BindingConstraintOperator.LESS
    assert call_ind_kwargs["terms"][0].data.area == "fr"
    assert call_ind_kwargs["terms"][0].data.cluster == "FR_DSR_industriel"
    assert call_ind_kwargs["less_term_matrix"].shape == (365, 1)
    assert call_ind_kwargs["less_term_matrix"].iloc[0, 0] == 960.0
